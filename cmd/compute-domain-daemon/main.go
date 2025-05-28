/*
 * Copyright (c) 2025 NVIDIA CORPORATION.  All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package main

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/urfave/cli/v2"

	nvapi "github.com/NVIDIA/k8s-dra-driver-gpu/api/nvidia.com/resource/v1beta1"
)

const (
	nodesConfig = "/etc/nvidia-imex/nodes_config.cfg"
	imexConfig  = "/etc/nvidia-imex/config.cfg"
	imexLog     = "/var/log/nvidia-imex.log"
	imexBinary  = "/usr/bin/nvidia-imex"
	imexCtl     = "/usr/bin/nvidia-imex-ctl"
)

type Flags struct {
	cliqueID               string
	computeDomainUUID      string
	computeDomainName      string
	computeDomainNamespace string
	nodeName               string
	podIP                  string
}

func main() {
	if err := newApp().Run(os.Args); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func newApp() *cli.App {
	// Create local flags variable
	var flags Flags

	// Create a wrapper that will be used to gracefully shut down all subcommands
	wrapper := func(ctx context.Context, f func(ctx context.Context, cancel context.CancelFunc, flags *Flags) error) error {
		// Create a cancelable context from the one passed in
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()

		// Handle SIGTERM
		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, syscall.SIGTERM)
		go func() {
			<-sigChan
			cancel()
		}()

		// Call the wrapped function
		return f(ctx, cancel, &flags)
	}

	// Create the app
	app := &cli.App{
		Name:  "compute-domain-daemon",
		Usage: "compute-domain-daemon manages the IMEX daemon for NVIDIA compute domains.",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:        "cliqueid",
				Usage:       "The clique ID for this node.",
				EnvVars:     []string{"CLIQUE_ID"},
				Destination: &flags.cliqueID,
			},
			&cli.StringFlag{
				Name:        "compute-domain-uuid",
				Usage:       "The UUID of the ComputeDomain to manage.",
				EnvVars:     []string{"COMPUTE_DOMAIN_UUID"},
				Destination: &flags.computeDomainUUID,
			},
			&cli.StringFlag{
				Name:        "compute-domain-name",
				Usage:       "The name of the ComputeDomain to manage.",
				EnvVars:     []string{"COMPUTE_DOMAIN_NAME"},
				Destination: &flags.computeDomainName,
			},
			&cli.StringFlag{
				Name:        "compute-domain-namespace",
				Usage:       "The namespace of the ComputeDomain to manage.",
				Value:       "default",
				EnvVars:     []string{"COMPUTE_DOMAIN_NAMESPACE"},
				Destination: &flags.computeDomainNamespace,
			},
			&cli.StringFlag{
				Name:        "node-name",
				Usage:       "The name of this Kubernetes node.",
				EnvVars:     []string{"NODE_NAME"},
				Destination: &flags.nodeName,
			},
			&cli.StringFlag{
				Name:        "pod-ip",
				Usage:       "The IP address of this pod.",
				EnvVars:     []string{"POD_IP"},
				Destination: &flags.podIP,
			},
		},
		Commands: []*cli.Command{
			{
				Name:  "run",
				Usage: "Run the compute domain daemon",
				Action: func(c *cli.Context) error {
					return wrapper(c.Context, run)
				},
			},
			{
				Name:  "check",
				Usage: "Check if the node is IMEX capable and if the IMEX daemon is ready",
				Action: func(c *cli.Context) error {
					return wrapper(c.Context, check)
				},
			},
		},
	}

	return app
}

// run runs the compute domain daemon, checking IMEX capability and managing the IMEX daemon lifecycle.
// It returns an error if any step fails.
func run(ctx context.Context, cancel context.CancelFunc, flags *Flags) error {
	// Create controller config
	config := &ControllerConfig{
		cliqueID:               flags.cliqueID,
		computeDomainUUID:      flags.computeDomainUUID,
		computeDomainName:      flags.computeDomainName,
		computeDomainNamespace: flags.computeDomainNamespace,
		nodeName:               flags.nodeName,
		podIP:                  flags.podIP,
	}

	// Create and start controller to watch for compute domain updates and keep
	// the IPs mapped in the nodes config file in sync
	controller, err := NewController(config)
	if err != nil {
		return fmt.Errorf("error creating controller: %w", err)
	}

	// Start controller in a goroutine
	errChan := make(chan error, 1)
	go func() {
		err := controller.Run(ctx)
		if err != nil {
			cancel()
		}
		errChan <- err
	}()

	// Wait until all nodes have joined the compute domain
	nodes := controller.BlockUntilAllNodesJoinComputeDomain()

	if flags.cliqueID == "" {
		fmt.Println("ClusterUUID and CliqueId are NOT set for GPUs on this node.")
		fmt.Println("The IMEX daemon will not be started.")
		fmt.Println("Sleeping forever...")
		<-ctx.Done()
		return nil
	}

	// Create nodes config file
	if err := createNodesConfig(ctx, flags.cliqueID, nodes); err != nil {
		return fmt.Errorf("error creating nodes config: %w", err)
	}

	// Print nodes config
	if err := printNodesConfig(ctx); err != nil {
		return fmt.Errorf("error printing nodes config: %w", err)
	}

	// Run IMEX daemon
	if err := runIMEXDaemon(ctx, imexConfig); err != nil {
		return fmt.Errorf("error running IMEX daemon: %w", err)
	}

	// Tail the log file
	if err := tail(ctx, imexLog); err != nil {
		return fmt.Errorf("error tailing log file: %w", err)
	}

	// Wait for controller to finish
	return <-errChan
}

// check verifies if the node is IMEX capable and if so, checks if the IMEX daemon is ready.
// It returns an error if any step fails.
func check(ctx context.Context, cancel context.CancelFunc, flags *Flags) error {
	if flags.cliqueID == "" {
		fmt.Println("ClusterUUID and CliqueId are NOT set for GPUs on this node.")
		return nil
	}

	// Check if IMEX daemon is ready
	cmd := exec.CommandContext(ctx, imexCtl, "-q")
	output, err := cmd.Output()
	if err != nil {
		return fmt.Errorf("error checking IMEX daemon status: %w", err)
	}

	if string(output) != "READY\n" {
		return fmt.Errorf("IMEX daemon not ready: %s", string(output))
	}

	return nil
}

// createNodesConfig creates a nodesConfig file with IPs for nodes in the same clique.
func createNodesConfig(ctx context.Context, cliqueID string, nodes []*nvapi.ComputeDomainNode) error {
	// Ensure the directory exists
	dir := filepath.Dir(nodesConfig)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create directory %s: %w", dir, err)
	}

	// Create or overwrite the nodesConfig file
	f, err := os.Create(nodesConfig)
	if err != nil {
		return fmt.Errorf("failed to read nodes config: %w", err)
	}
	defer f.Close()

	// Write IPs for nodes in the same clique
	for _, node := range nodes {
		if node.CliqueID == cliqueID {
			if _, err := fmt.Fprintf(f, "%s\n", node.IPAddress); err != nil {
				return fmt.Errorf("failed to write to nodesConfig file: %w", err)
			}
		}
	}

	return nil
}

// printNodesConfig reads and prints the contents of the nodes configuration file.
// It returns an error if the file cannot be read.
func printNodesConfig(ctx context.Context) error {
	fmt.Printf("%s:\n", nodesConfig)
	content, err := os.ReadFile(nodesConfig)
	if err != nil {
		return fmt.Errorf("failed to read nodes config: %w", err)
	}
	fmt.Println(string(content))
	return nil
}

// runIMEXDaemon starts the IMEX daemon with the specified configuration file.
// It returns an error if the daemon fails to start or exits unexpectedly.
func runIMEXDaemon(ctx context.Context, config string) error {
	cmd := exec.CommandContext(ctx, imexBinary, "-c", config)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

// tail continuously reads and prints new lines from the specified file using the system's tail command.
// It starts from the beginning of the file (-n +1) and follows new lines (-f).
// It blocks until the context is cancelled or an error occurs.
func tail(ctx context.Context, path string) error {
	cmd := exec.CommandContext(ctx, "tail", "-n", "+1", "-f", path)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}
