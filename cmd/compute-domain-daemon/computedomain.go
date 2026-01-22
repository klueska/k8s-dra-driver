/*
 * Copyright (c) 2025, NVIDIA CORPORATION.  All rights reserved.
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
	"encoding/json"
	"fmt"
	"maps"
	"math/rand"
	"sync"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"

	nvapi "github.com/NVIDIA/k8s-dra-driver-gpu/api/nvidia.com/resource/v1beta1"
	"github.com/NVIDIA/k8s-dra-driver-gpu/pkg/featuregates"
	nvinformers "github.com/NVIDIA/k8s-dra-driver-gpu/pkg/nvidia.com/informers/externalversions"
)

const (
	// Detecting when a CD daemon transitions from NotReady to Ready (based on
	// the startup probe) at the moment sometimes requires an informer resync,
	// see https://github.com/NVIDIA/k8s-dra-driver-gpu/issues/742.
	informerResyncPeriod = 4 * time.Minute
	mutationCacheTTL     = time.Hour
)

// GetComputeDomainFunc is a function type for getting a ComputeDomain by UID.
type GetComputeDomainFunc func(uid string) (*nvapi.ComputeDomain, error)

type IPSet map[string]struct{}

// ComputeDomainManager watches compute domains and updates their status with
// info about the ComputeDomain daemon running on this node.
type ComputeDomainManager struct {
	config        *ManagerConfig
	waitGroup     sync.WaitGroup
	cancelContext context.CancelFunc

	factory  nvinformers.SharedInformerFactory
	informer cache.SharedIndexInformer

	previousNodes    []*nvapi.ComputeDomainNode
	updatedNodesChan chan []*nvapi.ComputeDomainNode

	podManager    *PodManager
	mutationCache cache.MutationCache
}

// NewComputeDomainManager creates a new ComputeDomainManager instance.
func NewComputeDomainManager(config *ManagerConfig) *ComputeDomainManager {
	factory := nvinformers.NewSharedInformerFactoryWithOptions(
		config.clientsets.Nvidia,
		informerResyncPeriod,
		nvinformers.WithNamespace(config.computeDomainNamespace),
		nvinformers.WithTweakListOptions(func(opts *metav1.ListOptions) {
			opts.FieldSelector = fmt.Sprintf("metadata.name=%s", config.computeDomainName)
		}),
	)
	informer := factory.Resource().V1beta1().ComputeDomains().Informer()

	m := &ComputeDomainManager{
		config:           config,
		factory:          factory,
		informer:         informer,
		previousNodes:    []*nvapi.ComputeDomainNode{},
		updatedNodesChan: make(chan []*nvapi.ComputeDomainNode),
	}

	return m
}

// Start starts the compute domain manager.
func (m *ComputeDomainManager) Start(ctx context.Context) (rerr error) {
	ctx, cancel := context.WithCancel(ctx)
	m.cancelContext = cancel

	defer func() {
		if rerr != nil {
			if err := m.Stop(); err != nil {
				klog.Errorf("error stopping ComputeDomainManager: %v", err)
			}
		}
	}()

	err := m.informer.AddIndexers(cache.Indexers{
		"uid": uidIndexer[*nvapi.ComputeDomain],
	})
	if err != nil {
		return fmt.Errorf("error adding indexer for ComputeDomain UID: %w", err)
	}

	// Create mutation cache to track our own updates
	m.mutationCache = cache.NewIntegerResourceVersionMutationCache(
		klog.Background(),
		m.informer.GetStore(),
		m.informer.GetIndexer(),
		mutationCacheTTL,
		true,
	)

	m.podManager = NewPodManager(m.config, m.Get, m.mutationCache)

	// Use `WithKey` with hard-coded key, to cancel any previous update task (we
	// want to make sure that the latest CD status update wins).
	_, err = m.informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj any) {
			m.config.workQueue.EnqueueWithKey(obj, "cd", m.onAddOrUpdate)
		},
		UpdateFunc: func(objOld, objNew any) {
			m.config.workQueue.EnqueueWithKey(objNew, "cd", m.onAddOrUpdate)
		},
	})
	if err != nil {
		return fmt.Errorf("error adding event handlers for ComputeDomain informer: %w", err)
	}

	m.waitGroup.Add(1)
	go func() {
		defer m.waitGroup.Done()
		m.factory.Start(ctx.Done())
	}()

	if !cache.WaitForCacheSync(ctx.Done(), m.informer.HasSynced) {
		return fmt.Errorf("informer cache sync for ComputeDomains failed")
	}

	if err := m.podManager.Start(ctx); err != nil {
		return fmt.Errorf("failed to start pod manager: %w", err)
	}

	return nil
}

// Stop stops the compute domain manager.
//
//nolint:contextcheck
func (m *ComputeDomainManager) Stop() error {
	// Stop the pod manager first
	if err := m.podManager.Stop(); err != nil {
		klog.Errorf("Failed to stop pod manager: %v", err)
	}

	// Create a new context for cleanup operations since the original context might be cancelled
	cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cleanupCancel()

	// Attempt to remove this node from the ComputeDomain status before shutting down
	// Don't return error here as we still want to proceed with shutdown
	if err := m.removeNodeFromComputeDomain(cleanupCtx); err != nil {
		klog.Errorf("Failed to remove node from ComputeDomain during shutdown: %v", err)
	}

	if m.cancelContext != nil {
		m.cancelContext()
	}

	m.waitGroup.Wait()
	return nil
}

// Get gets the ComputeDomain by UID from the mutation cache.
func (m *ComputeDomainManager) Get(uid string) (*nvapi.ComputeDomain, error) {
	cds, err := getByComputeDomainUID[*nvapi.ComputeDomain](m.mutationCache, uid)
	if err != nil {
		return nil, fmt.Errorf("error retrieving ComputeDomain by UID: %w", err)
	}
	if len(cds) == 0 {
		return nil, nil
	}
	if len(cds) != 1 {
		return nil, fmt.Errorf("multiple ComputeDomains with the same UID")
	}
	return cds[0], nil
}

// onAddOrUpdate handles the addition or update of a ComputeDomain. Here, we
// receive updates not for all CDs in the system, but only for the CD that we
// are registered for (filtered by CD name). Note that the informer triggers
// this callback once upon startup for all existing objects.
func (m *ComputeDomainManager) onAddOrUpdate(ctx context.Context, obj any) error {
	// Cast the object to a ComputeDomain object
	o, ok := obj.(*nvapi.ComputeDomain)
	if !ok {
		return fmt.Errorf("failed to cast to ComputeDomain")
	}

	// Get the latest ComputeDomain object from the mutation cache (backed by
	// the informer cache) since we plan to update it later and always *must*
	// have the latest version.
	cd, err := m.Get(string(o.GetUID()))
	if err != nil {
		return fmt.Errorf("error getting latest ComputeDomain: %w", err)
	}
	if cd == nil {
		return nil
	}

	// Because the informer only filters by name:
	// Skip ComputeDomains that don't match on UUID
	if string(cd.UID) != m.config.computeDomainUUID {
		klog.Warningf("ComputeDomain processed with non-matching UID (%v, %v)", cd.UID, m.config.computeDomainUUID)
		return nil
	}

	// Update node info in ComputeDomain, if required.
	cd, err = m.EnsureNodeInfoInCD(ctx, cd)
	if err != nil {
		return fmt.Errorf("CD update: failed to insert/update node info in CD: %w", err)
	}

	m.MaybePushNodesUpdate(cd)

	return nil
}

// EnsureNodeInfoInCD makes sure that the current node (by node name) is
// represented in the `Nodes` field in the ComputeDomain object, and that it
// reports the IP address of this current pod running the CD daemon. If mutation
// is needed (first insertion, or IP address update) and successful, it reflects
// the mutation in `m.mutationCache`.
func (m *ComputeDomainManager) EnsureNodeInfoInCD(ctx context.Context, cd *nvapi.ComputeDomain) (*nvapi.ComputeDomain, error) {
	var mynode *nvapi.ComputeDomainNode

	// Create a deep copy of the ComputeDomain to avoid modifying the original
	newCD := cd.DeepCopy()

	// Try to find an existing entry for the current k8s node
	for _, node := range newCD.Status.Nodes {
		if node.Name == m.config.nodeName {
			mynode = node
			break
		}
	}

	// Detect DNS index collision -- if my self-chosen DNS index appears to be
	// used elsewhere (among the nodes with the same cliqueID), remove my
	// `ComputeDomainNode` object from the CD.status.nodes list. Rely on one of
	// the next calls into this function to make a better DNS index decision.
	if mynode != nil {
		for _, other := range newCD.Status.Nodes {
			if other.CliqueID != m.config.cliqueID {
				// Not my clique.
				continue
			}
			if other.Name == m.config.nodeName {
				// This is me.
				continue
			}
			if other.Index == mynode.Index {
				klog.V(4).Infof("EnsureNodeInfoInCD DNS index collision with %v -- remove myself from CD, later regenerate my node info", other)

				if err := m.removeNodeFromComputeDomain(ctx); err != nil {
					klog.Warningf("Failed to remove node from ComputeDomain: %v", err)
				}

				// Towards resolving this conflict fast, introduce a bit of jitter.
				time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)
				return nil, fmt.Errorf("picked the same DNS index as another node")
			}
		}
	}

	// If there is one and its IP is the same as this one, we are done
	if mynode != nil && mynode.IPAddress == m.config.podIP {
		klog.V(6).Infof("EnsureNodeInfoInCD noop: pod IP unchanged (%s)", m.config.podIP)
		return newCD, nil
	}

	// Create new ComputeDomainNode object representing myself, and insert it into the nodes list.
	if mynode == nil {
		// Get the next available index for this new node
		nextIndex, err := getNextAvailableIndex(m.config.cliqueID, newCD.Status.Nodes, m.config.maxNodesPerIMEXDomain)
		if err != nil {
			return nil, fmt.Errorf("error getting next available index: %w", err)
		}

		mynode = &nvapi.ComputeDomainNode{
			Name:     m.config.nodeName,
			CliqueID: m.config.cliqueID,
			Index:    nextIndex,
			// This is going to be switched to Ready by podmanager.
			Status: nvapi.ComputeDomainStatusNotReady,
		}

		klog.Infof("CD status does not contain node name '%s' yet, try to insert myself: %v", m.config.nodeName, mynode)
	}

	// Unconditionally update its IP address. Note that the nodeInfo.IPAddress
	// as of now translates into a pod IP address and may therefore change
	// across pod restarts.
	mynode.IPAddress = m.config.podIP

	// TODO: figure out from where to update the global status Maybe do this in
	// the controller instead? Or maybe do not do this anymore?
	// newCD.Status.Status == "" {
	//  newCD.Status.Status = nvapi.ComputeDomainStatusNotReady
	// }

	// Use server-side apply (SSA) to perform insertion or update, with a
	// localized patch affecting just one `ComputeDomainNode` item in the
	// `status.nodes` list. See
	// https://github.com/NVIDIA/k8s-dra-driver-gpu/issues/821 for context.
	// Note:
	// - The `Patch()` method requires the patch itself to be provided as
	//   byte sequence (as JSON document).
	// - The `apiVersion` and `kind`` fields are required in the patch payload.
	patchBytes, err := generatePatchForNodeInfo([]*nvapi.ComputeDomainNode{mynode})
	if err != nil {
		return nil, fmt.Errorf("could not serialize patch: %w", err)
	}

	updatedCD, err := m.patchCD(ctx, patchBytes)
	if err != nil {
		return nil, fmt.Errorf("error patching ComputeDomain status: %w", err)
	}

	// Store the latest version of the object as returned by the API server in the mutation cache.
	m.mutationCache.Mutation(updatedCD)
	klog.Infof("Successfully inserted/updated node in CD (nodeinfo: %v)", mynode)

	return updatedCD, nil
}

// The Index field in the Nodes section of the ComputeDomain status ensures a
// consistent IP-to-DNS name mapping across all machines within a given IMEX
// domain. Each node's index directly determines its DNS name using the format
// "compute-domain-daemon-{index}".
//
// getNextAvailableIndex finds the next available index for the current node by
// seeing which ones are already taken by other nodes in the ComputeDomain
// status that have the same cliqueID. It fills in gaps where it can, and returns
// an error if no index is available within maxNodesPerIMEXDomain.
//
// By filling gaps in the index sequence (rather than always appending), we
// maintain stable DNS names for existing nodes even when intermediate nodes
// are removed from the compute domain and new ones are added.
func getNextAvailableIndex(currentCliqueID string, nodes []*nvapi.ComputeDomainNode, maxNodesPerIMEXDomain int) (int, error) {
	// Filter nodes to only consider those with the same cliqueID
	var cliqueNodes []*nvapi.ComputeDomainNode
	for _, node := range nodes {
		if node.CliqueID == currentCliqueID {
			cliqueNodes = append(cliqueNodes, node)
		}
	}

	// Create a map to track used indices
	usedIndices := make(map[int]bool)

	// Collect all currently used indices from nodes with the same cliqueID
	for _, node := range cliqueNodes {
		usedIndices[node.Index] = true
	}

	// Find the next available index, starting from 0 and filling gaps
	nextIndex := 0
	for usedIndices[nextIndex] {
		nextIndex++
	}

	// Skip `maxNodesPerIMEXDomain` check in the special case of no clique ID
	// being set: this means that this node does not actually run an IMEX daemon
	// managed by us and the set of nodes in this "noop" mode in this CD is
	// allowed to grow larger than maxNodesPerIMEXDomain.
	if currentCliqueID == "" {
		return nextIndex, nil
	}

	// Ensure nextIndex is within the range 0..maxNodesPerIMEXDomain
	if nextIndex < 0 || nextIndex >= maxNodesPerIMEXDomain {
		return -1, fmt.Errorf("no available indices within maxNodesPerIMEXDomain (%d) for cliqueID %s", maxNodesPerIMEXDomain, currentCliqueID)
	}

	return nextIndex, nil
}

// If there was actually a change compared to the previously known set of
// nodes: pass info to IMEX daemon controller.
func (m *ComputeDomainManager) MaybePushNodesUpdate(cd *nvapi.ComputeDomain) {
	// When not running with the 'IMEXDaemonsWithDNSNames' feature enabled,
	// wait for all 'numNodes' nodes to show up before sending an update.
	if !featuregates.Enabled(featuregates.IMEXDaemonsWithDNSNames) {
		if len(cd.Status.Nodes) != cd.Spec.NumNodes {
			klog.Infof("numNodes: %d, nodes seen: %d", cd.Spec.NumNodes, len(cd.Status.Nodes))
			return
		}
	}

	// Do not update the IMEX daemon config if the current nodes list any
	// contains duplicate DNS indices.
	if m.HasDuplicateIndex(cd.Status.Nodes, m.config.cliqueID) {
		return
	}

	newIPs := getIPSet(cd.Status.Nodes)
	previousIPs := getIPSet(m.previousNodes)

	// Compare sets (i.e., without paying attention to order). Note: the order
	// of IP addresses written to the IMEX daemon's config file might matter (in
	// the sense that if across config files the set is equal but the order is
	// not: that may lead to an IMEX daemon startup error). Maybe we should
	// perform a stable sort of IP addresses before writing them to the nodes
	// config file. Note/TODO: we probably want to limit this check to IP
	// addresses relevant to _this_ clique.
	if !maps.Equal(newIPs, previousIPs) {
		klog.V(2).Infof("IP set changed")
		// This log message gets large for large node numbers
		klog.V(6).Infof("previous: %v; new: %v", previousIPs, newIPs)
		m.previousNodes = cd.Status.Nodes
		m.updatedNodesChan <- cd.Status.Nodes
	} else {
		klog.V(6).Infof("IP set did not change")
	}
}

func (m *ComputeDomainManager) GetNodesUpdateChan() chan []*nvapi.ComputeDomainNode {
	// Yields numNodes-size nodes updates.
	return m.updatedNodesChan
}

// removeNodeFromComputeDomain removes the current node's entry from the ComputeDomain status.
func (m *ComputeDomainManager) removeNodeFromComputeDomain(ctx context.Context) error {
	// Patch with an empty object: instructs to delete all list items owned by
	// the SSA field manager (which is precisely one:the item for this node, as
	// identified by node name).
	patchBytes, err := generatePatchForNodeInfo([]*nvapi.ComputeDomainNode{})
	if err != nil {
		return fmt.Errorf("could not serialize patch: %w", err)
	}

	updatedCD, err := m.patchCD(ctx, patchBytes)
	if err != nil {
		return fmt.Errorf("error patching ComputeDomain status for node removal: %w", err)
	}

	// Store the latest version of the object as returned by the API server in the mutation cache.
	m.mutationCache.Mutation(updatedCD)

	// TODO: figure out how to update the global CD status.
	//
	// If the number of nodes is now less than required, set status to NotReady
	// if len(updatedNodes) < newCD.Spec.NumNodes {
	// 	newCD.Status.Status = nvapi.ComputeDomainStatusNotReady
	// }

	klog.Infof("Successfully removed node with IP %s from ComputeDomain %s/%s", m.config.podIP, m.config.computeDomainNamespace, m.config.computeDomainName)
	return nil
}

// Notes:
//
// 1) Field owner and field manager are referring to the same concept.
// Specifically, `client.FieldOwner("foo")` (which is often shown in
// documentation snippets) renders as `PatchOptions{FieldManager: "foo"}`.
//
// 2) In SSA documentation, one finds the `force: true` concept -- it can be
// used to take ownership of fields that are currently owned by a different
// field manager. This is not needed in our context.
//
// 3) We cannot get away with one shared owner/manager that is used across
// nodes. SSA tracks field ownership at the field manager level, not at the
// client/process level. When a field manager applies a patch with a list,
// it's declaring the complete desired state for all entries that this field
// manager owns. When multiple writers use the same field manager name, all
// such writes are treated as coming from one logical actor. Notably, each
// apply operation must include all fields that field manager owns. If node
// A adds/updates entry "node-a" and node B adds/updates entry "node-b"
// using the same field manager, SSA interprets the omission as intent to
// delete those entries.
func (m *ComputeDomainManager) patchCD(ctx context.Context, data []byte) (*nvapi.ComputeDomain, error) {
	updatedCD, err := m.config.clientsets.Nvidia.ResourceV1beta1().ComputeDomains(m.config.computeDomainNamespace).Patch(
		ctx,
		m.config.computeDomainName,
		types.ApplyPatchType,
		data,
		metav1.PatchOptions{
			FieldManager: fmt.Sprintf("cd-writer-%s", m.config.nodeName),
		},
		"status",
	)
	return updatedCD, err
}

func getIPSet(nodeInfos []*nvapi.ComputeDomainNode) IPSet {
	set := make(IPSet)
	for _, n := range nodeInfos {
		set[n.IPAddress] = struct{}{}
	}
	return set
}

func generatePatchForNodeInfo(nodes []*nvapi.ComputeDomainNode) ([]byte, error) {
	patch := map[string]interface{}{
		"apiVersion": "resource.nvidia.com/v1beta1",
		"kind":       "ComputeDomain",
		"status": map[string]interface{}{
			"nodes": nodes,
		},
	}
	patchBytes, err := json.Marshal(patch)
	return patchBytes, err
}

// HasDuplicateIndex iterates over the list of ComputeDomainNodes (in this CD,
// and in this clique), and returns true if any Index appears more than once.
func (m *ComputeDomainManager) HasDuplicateIndex(nodeInfos []*nvapi.ComputeDomainNode, cliqueID string) bool {
	seen := make(map[int]struct{})

	for _, node := range nodeInfos {
		// Ignore nodes in a different clique.
		if node.CliqueID != cliqueID {
			continue
		}

		if _, exists := seen[node.Index]; exists {
			klog.V(4).Infof("DNS index collision detected: %v uses an index seen before (we are node %v)", node, m.config.nodeName)
			return true
		}

		// Mark as seen.
		seen[node.Index] = struct{}{}
	}

	return false
}
