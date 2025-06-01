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
	"fmt"
	"sync"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"

	nvapi "github.com/NVIDIA/k8s-dra-driver-gpu/api/nvidia.com/resource/v1beta1"
	nvinformers "github.com/NVIDIA/k8s-dra-driver-gpu/pkg/nvidia.com/informers/externalversions"
)

const (
	informerResyncPeriod = 10 * time.Minute
)

// ComputeDomainManager watches compute domains and updates their status with
// info about the ComputeDomain daemon running on this node.
type ComputeDomainManager struct {
	config        *ManagerConfig
	waitGroup     sync.WaitGroup
	cancelContext context.CancelFunc

	factory  nvinformers.SharedInformerFactory
	informer cache.SharedIndexInformer

	nodes     []*nvapi.ComputeDomainNode
	nodesChan chan struct{}
}

// NewComputeDomainManager creates a new ComputeDomainManager instance.
func NewComputeDomainManager(config *ManagerConfig) *ComputeDomainManager {
	factory := nvinformers.NewSharedInformerFactory(config.clientsets.Nvidia, informerResyncPeriod)
	informer := factory.Resource().V1beta1().ComputeDomains().Informer()

	m := &ComputeDomainManager{
		config:    config,
		factory:   factory,
		informer:  informer,
		nodesChan: make(chan struct{}),
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

	_, err := m.informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj any) {
			m.config.workQueue.Enqueue(obj, m.onAddOrUpdate)
		},
		UpdateFunc: func(objOld, objNew any) {
			m.config.workQueue.Enqueue(objNew, m.onAddOrUpdate)
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

	return nil
}

// Stop stops the compute domain manager.
func (m *ComputeDomainManager) Stop() error {
	m.cancelContext()
	m.waitGroup.Wait()
	return nil
}

// onAddOrUpdate handles the addition or update of a ComputeDomain.
func (m *ComputeDomainManager) onAddOrUpdate(ctx context.Context, obj any) error {
	cd, ok := obj.(*nvapi.ComputeDomain)
	if !ok {
		return fmt.Errorf("failed to cast to ComputeDomain")
	}

	// Update node info in ComputeDomain
	if err := m.UpdateComputeDomainNodeInfo(ctx, cd); err != nil {
		return fmt.Errorf("error updating node info in ComputeDomain: %w", err)
	}

	return nil
}

// UpdateComputeDomainNodeInfo updates the Nodes field in the ComputeDomain
// with info about the ComputeDomain daemon running on this node.
func (m *ComputeDomainManager) UpdateComputeDomainNodeInfo(ctx context.Context, cd *nvapi.ComputeDomain) error {
	var nodeInfo *nvapi.ComputeDomainNode

	// Create a deep copy of the ComputeDomain to avoid modifying the original
	newCD := cd.DeepCopy()

	// If we've reached the expected number of nodes, assign m.nodes and close
	// the channel as we exit this function to unblock the caller of
	// BlockUntilAllNodesJoinComputeDomain
	defer func() {
		if len(newCD.Status.Nodes) != newCD.Spec.NumNodes {
			return
		}
		if m.nodes == nil {
			close(m.nodesChan)
		}
		m.nodes = newCD.Status.Nodes
	}()

	// Try to find an existing entry for the current node
	for _, node := range newCD.Status.Nodes {
		if node.Name == m.config.nodeName {
			nodeInfo = node
			break
		}
	}

	// If there is one and its IP is the same as this one, we are done
	if nodeInfo != nil && nodeInfo.IPAddress == m.config.podIP {
		return nil
	}

	// If there isn't one, create one and append it to the list
	if nodeInfo == nil {
		nodeInfo = &nvapi.ComputeDomainNode{
			Name:     m.config.nodeName,
			CliqueID: m.config.cliqueID,
		}
		newCD.Status.Nodes = append(newCD.Status.Nodes, nodeInfo)
	}

	// Unconditionally update its IP address
	nodeInfo.IPAddress = m.config.podIP

	// Update the status
	if _, err := m.config.clientsets.Nvidia.ResourceV1beta1().ComputeDomains(newCD.Namespace).UpdateStatus(ctx, newCD, metav1.UpdateOptions{}); err != nil {
		return fmt.Errorf("error updating nodes in ComputeDomain status: %w", err)
	}

	return nil
}

// BlockUntilAllNodesJoinComputeDomain waits until all nodes have joined the compute domain
// and returns the list of nodes in the compute domain.
func (m *ComputeDomainManager) BlockUntilAllNodesJoinComputeDomain() []*nvapi.ComputeDomainNode {
	<-m.nodesChan
	return m.nodes
}
