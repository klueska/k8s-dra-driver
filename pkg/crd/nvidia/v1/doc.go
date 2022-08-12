/*
 * Copyright (c) 2022, NVIDIA CORPORATION.  All rights reserved.
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

//go:generate rm -f zz_generated.deepcopy.go
//go:generate controller-gen object:headerFile=../../../../hack/boilerplate.go.txt,year=2022 paths=./ output:object:dir=./

//go:generate rm -rf ../../../../deployments/static/crds
//go:generate controller-gen crd:crdVersions=v1 paths=./ output:crd:dir=../../../../deployments/static/crds

//go:generate rm -rf ../clientset
//go:generate client-gen --go-header-file=../../../../hack/boilerplate.go.txt --clientset-name "versioned" --build-tag="ignore_autogenerated" --output-package "github.com/NVIDIA/k8s-dra-driver/pkg/crd/nvidia/clientset" --input-base "github.com/NVIDIA/k8s-dra-driver/pkg/crd" --output-base "./tmp_clientset" --input "nvidia/v1"
//go:generate mv ./tmp_clientset/github.com/NVIDIA/k8s-dra-driver/pkg/crd/nvidia/clientset ../clientset
//go:generate rm -rf ./tmp_clientset

// +k8s:deepcopy-gen=package
// +groupName=dra.gpu.nvidia.com

package v1
