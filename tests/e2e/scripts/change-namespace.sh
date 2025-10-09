#!/bin/bash

# Test script upgrading across a namespace

CURRENT_DIR="$(cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd)"
SPECS_DIR="${CURRENT_DIR}/../specs"
PROJECT_DIR="${CURRENT_DIR}/../../.."

source "${CURRENT_DIR}/helpers.sh"

NAMESPACE1="nvidia-dra-driver-gpu"
NAMESPACE2="gpu-operator"

helm uninstall -n ${NAMESPACE1} nvidia-dra-driver-gpu 2>&1 || true
helm uninstall -n ${NAMESPACE2} nvidia-dra-driver-gpu 2>&1 || true

if ! helm install nvidia-dra-driver-gpu ${PROJECT_DIR}/deployments/helm/nvidia-dra-driver-gpu \
    --wait \
    --create-namespace \
    --namespace=${NAMESPACE1} \
    --set nvidiaDriverRoot=/run/nvidia/driver \
    --set resources.gpus.enabled=false; then
    echo "ERROR: Failed to install nvidia-dra-driver-gpu"
    exit 1
fi

kubectl apply -f imex-channel-injection.yaml
sleep 5
kubectl get pod -A

helm uninstall -n ${NAMESPACE1} nvidia-dra-driver-gpu

if ! helm install nvidia-dra-driver-gpu ${PROJECT_DIR}/deployments/helm/nvidia-dra-driver-gpu \
    --wait \
    --create-namespace \
    --namespace=${NAMESPACE2} \
    --set nvidiaDriverRoot=/run/nvidia/driver \
    --set resources.gpus.enabled=false \
    --set controller.containers.computeDomain.env[0].name=ADDITIONAL_NAMESPACES \
    --set controller.containers.computeDomain.env[0].value=${NAMESPACE1}; then
    echo "ERROR: Failed to install nvidia-dra-driver-gpu"
    exit 1
fi

kubectl delete -f imex-channel-injection.yaml
sleep 5
kubectl get pod -A

helm uninstall -n ${NAMESPACE2} nvidia-dra-driver-gpu
