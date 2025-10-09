#!/bin/bash

# Test script for stress testing nvbandwidth.

# Set overall timeout to 5 minutes
TIMEOUT=300  # 5 minutes in seconds

CURRENT_DIR="$(cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd)"
SPECS_DIR="${CURRENT_DIR}/../specs"
PROJECT_DIR="${CURRENT_DIR}/../../.."

source "${CURRENT_DIR}/helpers.sh"

CD_TMPL_FILE="${SPECS_DIR}/imex/channel-injection-cd.tmpl.yaml"
POD_TMPL_FILE="${SPECS_DIR}/imex/channel-injection-pod.tmpl.yaml"

# Start background timer
( sleep $TIMEOUT; kill -ALRM $$ ) &
TIMEOUT_PID=$!

# Set up cleanup handler
cleanup_handler() {
     EXIT_CODE="$?"
     INDEX=1 envsubst < "${CD_TMPL_FILE}" | kubectl delete --wait=false -f - > /dev/null 2>&1 || true
     INDEX=1 envsubst < "${POD_TMPL_FILE}" | kubectl delete --wait=false -f - > /dev/null 2>&1 || true
     INDEX=2 envsubst < "${CD_TMPL_FILE}" | kubectl delete --wait=false -f - > /dev/null 2>&1 || true
     INDEX=2 envsubst < "${POD_TMPL_FILE}" | kubectl delete --wait=false -f - > /dev/null 2>&1 || true
     kill $TIMEOUT_PID > /dev/null 2>&1 || true
     exit ${EXIT_CODE}
}

# Set up timeout handler
timeout_handler() {
    cleanup_handler
    echo "ERROR: Script exited without completing"
    exit 1
}

# Set up traps
trap cleanup_handler EXIT SIGINT SIGTERM
trap timeout_handler SIGALRM

# Prepare test
helm uninstall -n nvidia-dra-driver-gpu nvidia-dra-driver-gpu 2>&1 || true

if ! helm install nvidia-dra-driver-gpu ${PROJECT_DIR}/deployments/helm/nvidia-dra-driver-gpu \
    --wait \
    --create-namespace \
    --namespace nvidia-dra-driver-gpu \
    --set nvidiaDriverRoot=/run/nvidia/driver \
    --set resources.gpus.enabled=false \
    --set featureGates.IMEXDaemonsWithDNSNames=true; then
    echo "ERROR: Failed to install nvidia-dra-driver-gpu"
    exit 1
fi

# Begin test

# Apply first CD
echo "Applying first compute domain..."
if ! INDEX=1 envsubst < "${CD_TMPL_FILE}" | kubectl apply -f -; then
    echo "ERROR: Failed to apply first compute domain"
    exit 1
fi

# Apply first pod
echo "Applying first pod..."
if ! INDEX=1 envsubst < "${POD_TMPL_FILE}" | kubectl apply -f -; then
    echo "ERROR: Failed to apply first pod"
    exit 1
fi

# Wait for first pod running
echo "Waiting for first pod to be running..."
kubectl wait --for=condition=Ready pod imex-channel-injection-1 || {
    echo "ERROR: First pod failed to start"
    exit 1
}

# Get the node name of the first pod
echo "Getting node name of first pod..."
NODE_NAME=$(kubectl get pod imex-channel-injection-1 -o jsonpath='{.spec.nodeName}')
echo "Node name is ${NODE_NAME}"

# Delete first pod
echo "Deleting first pod..."
INDEX=1 envsubst < "${POD_TMPL_FILE}" | kubectl delete -f -

# Apply second CD
echo "Applying second compute domain..."
if ! INDEX=2 envsubst < "${CD_TMPL_FILE}" | kubectl apply -f -; then
    echo "ERROR: Failed to apply second compute domain"
    exit 1
fi

# Apply second pod
echo "Applying second pod..."
if ! INDEX=2 NODE_SELECTOR="kubernetes.io/hostname: ${NODE_NAME}" envsubst < "${POD_TMPL_FILE}" | kubectl apply -f -; then
    echo "ERROR: Failed to apply second pod"
    exit 1
fi

# Wait for second pod running
echo "Waiting for second pod to be running..."
kubectl wait --for=condition=Ready pod imex-channel-injection-2 || {
    echo "ERROR: Second pod failed to start"
    exit 1
}

# Test success
echo "Test completed successfully."
