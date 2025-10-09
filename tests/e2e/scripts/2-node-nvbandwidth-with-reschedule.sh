#!/bin/bash

# Test script for stress testing nvbandwidth.

# Set overall timeout to 5 minutes
TIMEOUT=300  # 5 minutes in seconds

CURRENT_DIR="$(cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd)"
SPECS_DIR="${CURRENT_DIR}/../specs"
PROJECT_DIR="${CURRENT_DIR}/../../.."

source "${CURRENT_DIR}/helpers.sh"

INDEX="${INDEX:=1}"
TEST_FILE="${SPECS_DIR}/imex/2-node-nvbandwidth.tmpl.yaml"
CD_NAME="nvbandwidth-test-compute-domain-${INDEX}"
LAUNCHER_POD_LABEL="nvbandwidth-test-replica=mpi-launcher-${INDEX}"
WORKER_POD_LABEL="nvbandwidth-test-replica=mpi-worker-${INDEX}"

export INDEX

# Start background timer
( sleep $TIMEOUT; kill -ALRM $$ ) &
TIMEOUT_PID=$!

# Set up cleanup handler
cleanup_handler() {
     EXIT_CODE="$?"
     envsubst < "${TEST_FILE}" | kubectl delete --wait=false -f - > /dev/null 2>&1 || true
     kubectl label nodes -l nvbandwidth-test=${INDEX} nvbandwidth-test- > /dev/null 2>&1 || true
     kill $TIMEOUT_PID > /dev/null 2>&1 || true
     echo ""
     exit ${EXIT_CODE}
}

# Set up timeout handler
timeout_handler() {
    cleanup_handler
    echo "ERROR: Script exited with timeout"
    exit 1
}

# Set up traps
trap cleanup_handler EXIT SIGINT SIGTERM
trap timeout_handler SIGALRM

## Prepare test
#helm uninstall -n nvidia-dra-driver-gpu nvidia-dra-driver-gpu 2>&1 || true
#
#if ! helm install nvidia-dra-driver-gpu ${PROJECT_DIR}/deployments/helm/nvidia-dra-driver-gpu \
#    --wait \
#    --create-namespace \
#    --namespace nvidia-dra-driver-gpu \
#    --set resources.gpus.enabled=false \
#    --set featureGates.IMEXDaemonsWithDNSNames=true; then
#    echo "ERROR: Failed to install nvidia-dra-driver-gpu"
#    exit 1
#fi

# Begin test

# Pick two worker nodes to run the test on and label them
while true; do
  for node in $(kubectl get nodes -o jsonpath='{.items[*].metadata.name}'); do
    existing=$(kubectl get node "${node}" -o jsonpath='{.metadata.labels.nvbandwidth-test}')
    [ -n "${existing}" ] && continue
    kubectl label node "${node}" nvbandwidth-test=${INDEX} --overwrite=false
    labeled=$(kubectl get nodes -l nvbandwidth-test=${INDEX} -o jsonpath='{.items[*].metadata.name}' | wc -w)
    [ "${labeled}" -ge 2 ] && break 2
  done
  sleep 1
done

# Apply the compute domain and workload
if ! envsubst < "${TEST_FILE}" | kubectl apply -f -; then
    echo "ERROR: Failed to apply test file"
    exit 1
fi

# Get the compute domain UID
CD_UID=$(kubectl get computedomain ${CD_NAME} -o jsonpath="{.metadata.uid}")

# Wait for launcher pod to appear and be ready
echo -e "Waiting for launcher pod to be running..."
while ! kubectl get pods -l ${LAUNCHER_POD_LABEL} --no-headers 2>/dev/null | grep -q .; do
    sleep 5
done
kubectl wait --for=condition=Ready pod -l ${LAUNCHER_POD_LABEL} --timeout=-1s || {
    echo "ERROR: Initial pod failed to start"
    exit 1
}

# Remove the label to force the worker pod to reschedule when deleted
node=$(kubectl get nodes -l nvbandwidth-test=${INDEX} -o jsonpath='{.items[0].metadata.name}')
kubectl label node "${node}" nvbandwidth-test- --overwrite=true

# Delete the worker pod from the node where the label was removed
echo "Deleted worker pod"
pod=$(kubectl get pods -l ${WORKER_POD_LABEL} --field-selector=spec.nodeName=${node} -o jsonpath='{.items[0].metadata.name}')
kubectl delete pod "${pod}"

# Put the the label on a different node to allow the worker pod to schedule there
while true; do
  for newnode in $(kubectl get nodes -o jsonpath='{.items[*].metadata.name}'); do
    [ "${newnode}" = "${node}" ] && continue
    existing=$(kubectl get node "${newnode}" -o jsonpath='{.metadata.labels.nvbandwidth-test}')
    [ -n "${existing}" ] && continue
    kubectl label node "${newnode}" nvbandwidth-test=${INDEX} --overwrite=false
    labeled=$(kubectl get nodes -l nvbandwidth-test=${INDEX} -o jsonpath='{.items[*].metadata.name}' | wc -w)
    [ "${labeled}" -ge 2 ] && break 2
  done
  sleep 1
done

# Wait for new worker pod to be running
echo "Waiting for new worker pod to be running..."
kubectl wait --for=condition=Ready pod -l ${WORKER_POD_LABEL} --timeout=-1s || {
    echo "ERROR: New worker pod failed to start"
    exit 1
}

# Delete both daemon pods
kubectl delete pod -n nvidia-dra-driver-gpu -l resource.nvidia.com/computeDomain=${CD_UID} --grace-period=0 --force
echo "Deleted daemon pods"

# Wait for launcher pod to complete
echo "Waiting for launcher pod to complete..."
POD_STATUS=$(kubectl get pod -l ${LAUNCHER_POD_LABEL} -o jsonpath="{.items[0].status.phase}" 2>/dev/null)
while [ "$POD_STATUS" != "Succeeded" ]; do
    POD_STATUS=$(kubectl get pod -l ${LAUNCHER_POD_LABEL} -o jsonpath="{.items[0].status.phase}" 2>/dev/null)
    sleep 1
done

echo "Test completed successfully."
