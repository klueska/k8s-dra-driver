#!/bin/bash

# Test script for stress testing imex channel injection.

# Set per iteration timeout to 5 minutes
TIMEOUT=300  # 5 minutes in seconds
MAX_ITER=100

CURRENT_DIR="$(cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd)"
SPECS_DIR="${CURRENT_DIR}/../specs"
PROJECT_DIR="${CURRENT_DIR}/../../.."

# Source the imex channel test library
source "${CURRENT_DIR}/helpers.sh"

CD_TMPL_FILE="${SPECS_DIR}/imex/channel-injection-cd.tmpl.yaml"
POD_TMPL_FILE="${SPECS_DIR}/imex/channel-injection-pod.tmpl.yaml"

# Set up cleanup handler
cleanup_handler() {
    EXIT_CODE="$?" 
    INDEX=1 envsubst < "${CD_TMPL_FILE}" | kubectl delete --wait=false -f - > /dev/null 2>&1 || true
    INDEX=1 envsubst < "${POD_TMPL_FILE}" | kubectl delete --wait=false -f - > /dev/null 2>&1 || true
    exit ${EXIT_CODE}
}

# Set up traps
trap cleanup_handler EXIT SIGINT SIGTERM

# Prepare test
#helm uninstall -n nvidia-dra-driver-gpu nvidia-dra-driver-gpu 2>&1 || true
#
#if ! helm install nvidia-dra-driver-gpu ${PROJECT_DIR}/deployments/helm/nvidia-dra-driver-gpu \
#    --wait \
#    --create-namespace \
#    --namespace nvidia-dra-driver-gpu \
#    --set nvidiaDriverRoot=/run/nvidia/driver \
#    --set resources.gpus.enabled=false \
#    --set featureGates.IMEXDaemonsWithDNSNames=true; then
#    echo "ERROR: Failed to install nvidia-dra-driver-gpu"
#    exit 1
#fi

# Begin test
for ((i=1; i<=MAX_ITER; i++)); do
    echo -e "\nStarting iteration $i"

    if ! INDEX=1 envsubst < "${CD_TMPL_FILE}" | kubectl delete -f -; then
        echo "Warning: Failed to delete existing compute domain (may not exist)"
    fi
    
    if ! INDEX=1 envsubst < "${POD_TMPL_FILE}" | kubectl delete -f -; then
        echo "Warning: Failed to delete existing pod (may not exist)"
    fi
    
    if ! INDEX=1 envsubst < "${CD_TMPL_FILE}" | kubectl apply -f -; then
        echo "ERROR: Failed to apply compute domain in iteration $i"
        exit 1
    fi

    if ! INDEX=1 envsubst < "${POD_TMPL_FILE}" | kubectl apply -f -; then
        echo "ERROR: Failed to apply pod in iteration $i"
        exit 1
    fi

    # Wait up to TIMEOUT seconds for the pod to enter the running state
    SECONDS=0
    while true; do
        STATUS=$(kubectl get pod imex-channel-injection-1 -o jsonpath="{.status.phase}" 2>/dev/null || echo "NotFound")

        # Pod succeeded
        if [ "$STATUS" == "Running" ]; then
            echo "Pod running successfully."
            
            # Test imex channel injection
            if ! test_imex_channel_injection "imex-channel-injection-1"; then
                echo "ERROR: imex channel injection test failed in iteration $i"
                exit 1
            fi

            INDEX=1 envsubst < "${CD_TMPL_FILE}" | kubectl apply -f -
            INDEX=1 envsubst < "${POD_TMPL_FILE}" | kubectl apply -f -
            break

        # Pod failed
        elif [ "$STATUS" == "Failed" ]; then
            echo "ERROR: Pod failed in iteration $i"
            exit 1

        # Timeout reached
        elif [ "$SECONDS" -ge $TIMEOUT ]; then
            echo "ERROR: Timeout reached ($TIMEOUT seconds) in iteration $i"
            exit 1
        fi

        sleep 5
    done

done

echo "Finished $MAX_ITER iterations successfully."
