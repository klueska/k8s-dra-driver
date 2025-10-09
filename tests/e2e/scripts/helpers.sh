#!/bin/bash

# Library functions for testing imex channel injection
# This file should be sourced by test scripts, not executed directly

# Function to test imex channel device injection
test_imex_channel_injection() {
    if [ $# -eq 0 ] || [ -z "$1" ]; then
        echo "ERROR: test_imex_channel_injection requires a pod name argument"
        echo "Usage: test_imex_channel_injection <pod_name>"
        return 1
    fi
    
    local pod_name="$1"
    local channel_path="/dev/nvidia-caps-imex-channels/channel0"
    
    echo "Testing imex channel device injection in pod $pod_name..."
    
    # Check if the device file exists
    echo "Checking if $channel_path exists..."
    if ! kubectl exec $pod_name -- test -e "$channel_path"; then
        echo "ERROR: Device file $channel_path does not exist"
        return 1
    fi
    echo "✓ Device file $channel_path exists"
    
    # Check file permissions and type
    echo "Checking device file properties..."
    local file_info=$(kubectl exec $pod_name -- ls -la "$channel_path" 2>/dev/null)
    
    # Verify it's a character device (starts with 'c')
    if ! echo "$file_info" | grep -q "^c"; then
        echo "ERROR: $channel_path is not a character device"
        return 1
    fi
    echo "✓ Device file is a character device"
    
    # Test that we have cgroup access to the device (not just a dummy device
    # file). We verify this by checking that reading produces "Invalid
    # argument" error, which indicates the device is accessible rather than a
    # permission error (expected behavior).
    if ! test_cgroup_access "$pod_name" "$channel_path"; then
        echo "ERROR: Device cgroup access test failed - device may be dummy or inaccessible"
        return 1
    fi
    
    echo "✓ All imex channel injection tests passed!"
    return 0
}

# Function to test cgroup access
test_cgroup_access() {
    if [ $# -lt 2 ] || [ -z "$1" ] || [ -z "$2" ]; then
        echo "ERROR: test_cgroup_access requires pod name and device path arguments"
        echo "Usage: test_cgroup_access <pod_name> <device_path>"
        return 1
    fi
    
    local pod_name="$1"
    local device_path="$2"
    
    echo "Testing cgroup access to device $device_path..."
    
    # Capture both stdout and stderr
    local read_output
    local read_exit_code
    
    read_output=$(kubectl exec $pod_name -- cat "$device_path" 2>&1 || read_exit_code=$?)
    
    # Check for "Invalid argument" error which indicates proper cgroup access
    if echo "$read_output" | grep -qi "invalid argument\|EINVAL"; then
        echo "✓ Device has proper cgroup access"
        return 0
    else
        echo "ERROR: Expected 'Invalid argument' error, got: $read_output"
        return 1
    fi
}
