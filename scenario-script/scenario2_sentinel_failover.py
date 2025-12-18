#!/usr/bin/env python3
"""
Skenario 2: Failover pada Redis Sentinel (Modified for Local Run)
Tujuan: Menguji leader election otomatis dari Laptop Lokal
"""

import redis
from redis.sentinel import Sentinel
import time
from datetime import datetime
import json

VPS_PUBLIC_IP = '134.209.106.37'  # IP VPS 

IP_MAP = {
    '172.18.0.2': 6379,   # redis-master
    '172.18.0.4': 6380,   # redis-replica-1
    '172.18.0.3': 6381,   # redis-replica-2
    '172.18.0.5': 26379,  # sentinel-1
    '172.18.0.7': 26380,  # sentinel-2
    '172.18.0.6': 26381   # sentinel-3
}

# --- 2. HOST NAME MAPPING (IP -> Container Name) ---
HOST_MAP = {
    '172.18.0.2': 'redis-master',
    '172.18.0.4': 'redis-replica-1',
    '172.18.0.3': 'redis-replica-2',
    '172.18.0.5': 'sentinel-1',
    '172.18.0.7': 'sentinel-2',
    '172.18.0.6': 'sentinel-3'
}

# Sentinel Connection List (Internal IPs)
SENTINEL_INTERNAL_HOSTS = [
    ('172.18.0.5', 26379),
    ('172.18.0.7', 26379),
    ('172.18.0.6', 26379)
]

MASTER_NAME = 'mymaster'
CHECK_INTERVAL = 2  # seconds

def get_node_info(ip):
    """Helper to get nice string: 'redis-master (172.18.0.2)'"""
    name = HOST_MAP.get(ip, "Unknown-Host")
    port = IP_MAP.get(ip, "?")
    return f"{name} [{ip}] -> Port {port}"

def connect_sentinel():
    """Connect to Redis Sentinel with IP Mapping"""
    try:
        mapped_sentinels = []
        print("Connecting to Sentinel Cluster (Mapping Mode)...")
        
        for internal_ip, internal_port in SENTINEL_INTERNAL_HOSTS:
            if internal_ip in IP_MAP:
                external_port = IP_MAP[internal_ip]
                mapped_sentinels.append((VPS_PUBLIC_IP, external_port))
            else:
                print(f"  ⚠ Warning: No mapping found for Sentinel {internal_ip}")
        
        if not mapped_sentinels:
            print("✗ No reachable sentinels found in map. Using default...")
            sentinel = Sentinel([(VPS_PUBLIC_IP, 26379)], socket_timeout=5)
        else:
            sentinel = Sentinel(mapped_sentinels, socket_timeout=5)

        print(f"✓ Connected to Sentinel cluster")
        return sentinel
    except Exception as e:
        print(f"✗ Failed to connect to Sentinel: {e}")
        return None

def get_master_info(sentinel):
    """Get current master information"""
    try:
        master_address = sentinel.discover_master(MASTER_NAME)
        return master_address
    except Exception as e:
        return None

def get_replicas_info(sentinel):
    """Get current replicas information"""
    try:
        replicas = sentinel.discover_slaves(MASTER_NAME)
        return replicas
    except Exception as e:
        print(f"✗ Failed to get replicas info: {e}")
        return []

def test_write(sentinel):
    """Test writing to master (Modified for Local Access)"""
    try:
        master_info = sentinel.discover_master(MASTER_NAME)
        if not master_info: return False, "Master not found"
        
        internal_ip = master_info[0]
        
        if internal_ip in IP_MAP:
            external_port = IP_MAP[internal_ip]
            master = redis.Redis(host=VPS_PUBLIC_IP, port=external_port, socket_timeout=5, decode_responses=True)
        else:
            print(f"⚠ Warning: IP {internal_ip} not found in IP_MAP. Cannot route connection.")
            return False, "IP Mapping Error"

        test_key = f"failover_test_{datetime.now().timestamp()}"
        master.set(test_key, "test_value")
        return True, test_key
    except Exception as e:
        return False, str(e)

def monitor_cluster_state(sentinel):
    """Monitor and display cluster state"""
    master = get_master_info(sentinel)
    replicas = get_replicas_info(sentinel)
    
    if master:
        print(f"  Current Master: {get_node_info(master[0])}")
    
    print(f"  Replicas: {len(replicas)}")
    for i, replica in enumerate(replicas, 1):
        print(f"    Replica {i}: {get_node_info(replica[0])}")
    
    return master, replicas

def run_scenario_2():
    """Run failover test scenario"""
    print("\n" + "="*70)
    print("SKENARIO 2: REDIS SENTINEL FAILOVER TEST (LOCAL MODE)")
    print("="*70 + "\n")
    
    # Connect to Sentinel
    sentinel = connect_sentinel()
    if not sentinel:
        print("✗ Cannot proceed: Failed to connect to Sentinel")
        return
    
    print(f"\nTest started at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    # Display initial state
    print("INITIAL CLUSTER STATE:")
    print("-"*70)
    initial_master, initial_replicas = monitor_cluster_state(sentinel)
    
    # Test initial write
    print("\nTesting initial write to master...")
    success, result = test_write(sentinel)
    if success:
        print(f"✓ Write successful: {result}")
    else:
        print(f"✗ Write failed: {result}")
        print("  (TIP: Check your VPS firewall or if docker is running)")
    
    # Monitoring setup
    print("\n" + "="*70)
    print("FAILOVER MONITORING")
    print("="*70)
    print("\n⚠ INSTRUCTIONS:")
    print("  1. The script is now monitoring the cluster")
    print("  2. Open another terminal (SSH to VPS) and run:")
    print("     docker stop redis-master")
    print("     (Or stop whichever node is currently the Master)")
    print("  3. Watch the failover happen automatically")
    print("  4. Press Ctrl+C to stop monitoring\n")
    print("-"*70)
    
    # Monitoring loop
    failover_events = []
    current_master = initial_master
    failover_detected = False
    failover_start_time = None
    failover_end_time = None
    
    iteration = 0
    max_iterations = 300  # 10 minutes max (2 sec interval)
    
    try:
        while iteration < max_iterations:
            iteration += 1
            time.sleep(CHECK_INTERVAL)
            
            timestamp = datetime.now().strftime('%H:%M:%S')
            
            # Get current master
            new_master = get_master_info(sentinel)
            
            if new_master is None:
                print(f"[{timestamp}] ⚠ Cannot detect master - Possible failover in progress...")
                if not failover_detected:
                    failover_detected = True
                    failover_start_time = time.time()
                    failover_events.append({
                        'timestamp': timestamp,
                        'event': 'Master down detected',
                        'old_master': f"{current_master[0]}:{current_master[1]}" if current_master else "Unknown"
                    })
                continue
            
            # Check if master changed (failover happened)
            if new_master != current_master:
                if not failover_end_time:
                    failover_end_time = time.time()
                    failover_duration = failover_end_time - failover_start_time if failover_start_time else 0
                    
                    print(f"\n{'='*70}")
                    print(f"[{timestamp}] 🔄 FAILOVER DETECTED!")
                    print(f"{'='*70}")
                    
                    old_name = get_node_info(current_master[0]) if current_master else "Unknown"
                    new_name = get_node_info(new_master[0])
                    
                    print(f"  Old Master: {old_name}")
                    print(f"  New Master: {new_name}")
                    print(f"  Failover Duration: {failover_duration:.2f} seconds")
                    print(f"{'='*70}\n")
                    
                    failover_events.append({
                        'timestamp': timestamp,
                        'event': 'Failover completed',
                        'old_master': old_name,
                        'new_master': new_name,
                        'duration': failover_duration
                    })
                    
                    current_master = new_master
                    
                    # Test write after failover
                    print("Testing write to new master...")
                    success, result = test_write(sentinel)
                    if success:
                        print(f"✓ Write successful to new master: {result}\n")
                        failover_events.append({
                            'timestamp': datetime.now().strftime('%H:%M:%S'),
                            'event': 'Write test successful',
                            'key': result
                        })
                    else:
                        print(f"✗ Write failed to new master: {result}\n")
                        failover_events.append({
                            'timestamp': datetime.now().strftime('%H:%M:%S'),
                            'event': 'Write test failed',
                            'error': result
                        })
            else:
                # Normal monitoring
                if iteration % 10 == 0:  # Print every 20 seconds
                    replicas = get_replicas_info(sentinel)
                    master_node = get_node_info(new_master[0])
                    print(f"[{timestamp}] Master: {master_node}")
    
    except KeyboardInterrupt:
        print("\n\n✓ Monitoring stopped by user")
    
    # Final state
    print("\n" + "="*70)
    print("FINAL CLUSTER STATE:")
    print("-"*70)
    final_master, final_replicas = monitor_cluster_state(sentinel)
    
    # Summary
    print("\n" + "="*70)
    print("FAILOVER SUMMARY")
    print("="*70)
    
    if failover_events:
        print(f"\nTotal Events: {len(failover_events)}")
        for i, event in enumerate(failover_events, 1):
            print(f"\n{i}. [{event['timestamp']}] {event['event']}")
            for key, value in event.items():
                if key not in ['timestamp', 'event']:
                    print(f"   {key}: {value}")
    else:
        print("\n⚠ No failover events detected")
    
    # Save results
    result_data = {
        'scenario': 'Redis Sentinel Failover',
        'timestamp': datetime.now().isoformat(),
        'initial_state': {
            'master': f"{initial_master[0]}:{initial_master[1]}" if initial_master else "Unknown",
            'replicas': [f"{r[0]}:{r[1]}" for r in initial_replicas]
        },
        'final_state': {
            'master': f"{final_master[0]}:{final_master[1]}" if final_master else "Unknown",
            'replicas': [f"{r[0]}:{r[1]}" for r in final_replicas]
        },
        'failover_events': failover_events,
        'failover_occurred': len(failover_events) > 0
    }
    
    output_file = f"scenario2_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(output_file, 'w') as f:
        json.dump(result_data, f, indent=2)
    
    print(f"\n✓ Results saved to {output_file}")
    print("="*70 + "\n")

if __name__ == "__main__":
    try:
        run_scenario_2()
    except Exception as e:
        print(f"\n✗ Error during test: {e}")
        import traceback
        traceback.print_exc()