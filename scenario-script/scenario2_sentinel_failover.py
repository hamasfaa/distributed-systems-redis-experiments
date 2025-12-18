#!/usr/bin/env python3
"""
Skenario 2: Failover pada Redis Sentinel
Tujuan: Menguji leader election otomatis
Aspek: Leader election, availability, CAP trade-off
"""

import redis
from redis.sentinel import Sentinel
import time
from datetime import datetime
import json

# Configuration
SENTINEL_HOSTS = [
    ('134.209.106.37', 26379),  # Ganti dengan IP VPS2
    ('134.209.106.37', 26380),
    ('134.209.106.37', 26381)
]
MASTER_NAME = 'mymaster'
CHECK_INTERVAL = 2  # seconds
VPS2_HOST = '134.209.106.37'

IP_PORT_MAPPING = {
    '172.18.0.2': ('134.209.106.37', 6379),  # redis-master
    '172.18.0.3': ('134.209.106.37', 6380),  # redis-replica-1
    '172.18.0.4': ('134.209.106.37', 6381),  # redis-replica-2
    '172.18.0.5': ('134.209.106.37', 6379),  # Possible IP after restart
    '172.18.0.6': ('134.209.106.37', 6380),  # Possible IP after restart
    '172.18.0.7': ('134.209.106.37', 6381),  # Possible IP after restart
}

def connect_sentinel():
    """Connect to Redis Sentinel"""
    try:
        sentinel = Sentinel(SENTINEL_HOSTS, socket_timeout=5)
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
        print(f"✗ Failed to get master info: {e}")
        return None

def get_replicas_info(sentinel):
    """Get current replicas information"""
    try:
        replicas = sentinel.discover_slaves(MASTER_NAME)
        return replicas
    except Exception as e:
        print(f"✗ Failed to get replicas info: {e}")
        return []

def map_internal_to_external(internal_ip, internal_port):
    """Map internal Docker IP to external accessible IP+port"""
    if internal_ip in IP_PORT_MAPPING:
        return IP_PORT_MAPPING[internal_ip]
    # Fallback to original if not in mapping
    return (internal_ip, internal_port)

def test_write(sentinel):
    """Test writing to master"""
    try:
        # Get master address from sentinel
        master_addr = sentinel.discover_master(MASTER_NAME)
        internal_ip, internal_port = master_addr
        
        # Map to external accessible address
        external_ip, external_port = map_internal_to_external(internal_ip, internal_port)
        
        # Connect directly using external address
        master = redis.Redis(host=external_ip, port=external_port, socket_timeout=10)
        master.ping()  # Test connection
        
        test_key = f"failover_test_{datetime.now().timestamp()}"
        master.set(test_key, "test_value")
        master.close()
        return True, test_key
    except Exception as e:
        return False, str(e)

def monitor_cluster_state(sentinel):
    """Monitor and display cluster state"""
    master = get_master_info(sentinel)
    replicas = get_replicas_info(sentinel)
    
    print(f"  Master: {master[0]}:{master[1]}")
    print(f"  Replicas: {len(replicas)}")
    for i, replica in enumerate(replicas, 1):
        print(f"    Replica {i}: {replica[0]}:{replica[1]}")
    
    return master, replicas

def run_scenario_2():
    """Run failover test scenario"""
    print("\n" + "="*70)
    print("SKENARIO 2: REDIS SENTINEL FAILOVER TEST")
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
    
    # Monitoring setup
    print("\n" + "="*70)
    print("FAILOVER MONITORING")
    print("="*70)
    print("\n⚠ INSTRUCTIONS:")
    print("  1. The script is now monitoring the cluster")
    print("  2. Open another terminal and run:")
    print("     docker stop redis-master")
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
                        'old_master': f"{current_master[0]}:{current_master[1]}"
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
                    print(f"  Old Master: {current_master[0]}:{current_master[1]}")
                    print(f"  New Master: {new_master[0]}:{new_master[1]}")
                    print(f"  Failover Duration: {failover_duration:.2f} seconds")
                    print(f"{'='*70}\n")
                    
                    failover_events.append({
                        'timestamp': timestamp,
                        'event': 'Failover completed',
                        'old_master': f"{current_master[0]}:{current_master[1]}",
                        'new_master': f"{new_master[0]}:{new_master[1]}",
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
                    print(f"[{timestamp}] Status: Master={new_master[0]}:{new_master[1]}, Replicas={len(replicas)}")
    
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
            'master': f"{initial_master[0]}:{initial_master[1]}",
            'replicas': [f"{r[0]}:{r[1]}" for r in initial_replicas]
        },
        'final_state': {
            'master': f"{final_master[0]}:{final_master[1]}",
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