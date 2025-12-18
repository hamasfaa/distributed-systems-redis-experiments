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
    # --- DATA NODES (Master & Replica) ---
    '172.18.0.2': 6379,   # IP Internal redis-master -> Port External 6379
    '172.18.0.5': 6380,   # IP Internal redis-replica-1 -> Port External 6380
    '172.18.0.7': 6381,   # IP Internal redis-replica-2 -> Port External 6381

    # --- SENTINEL NODES ---
    '172.18.0.6': 26379,  # IP Internal sentinel-1 -> Port External 26379
    '172.18.0.3': 26380,  # IP Internal sentinel-2 -> Port External 26380
    '172.18.0.4': 26381   # IP Internal sentinel-3 -> Port External 26381
}

SENTINEL_INTERNAL_HOSTS = [
    ('172.18.0.6', 26379), # IP Internal sentinel-1
    ('172.18.0.3', 26379), # IP Internal sentinel-2
    ('172.18.0.4', 26379)  # IP Internal sentinel-3
]

MASTER_NAME = 'mymaster'
CHECK_INTERVAL = 2  # seconds

def connect_sentinel():
    """Connect to Redis Sentinel with IP Mapping"""
    try:
        # LOGIKA BARU: Menerjemahkan IP Internal Sentinel ke External
        mapped_sentinels = []
        print("Connecting to Sentinel Cluster (Mapping Mode)...")
        
        for internal_ip, internal_port in SENTINEL_INTERNAL_HOSTS:
            if internal_ip in IP_MAP:
                external_port = IP_MAP[internal_ip]
                print(f"  Mapping: {internal_ip} -> {VPS_PUBLIC_IP}:{external_port}")
                mapped_sentinels.append((VPS_PUBLIC_IP, external_port))
            else:
                print(f"  ⚠ Warning: No mapping found for Sentinel {internal_ip}")
        
        if not mapped_sentinels:
            print("✗ No reachable sentinels found in map. Using default...")
            # Fallback ke konfigurasi manual jika mapping gagal
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
        # print(f"✗ Failed to get master info: {e}")
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
            print(f"⚠ Warning: IP {internal_ip} tidak ada di IP_MAP. Coba pakai koneksi standar...")
            master = sentinel.master_for(MASTER_NAME, socket_timeout=5)

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
        print(f"  Master (Internal): {master[0]}:{master[1]}")
        # Tampilkan juga info mappingnya biar jelas
        if master[0] in IP_MAP:
             print(f"  -> Mapped to External: {VPS_PUBLIC_IP}:{IP_MAP[master[0]]}")
    
    print(f"  Replicas: {len(replicas)}")
    for i, replica in enumerate(replicas, 1):
        print(f"    Replica {i}: {replica[0]}:{replica[1]}")
    
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
        print("  (TIP: Pastikan IP_MAP di script sudah sesuai dengan 'docker inspect' di VPS)")
    
    # Monitoring setup
    print("\n" + "="*70)
    print("FAILOVER MONITORING")
    print("="*70)
    print("\n⚠ INSTRUCTIONS:")
    print("  1. The script is now monitoring the cluster")
    print("  2. Open another terminal (SSH to VPS) and run:")
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
                    print(f"  Old Master: {current_master[0]}:{current_master[1]}" if current_master else "Unknown")
                    print(f"  New Master: {new_master[0]}:{new_master[1]}")
                    print(f"  Failover Duration: {failover_duration:.2f} seconds")
                    print(f"{'='*70}\n")
                    
                    failover_events.append({
                        'timestamp': timestamp,
                        'event': 'Failover completed',
                        'old_master': f"{current_master[0]}:{current_master[1]}" if current_master else "Unknown",
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