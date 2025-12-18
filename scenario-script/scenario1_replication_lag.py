#!/usr/bin/env python3
"""
Skenario 1: Replication Lag & Consistency Test
Tujuan: Mengamati konsistensi antara master dan replica
Aspek: Eventual consistency, replication delay
"""

import redis
import time
from datetime import datetime
import json

# Configuration
REDIS_MASTER_HOST = '134.209.106.37'  # IP VPS2
REDIS_MASTER_PORT = 6379
REDIS_REPLICA_1_HOST = '134.209.106.37'  # IP VPS2
REDIS_REPLICA_1_PORT = 6380
REDIS_REPLICA_2_HOST = '134.209.106.37'  # IP VPS2
REDIS_REPLICA_2_PORT = 6381

NUM_WRITES = 1000

def connect_redis(host, port, name):
    """Connect to Redis instance"""
    try:
        client = redis.Redis(host=host, port=port, decode_responses=True)
        client.ping()
        print(f"✓ Connected to {name} at {host}:{port}")
        return client
    except Exception as e:
        print(f"✗ Failed to connect to {name}: {e}")
        return None

def run_scenario_1():
    """Run replication lag and consistency test"""
    print("\n" + "="*70)
    print("SKENARIO 1: REPLICATION LAG & CONSISTENCY TEST")
    print("="*70 + "\n")
    
    # Connect to Redis instances
    master = connect_redis(REDIS_MASTER_HOST, REDIS_MASTER_PORT, "Master")
    replica1 = connect_redis(REDIS_REPLICA_1_HOST, REDIS_REPLICA_1_PORT, "Replica 1")
    replica2 = connect_redis(REDIS_REPLICA_2_HOST, REDIS_REPLICA_2_PORT, "Replica 2")
    
    if not all([master, replica1, replica2]):
        print("\n✗ Cannot proceed: Failed to connect to all Redis instances")
        return
    
    print(f"\nStarting test at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Writing {NUM_WRITES} keys to master...\n")
    
    # Clear any existing test keys
    master.flushdb()
    time.sleep(1)
    
    # Write data to master
    start_time = time.time()
    for i in range(NUM_WRITES):
        key = f"test_key:{i}"
        value = f"value_{i}_{datetime.now().timestamp()}"
        master.set(key, value)
        
        if (i + 1) % 100 == 0:
            print(f"  Written {i + 1}/{NUM_WRITES} keys...")
    
    write_duration = time.time() - start_time
    print(f"\n✓ Completed writing {NUM_WRITES} keys in {write_duration:.2f} seconds")
    print(f"  Average: {NUM_WRITES/write_duration:.2f} writes/sec\n")
    
    # Immediately read from replicas
    print("Reading from replicas immediately after write...\n")
    
    results = {
        'replica1': {'synced': 0, 'missing': 0, 'mismatched': 0},
        'replica2': {'synced': 0, 'missing': 0, 'mismatched': 0}
    }
    
    missing_keys = {'replica1': [], 'replica2': []}
    
    read_start = time.time()
    
    for i in range(NUM_WRITES):
        key = f"test_key:{i}"
        master_value = master.get(key)
        
        # Check replica 1
        replica1_value = replica1.get(key)
        if replica1_value is None:
            results['replica1']['missing'] += 1
            missing_keys['replica1'].append(key)
        elif replica1_value != master_value:
            results['replica1']['mismatched'] += 1
        else:
            results['replica1']['synced'] += 1
        
        # Check replica 2
        replica2_value = replica2.get(key)
        if replica2_value is None:
            results['replica2']['missing'] += 1
            missing_keys['replica2'].append(key)
        elif replica2_value != master_value:
            results['replica2']['mismatched'] += 1
        else:
            results['replica2']['synced'] += 1
    
    read_duration = time.time() - read_start
    
    # Display Results
    print("="*70)
    print("HASIL PENGUJIAN")
    print("="*70)
    
    print(f"\nTotal Keys: {NUM_WRITES}")
    print(f"Write Duration: {write_duration:.2f} seconds")
    print(f"Read Duration: {read_duration:.2f} seconds")
    
    for replica_name, stats in results.items():
        print(f"\n{replica_name.upper()}:")
        print(f"  ✓ Synced:      {stats['synced']:4d} ({stats['synced']/NUM_WRITES*100:.1f}%)")
        print(f"  ✗ Missing:     {stats['missing']:4d} ({stats['missing']/NUM_WRITES*100:.1f}%)")
        print(f"  ⚠ Mismatched:  {stats['mismatched']:4d} ({stats['mismatched']/NUM_WRITES*100:.1f}%)")
    
    # Wait and re-check after some time
    print("\n" + "-"*70)
    print("Waiting 5 seconds for replication to complete...")
    time.sleep(5)
    
    print("\nRe-checking consistency after wait...\n")
    
    results_after = {
        'replica1': {'synced': 0, 'missing': 0, 'mismatched': 0},
        'replica2': {'synced': 0, 'missing': 0, 'mismatched': 0}
    }
    
    for i in range(NUM_WRITES):
        key = f"test_key:{i}"
        master_value = master.get(key)
        
        # Check replica 1
        replica1_value = replica1.get(key)
        if replica1_value is None:
            results_after['replica1']['missing'] += 1
        elif replica1_value != master_value:
            results_after['replica1']['mismatched'] += 1
        else:
            results_after['replica1']['synced'] += 1
        
        # Check replica 2
        replica2_value = replica2.get(key)
        if replica2_value is None:
            results_after['replica2']['missing'] += 1
        elif replica2_value != master_value:
            results_after['replica2']['mismatched'] += 1
        else:
            results_after['replica2']['synced'] += 1
    
    print("HASIL SETELAH 5 DETIK:")
    print("="*70)
    
    for replica_name, stats in results_after.items():
        print(f"\n{replica_name.upper()}:")
        print(f"  ✓ Synced:      {stats['synced']:4d} ({stats['synced']/NUM_WRITES*100:.1f}%)")
        print(f"  ✗ Missing:     {stats['missing']:4d} ({stats['missing']/NUM_WRITES*100:.1f}%)")
        print(f"  ⚠ Mismatched:  {stats['mismatched']:4d} ({stats['mismatched']/NUM_WRITES*100:.1f}%)")
    
    # Save results to JSON
    result_data = {
        'scenario': 'Replication Lag & Consistency',
        'timestamp': datetime.now().isoformat(),
        'config': {
            'num_writes': NUM_WRITES,
            'write_duration': write_duration,
            'read_duration': read_duration
        },
        'immediate_results': results,
        'after_wait_results': results_after,
        'missing_keys_sample': {
            'replica1': missing_keys['replica1'][:10],
            'replica2': missing_keys['replica2'][:10]
        }
    }
    
    output_file = f"scenario1_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(output_file, 'w') as f:
        json.dump(result_data, f, indent=2)
    
    print(f"\n✓ Results saved to {output_file}")
    print("="*70 + "\n")
    
    # Cleanup
    master.close()
    replica1.close()
    replica2.close()

if __name__ == "__main__":
    try:
        run_scenario_1()
    except KeyboardInterrupt:
        print("\n\n✗ Test interrupted by user")
    except Exception as e:
        print(f"\n✗ Error during test: {e}")
        import traceback
        traceback.print_exc()
