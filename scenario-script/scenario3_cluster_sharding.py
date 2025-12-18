#!/usr/bin/env python3
"""
Skenario 3: Sharding pada Redis Cluster
Tujuan: Melihat distribusi key ke hash slot
Aspek: Partitioning, consistent hashing, scaling out
"""

import redis
from redis.cluster import RedisCluster
import time
from datetime import datetime
import json
from collections import defaultdict

# Configuration
CLUSTER_NODES = [
    {'host': '139.59.119.65', 'port': 7001},  # Ganti dengan IP VPS1
    {'host': '139.59.119.65', 'port': 7002},
    {'host': '139.59.119.65', 'port': 7003},
]

NUM_KEYS = 10000

def connect_cluster():
    """Connect to Redis Cluster"""
    try:
        cluster = RedisCluster(
            host=CLUSTER_NODES[0]['host'],
            port=CLUSTER_NODES[0]['port'],
            decode_responses=True,
            socket_timeout=30
        )
        print(f"✓ Connected to Redis Cluster")
        return cluster
    except Exception as e:
        print(f"✗ Failed to connect to cluster: {e}")
        return None

def get_cluster_info(cluster):
    """Get cluster information"""
    try:
        cluster_info = cluster.cluster_info()
        cluster_nodes = cluster.cluster_nodes()
        return cluster_info, cluster_nodes
    except Exception as e:
        print(f"⚠ Could not get cluster info: {e}")
        return None, None

def get_key_slot(key):
    """Calculate hash slot for a key"""
    # Redis uses CRC16 for hash slot calculation
    import binascii
    
    # Check for hash tag
    s = key.find('{')
    if s > -1:
        e = key.find('}', s+1)
        if e > -1 and e != s+1:
            key = key[s+1:e]
    
    crc = binascii.crc_hqx(key.encode('utf-8'), 0)
    return crc % 16384

def run_scenario_3():
    """Run sharding test scenario"""
    print("\n" + "="*70)
    print("SKENARIO 3: REDIS CLUSTER SHARDING TEST")
    print("="*70 + "\n")
    
    # Connect to cluster
    cluster = connect_cluster()
    if not cluster:
        print("✗ Cannot proceed: Failed to connect to Redis Cluster")
        return
    
    print(f"\nTest started at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    # Get cluster information
    print("CLUSTER INFORMATION:")
    print("-"*70)
    cluster_info, cluster_nodes = get_cluster_info(cluster)
    
    if cluster_info:
        print("Cluster Info:")
        for key, value in cluster_info.items():
            print(f"  {key}: {value}")
    
    if cluster_nodes:
        print("\nCluster Nodes:")
        print(cluster_nodes)
    
    print("\n" + "="*70)
    print("WRITING KEYS TO CLUSTER")
    print("="*70 + "\n")
    
    # Clear existing test keys (optional)
    print("Clearing any existing test keys...")
    try:
        # Note: FLUSHALL in cluster mode may not work, so we skip this
        pass
    except:
        pass
    
    # Write keys to cluster
    print(f"Writing {NUM_KEYS} keys to cluster...\n")
    
    slot_distribution = defaultdict(int)
    node_distribution = defaultdict(int)
    write_errors = []
    
    start_time = time.time()
    
    for i in range(NUM_KEYS):
        key = f"key{i}"
        value = f"value_{i}_{datetime.now().timestamp()}"
        
        # Calculate slot
        slot = get_key_slot(key)
        
        try:
            cluster.set(key, value)
            slot_distribution[slot] += 1
            
            # Try to determine which node received the key
            # This is approximate based on slot ranges
            if slot < 5461:
                node_distribution['node1'] += 1
            elif slot < 10923:
                node_distribution['node2'] += 1
            else:
                node_distribution['node3'] += 1
                
        except Exception as e:
            write_errors.append({'key': key, 'slot': slot, 'error': str(e)})
        
        if (i + 1) % 1000 == 0:
            print(f"  Written {i + 1}/{NUM_KEYS} keys...")
    
    write_duration = time.time() - start_time
    
    print(f"\n✓ Completed writing keys in {write_duration:.2f} seconds")
    print(f"  Average: {NUM_KEYS/write_duration:.2f} writes/sec")
    print(f"  Errors: {len(write_errors)}\n")
    
    # Analyze distribution
    print("="*70)
    print("HASH SLOT DISTRIBUTION ANALYSIS")
    print("="*70 + "\n")
    
    print(f"Total unique slots used: {len(slot_distribution)}")
    print(f"Total possible slots: 16384")
    print(f"Slot utilization: {len(slot_distribution)/16384*100:.2f}%\n")
    
    # Top 10 slots
    top_slots = sorted(slot_distribution.items(), key=lambda x: x[1], reverse=True)[:10]
    print("Top 10 slots by key count:")
    for slot, count in top_slots:
        print(f"  Slot {slot:5d}: {count:4d} keys")
    
    # Node distribution
    print(f"\nApproximate node distribution:")
    total_keys = sum(node_distribution.values())
    for node, count in sorted(node_distribution.items()):
        print(f"  {node}: {count:6d} keys ({count/total_keys*100:.1f}%)")
    
    # Slot range statistics
    slot_ranges = {
        'Range 0-5460 (Node 1)': sum(1 for s in slot_distribution.keys() if 0 <= s <= 5460),
        'Range 5461-10922 (Node 2)': sum(1 for s in slot_distribution.keys() if 5461 <= s <= 10922),
        'Range 10923-16383 (Node 3)': sum(1 for s in slot_distribution.keys() if 10923 <= s <= 16383)
    }
    
    print(f"\nSlot range coverage:")
    for range_name, slot_count in slot_ranges.items():
        print(f"  {range_name}: {slot_count} unique slots")
    
    # Test reading
    print("\n" + "="*70)
    print("READING KEYS FROM CLUSTER")
    print("="*70 + "\n")
    
    print("Testing read consistency...")
    read_errors = 0
    read_start = time.time()
    
    sample_size = min(1000, NUM_KEYS)
    for i in range(sample_size):
        key = f"key{i}"
        try:
            value = cluster.get(key)
            if value is None:
                read_errors += 1
        except Exception as e:
            read_errors += 1
    
    read_duration = time.time() - read_start
    
    print(f"✓ Read {sample_size} keys in {read_duration:.2f} seconds")
    print(f"  Average: {sample_size/read_duration:.2f} reads/sec")
    print(f"  Missing/Error: {read_errors} ({read_errors/sample_size*100:.1f}%)\n")
    
    # Key pattern analysis
    print("="*70)
    print("KEY PATTERN ANALYSIS")
    print("="*70 + "\n")
    
    # Test different key patterns
    patterns = {
        'numeric': ['key0', 'key100', 'key1000', 'key5000'],
        'hash_tag': ['{user1}:email', '{user1}:name', '{user2}:email', '{user2}:name']
    }
    
    print("Hash slot for different key patterns:")
    for pattern_type, keys in patterns.items():
        print(f"\n{pattern_type.upper()}:")
        for key in keys:
            slot = get_key_slot(key)
            node = 'node1' if slot < 5461 else ('node2' if slot < 10923 else 'node3')
            print(f"  {key:20s} → Slot {slot:5d} → {node}")
    
    # Save results
    result_data = {
        'scenario': 'Redis Cluster Sharding',
        'timestamp': datetime.now().isoformat(),
        'config': {
            'num_keys': NUM_KEYS,
            'cluster_nodes': CLUSTER_NODES
        },
        'write_stats': {
            'duration': write_duration,
            'keys_per_sec': NUM_KEYS/write_duration,
            'errors': len(write_errors)
        },
        'read_stats': {
            'sample_size': sample_size,
            'duration': read_duration,
            'keys_per_sec': sample_size/read_duration,
            'errors': read_errors
        },
        'distribution': {
            'unique_slots': len(slot_distribution),
            'total_slots': 16384,
            'slot_utilization': len(slot_distribution)/16384,
            'node_distribution': dict(node_distribution),
            'slot_range_coverage': slot_ranges,
            'top_10_slots': [{'slot': s, 'count': c} for s, c in top_slots]
        },
        'cluster_info': cluster_info if cluster_info else 'Not available',
        'write_errors_sample': write_errors[:10] if write_errors else []
    }
    
    output_file = f"scenario3_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(output_file, 'w') as f:
        json.dump(result_data, f, indent=2)
    
    print(f"\n✓ Results saved to {output_file}")
    print("="*70 + "\n")
    
    # Cleanup
    cluster.close()

if __name__ == "__main__":
    try:
        run_scenario_3()
    except KeyboardInterrupt:
        print("\n\n✗ Test interrupted by user")
    except Exception as e:
        print(f"\n✗ Error during test: {e}")
        import traceback
        traceback.print_exc()
