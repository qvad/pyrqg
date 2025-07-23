"""
Query uniqueness tracking system for billion-scale generation.

Uses probabilistic data structures (Bloom filters) to efficiently track
generated queries and prevent duplicates while maintaining low memory usage.
"""

import hashlib
import mmh3  # MurmurHash3 for fast hashing
import math
from typing import Optional, List, Tuple, Dict, Any
from dataclasses import dataclass, field
from enum import Enum
import struct


class UniquenessMode(Enum):
    """Uniqueness checking modes"""
    STRICT = "strict"  # Guarantee no duplicates (slower, more memory)
    PROBABILISTIC = "probabilistic"  # Allow small false positive rate
    NONE = "none"  # No checking (fastest)


@dataclass
class UniquenessConfig:
    """Configuration for uniqueness tracking"""
    mode: UniquenessMode = UniquenessMode.PROBABILISTIC
    false_positive_rate: float = 0.0001  # 0.01% false positive rate
    expected_elements: int = 1_000_000_000  # Expected number of queries
    bloom_filter_size_mb: int = 1024  # Max size per bloom filter in MB
    rotation_interval: int = 100_000_000  # Rotate filters after N elements
    hash_functions: int = 7  # Number of hash functions for bloom filter
    
    # For strict mode
    use_disk_backing: bool = True  # Use disk for overflow in strict mode
    disk_cache_dir: str = "/tmp/pyrqg_cache"
    
    # Query normalization
    normalize_whitespace: bool = True
    normalize_case: bool = True
    normalize_literals: bool = False  # Replace literals with placeholders


class BloomFilter:
    """
    Space-efficient probabilistic data structure for membership testing.
    
    Optimized for billion-scale with minimal memory usage.
    """
    
    def __init__(self, expected_elements: int, false_positive_rate: float, 
                 max_size_bytes: Optional[int] = None):
        # Calculate optimal size and hash functions
        self.expected_elements = expected_elements
        self.false_positive_rate = false_positive_rate
        
        # Optimal bit array size: m = -n * ln(p) / (ln(2)^2)
        ln2_squared = 0.4804530139182014  # ln(2)^2
        optimal_bits = int(-expected_elements * math.log(false_positive_rate) / ln2_squared)
        
        # Limit size if specified
        if max_size_bytes:
            max_bits = max_size_bytes * 8
            optimal_bits = min(optimal_bits, max_bits)
            
        # Round to nearest byte
        self.size_bits = (optimal_bits + 7) & ~7
        self.size_bytes = self.size_bits // 8
        
        # Optimal number of hash functions: k = m/n * ln(2)
        self.num_hashes = max(1, int(self.size_bits / expected_elements * 0.693147))
        
        # Initialize bit array
        self.bit_array = bytearray(self.size_bytes)
        self.element_count = 0
        
        # Calculate actual false positive rate with limited size
        if max_size_bytes and optimal_bits > self.size_bits:
            # Recalculate FPR with actual size
            self.actual_fpr = (1 - math.exp(-self.num_hashes * expected_elements / self.size_bits)) ** self.num_hashes
        else:
            self.actual_fpr = false_positive_rate
    
    def _get_hash_values(self, item: bytes) -> List[int]:
        """Generate hash values for item using double hashing"""
        # Use MurmurHash3 for speed
        hash1 = mmh3.hash(item, seed=0, signed=False)
        hash2 = mmh3.hash(item, seed=hash1, signed=False)
        
        # Generate k hash values using double hashing
        # h_i = h1 + i * h2
        hashes = []
        for i in range(self.num_hashes):
            hash_val = (hash1 + i * hash2) % self.size_bits
            hashes.append(hash_val)
            
        return hashes
    
    def add(self, item: bytes) -> bool:
        """Add item to filter, returns True if item was probably not in set"""
        was_new = False
        
        for bit_pos in self._get_hash_values(item):
            byte_pos = bit_pos // 8
            bit_offset = bit_pos % 8
            
            # Check if bit is already set
            if not (self.bit_array[byte_pos] & (1 << bit_offset)):
                was_new = True
                
            # Set the bit
            self.bit_array[byte_pos] |= (1 << bit_offset)
            
        if was_new:
            self.element_count += 1
            
        return was_new
    
    def contains(self, item: bytes) -> bool:
        """Check if item might be in set (can have false positives)"""
        for bit_pos in self._get_hash_values(item):
            byte_pos = bit_pos // 8
            bit_offset = bit_pos % 8
            
            # If any bit is not set, item is definitely not in set
            if not (self.bit_array[byte_pos] & (1 << bit_offset)):
                return False
                
        # All bits set - item might be in set
        return True
    
    def get_load_factor(self) -> float:
        """Get the current load factor (fraction of bits set)"""
        # Sample bits instead of checking all for performance
        sample_size = min(10000, self.size_bytes)
        set_bits = 0
        
        for i in range(0, self.size_bytes, self.size_bytes // sample_size):
            byte_val = self.bit_array[i]
            set_bits += bin(byte_val).count('1')
            
        return (set_bits * self.size_bytes) / (sample_size * 8 * self.size_bits)
    
    def estimate_fpr(self) -> float:
        """Estimate current false positive rate based on load"""
        load = self.get_load_factor()
        return load ** self.num_hashes
    
    def merge(self, other: 'BloomFilter'):
        """Merge another bloom filter into this one"""
        if self.size_bytes != other.size_bytes:
            raise ValueError("Cannot merge bloom filters of different sizes")
            
        # OR the bit arrays
        for i in range(self.size_bytes):
            self.bit_array[i] |= other.bit_array[i]
            
        self.element_count += other.element_count


class RotatingBloomFilter:
    """
    Manages multiple bloom filters with rotation for unlimited capacity.
    
    Old filters are archived or discarded based on configuration.
    """
    
    def __init__(self, config: UniquenessConfig):
        self.config = config
        self.current_filter = None
        self.archived_filters: List[BloomFilter] = []
        self.total_elements = 0
        
        # Calculate size per filter
        total_size_bytes = config.bloom_filter_size_mb * 1024 * 1024
        filters_needed = math.ceil(config.expected_elements / config.rotation_interval)
        size_per_filter = total_size_bytes // max(1, filters_needed)
        
        self.size_per_filter = size_per_filter
        self._create_new_filter()
    
    def _create_new_filter(self):
        """Create a new bloom filter"""
        self.current_filter = BloomFilter(
            expected_elements=self.config.rotation_interval,
            false_positive_rate=self.config.false_positive_rate,
            max_size_bytes=self.size_per_filter
        )
    
    def add(self, item: bytes) -> bool:
        """Add item, returns True if probably new"""
        # Check archived filters first
        for old_filter in self.archived_filters:
            if old_filter.contains(item):
                return False  # Probably duplicate
                
        # Add to current filter
        was_new = self.current_filter.add(item)
        self.total_elements += 1
        
        # Check if rotation needed
        if self.current_filter.element_count >= self.config.rotation_interval:
            self._rotate()
            
        return was_new
    
    def contains(self, item: bytes) -> bool:
        """Check if item might be in any filter"""
        # Check current filter
        if self.current_filter.contains(item):
            return True
            
        # Check archived filters
        for old_filter in self.archived_filters:
            if old_filter.contains(item):
                return True
                
        return False
    
    def _rotate(self):
        """Rotate to a new filter"""
        # Archive current filter
        self.archived_filters.append(self.current_filter)
        
        # Limit archived filters to prevent unbounded growth
        max_archived = 10  # Keep last 10 filters
        if len(self.archived_filters) > max_archived:
            self.archived_filters.pop(0)
            
        # Create new filter
        self._create_new_filter()


class QueryHasher:
    """
    Generates consistent hashes for queries with optional normalization.
    """
    
    def __init__(self, config: UniquenessConfig):
        self.config = config
        
    def hash_query(self, query: str) -> bytes:
        """Generate hash for query with normalization"""
        normalized = query
        
        # Normalize whitespace
        if self.config.normalize_whitespace:
            # Replace multiple spaces with single space
            import re
            normalized = re.sub(r'\s+', ' ', normalized).strip()
            
        # Normalize case
        if self.config.normalize_case:
            normalized = normalized.upper()
            
        # Normalize literals (more complex, simplified version)
        if self.config.normalize_literals:
            # Compile regexes once for performance
            if not hasattr(self, '_num_regex'):
                self._num_regex = re.compile(r'\b\d+\b')
                self._str_regex = re.compile(r"'[^']*'")
            
            # Replace numbers with placeholder
            normalized = self._num_regex.sub('#NUM#', normalized)
            # Replace quoted strings with placeholder
            normalized = self._str_regex.sub("'#STR#'", normalized)
            
        # Generate hash
        return hashlib.sha256(normalized.encode('utf-8')).digest()
    
    def get_query_fingerprint(self, query: str) -> str:
        """Get human-readable fingerprint for query"""
        hash_bytes = self.hash_query(query)
        return hashlib.md5(hash_bytes).hexdigest()[:16]


class UniquenessTracker:
    """
    Main interface for tracking query uniqueness at billion scale.
    
    Supports multiple modes:
    - Strict: Guarantees no duplicates (uses more memory/disk)
    - Probabilistic: Very low false positive rate with minimal memory
    - None: No tracking (for maximum performance)
    """
    
    def __init__(self, config: UniquenessConfig):
        self.config = config
        self.hasher = QueryHasher(config)
        
        # Statistics
        self.total_queries = 0
        self.unique_queries = 0
        self.duplicate_queries = 0
        
        # Initialize based on mode
        if config.mode == UniquenessMode.NONE:
            self.tracker = None
        elif config.mode == UniquenessMode.PROBABILISTIC:
            self.tracker = RotatingBloomFilter(config)
        else:  # STRICT mode
            # For strict mode, we'd use a different approach
            # like a disk-backed set or database
            # For now, use bloom filter with very low FPR
            strict_config = UniquenessConfig(
                mode=UniquenessMode.PROBABILISTIC,
                false_positive_rate=0.000001,  # 0.0001% FPR
                expected_elements=config.expected_elements,
                bloom_filter_size_mb=config.bloom_filter_size_mb * 4  # 4x size
            )
            self.tracker = RotatingBloomFilter(strict_config)
    
    def check_and_add(self, query: str) -> bool:
        """
        Check if query is unique and add to tracker.
        
        Returns:
            True if query is unique (not seen before)
            False if query is duplicate (or probably duplicate)
        """
        self.total_queries += 1
        
        # No tracking mode - always return unique
        if self.config.mode == UniquenessMode.NONE:
            self.unique_queries += 1
            return True
            
        # Generate hash
        query_hash = self.hasher.hash_query(query)
        
        # Check and add
        is_unique = self.tracker.add(query_hash)
        
        if is_unique:
            self.unique_queries += 1
        else:
            self.duplicate_queries += 1
            
        return is_unique
    
    def check_only(self, query: str) -> bool:
        """Check if query exists without adding it"""
        if self.config.mode == UniquenessMode.NONE:
            return False
            
        query_hash = self.hasher.hash_query(query)
        return self.tracker.contains(query_hash)
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get uniqueness statistics"""
        stats = {
            "mode": self.config.mode.value,
            "total_queries": self.total_queries,
            "unique_queries": self.unique_queries,
            "duplicate_queries": self.duplicate_queries,
            "uniqueness_rate": self.unique_queries / max(1, self.total_queries),
        }
        
        if self.config.mode != UniquenessMode.NONE:
            if isinstance(self.tracker, RotatingBloomFilter):
                stats["bloom_filters"] = {
                    "active_elements": self.tracker.current_filter.element_count,
                    "archived_filters": len(self.tracker.archived_filters),
                    "total_elements": self.tracker.total_elements,
                    "estimated_fpr": self.tracker.current_filter.estimate_fpr(),
                    "size_mb": self.tracker.size_per_filter / (1024 * 1024)
                }
                
        return stats
    
    def estimate_memory_usage(self) -> int:
        """Estimate current memory usage in bytes"""
        if self.config.mode == UniquenessMode.NONE:
            return 0
            
        if isinstance(self.tracker, RotatingBloomFilter):
            # Current filter + archived filters
            num_filters = 1 + len(self.tracker.archived_filters)
            return num_filters * self.tracker.size_per_filter
            
        return 0
    
    def reset(self):
        """Reset the tracker"""
        self.total_queries = 0
        self.unique_queries = 0
        self.duplicate_queries = 0
        
        if self.config.mode != UniquenessMode.NONE:
            if isinstance(self.tracker, RotatingBloomFilter):
                self.tracker.archived_filters.clear()
                self.tracker._create_new_filter()
                self.tracker.total_elements = 0