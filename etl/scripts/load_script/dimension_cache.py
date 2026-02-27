import logging
from typing import Any, Dict

logger = logging.getLogger(__name__)

class DimensionCache:
    def __init__(self, max_size: int = 10_000):
        self.cache: Dict[str, Any] = {}
        self.max_size = max_size
        self.hits = 0
        self.misses = 0

    def get(self, key):
        val = self.cache.get(key)
        if val is not None:
            self.hits += 1
        else:
            self.misses += 1
        return val

    def set(self, key, value):
        if len(self.cache) >= self.max_size:
            oldest_key = next(iter(self.cache))
            self.cache.pop(oldest_key, None)
        self.cache[key] = value

    def stats(self):
        total = self.hits + self.misses
        hit_rate = (self.hits / total * 100) if total else 0
        logger.info(
            f"Cache stats - hits:{self.hits} misses:{self.misses} hit_rate:{hit_rate:.1f}% size:{len(self.cache)}"
        )

    def clear(self):
        self.stats()
        self.cache.clear()
        self.hits = 0
        self.misses = 0

dim_cache = DimensionCache()