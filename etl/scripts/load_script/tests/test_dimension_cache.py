from load_script.dimension_cache import DimensionCache

def test_dimension_cache_basic():
    cache = DimensionCache(max_size=2)
    assert cache.get("a") is None
    cache.set("a", 1)
    assert cache.get("a") == 1
    cache.set("b", 2)
    assert cache.get("b") == 2
    cache.set("c", 3)
    # "a" should be evicted (FIFO)
    assert cache.get("a") is None
    assert cache.get("c") == 3
    cache.clear()
    assert cache.get("b") is None