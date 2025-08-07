import pytest

def test_rediswriter_import():
    try:
        from rediswriter.rediswriter import RedisWriter
    except ImportError as e:
        pytest.fail(f"Failed to import RedisWriter: {e}")

    assert RedisWriter is not None, "RedisWriter should be imported successfully"