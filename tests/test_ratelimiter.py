import time

import pytest
import pytest_asyncio
from redis.asyncio import Redis

from ratelimitx import RateLimiter


@pytest_asyncio.fixture
async def client():
    client = Redis()
    await client.flushdb()
    return client


def test_ratelimiter_key(client):
    # test default
    limiter = RateLimiter(
        client=client,
        identifier="test",
        duration=60,
        n=5
    )

    assert limiter.key == "ratelimit|test|60"

    # Test custom prefix
    limiter = RateLimiter(
        client=client,
        identifier="test",
        duration=60,
        n=5,
        prefix="prefix"
    )

    assert limiter.key == "prefix|test|60"

    # Test custom deilimiter.
    limiter = RateLimiter(
        client=client,
        identifier="test",
        duration=60,
        n=5,
        delimiter=":"
    )

    assert limiter.key == "ratelimit:test:60"


@pytest.mark.asyncio
async def test_add_timestamp(client):
    limiter = RateLimiter(
        client=client,
        identifier="test",
        duration=60,
        n=5
    )

    await limiter.add_timestamp()
    
    # Confirm that the ttl was set.
    assert await client.ttl(limiter.key) == limiter.duration

    # confirm that a single timestamp was added.
    assert await client.zcount(limiter.key, min=0, max=time.time()) == 1


@pytest.mark.asyncio
async def test_ratelimit(client):
    limiter = RateLimiter(
        client=client,
        identifier="test",
        duration=15,
        n=3
    )

    t = time.time()

    # simulate 15 requests.
    timestamps = [t + i for i in range(30)]

    for idx, timestamp in enumerate(timestamps, 1):
        # each loops represents a "request" at a rate
        # of one request per second.

        result = await limiter(timestamp)
        if idx in (1, 2, 3, 16, 17, 18):
            assert result == True
            assert limiter.retry_after == 0
        
        else:
            assert result == False
            # assert limiter.retry_after == ...
