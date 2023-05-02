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
        client=client, identifier="test", duration=60, n=5
    )

    await limiter.add_timestamp()
    
    # Confirm that the ttl was set.
    assert await client.ttl(limiter.key) == limiter.duration

    # confirm that a single timestamp was added.
    assert await client.zcount(limiter.key, min=0, max=time.time()) == 1


@pytest.mark.asyncio
async def test_slide_window_one(client):
    limiter = RateLimiter(
        client=client, identifier="test", duration=60, n=5
    )

    # Test running slide_window() without adding any timestamps.
    ts = time.time()
    count, least_recent_timestamp = await limiter.slide_window(ts)
    assert count == 0
    assert least_recent_timestamp is None


@pytest.mark.asyncio
async def test_slide_window_two(client):
    limiter = RateLimiter(
        client=client, identifier="test", duration=60, n=5
    )

    # test to make sure that a timestamp will show up in the window.
    ts = time.time()
    await limiter.add_timestamp(ts)

    count, least_recent_timestamp = await limiter.slide_window(ts)
    assert count == 1
    assert least_recent_timestamp == ts


@pytest.mark.asyncio
async def test_slide_window_three(client):
    limiter = RateLimiter(
        client=client, identifier="test", duration=60, n=5
    )

    # Test to make sure that a timestamp will slide out of the window
    start = time.time()
    await limiter.add_timestamp(start)

    # 59 seconds later (one second before the window)
    ts = start + 59

    count, least_recent_timestamp = await limiter.slide_window(ts)
    assert count == 1
    assert least_recent_timestamp == start

    # 60 seconds later, the timestamp is no longer in the window.
    ts = start + 60
    count, least_recent_timestamp = await limiter.slide_window(ts)
    assert count == 0
    assert least_recent_timestamp == None


@pytest.mark.asyncio
async def test_slide_window_four(client):
    limiter = RateLimiter(
        client=client, identifier="test", duration=60, n=5
    )

    # make sure that the least recent timestamp is accurate over time.

    start = time.time()
    await limiter.add_timestamp(start)

    fifteen = start + 15
    await limiter.add_timestamp(fifteen)

    thirty = start + 30
    await limiter.add_timestamp(thirty)

    fourty_five = start + 45
    await limiter.add_timestamp(fourty_five)

    # check results of slide_window at the 45th second.
    count, least_recent_timestamp = await limiter.slide_window(fourty_five)
    assert count == 4
    assert least_recent_timestamp == start

    # Confirm that at the 60th second, the first ts is no longer in the window,
    # thus making the count 3, and the least recent timestamp to be fifteen.
    sixty = start + 60
    count, least_recent_timestamp = await limiter.slide_window(sixty)
    assert count == 3
    assert least_recent_timestamp == fifteen

    seventy_five = start + 75
    count, least_recent_timestamp = await limiter.slide_window(seventy_five)
    assert count == 2
    assert least_recent_timestamp == thirty

    ninety = start + 90
    count, least_recent_timestamp = await limiter.slide_window(ninety)
    assert count == 1
    assert least_recent_timestamp == fourty_five


@pytest.mark.asyncio
async def test_ratelimit(client):
    limiter = RateLimiter(
        client=client, identifier="test", duration=60, n=3
    )

    start = time.time()

    result = await limiter(start)
    assert result == True
    assert limiter.retry_after == 0

    # 15 seconds later
    fifteen = start + 15
    result = await limiter(fifteen)
    assert result == True
    assert limiter.retry_after == 0

    # 30 seconds later
    thirty = start + 30
    result = await limiter(thirty)
    assert result == True
    assert limiter.retry_after == 0

    # 45 seconds later
    forty_five = start + 45
    result = await limiter(forty_five)
    assert result == False
    assert limiter.retry_after == 15

    # 59 seconds later
    fifty_nine = start + 59
    result = await limiter(fifty_nine)
    assert result == False
    assert limiter.retry_after == 1

    # 60 seconds later
    sixty = start + 60
    result = await limiter(sixty)
    assert result == True
    assert limiter.retry_after == 0

    # 61 seconds later
    sixty_one = start + 61
    result = await limiter(sixty_one)
    assert result == False
    assert limiter.retry_after == 14
