from datetime import datetime
import time

import pytest
import pytest_asyncio
from redis.asyncio import Redis

from ratelimitx import RateLimiter, RateLimitError


@pytest_asyncio.fixture
async def client():
    client = Redis()
    await client.flushdb()
    return client


def test_build_key(client):
    # test default
    limiter = RateLimiter(
        client=client,
        window_length=60,
        n=5
    )

    assert limiter._build_key("test") == "test|60"

    # Test custom prefix
    limiter = RateLimiter(
        client=client,
        window_length=60,
        n=5,
        prefix="prefix"
    )

    assert limiter._build_key("test") == "prefix|test|60"

    # Test custom deilimiter.
    limiter = RateLimiter(
        client=client,
        window_length=60,
        n=5,
        delimiter=":"
    )

    assert limiter._build_key("test") == "test:60"


@pytest.mark.asyncio
async def test_add_timestamp(client):
    limiter = RateLimiter(
        client=client, window_length=60, n=5
    )

    await limiter.add_timestamp("test")

    key = limiter._build_key("test")
    
    # Confirm that the ttl was set.
    assert await client.ttl(key) == limiter.window_length

    # confirm that a single timestamp was added.
    assert await client.zcount(key, min=0, max=time.time()) == 1


@pytest.mark.asyncio
async def test_slide_window_one(client):
    limiter = RateLimiter(
        client=client, window_length=60, n=5
    )

    # Test running slide_window() without adding any timestamps.
    ts = time.time()
    count, least_recent_timestamp = await limiter.slide_window("test", ts)
    assert count == 0
    assert least_recent_timestamp is None


@pytest.mark.asyncio
async def test_slide_window_two(client):
    limiter = RateLimiter(
        client=client, window_length=60, n=5
    )

    # test to make sure that a timestamp will show up in the window.
    ts = time.time()
    await limiter.add_timestamp("test", ts)

    count, least_recent_timestamp = await limiter.slide_window("test", ts)
    assert count == 1
    assert least_recent_timestamp == ts


@pytest.mark.asyncio
async def test_slide_window_three(client):
    limiter = RateLimiter(
        client=client, window_length=60, n=5
    )

    # Test to make sure that a timestamp will slide out of the window
    start = time.time()
    await limiter.add_timestamp("test", start)

    # 59 seconds later (one second before the window)
    ts = start + 59

    count, least_recent_timestamp = await limiter.slide_window("test", ts)
    assert count == 1
    assert least_recent_timestamp == start

    # 60 seconds later, the timestamp is no longer in the window.
    ts = start + 60
    count, least_recent_timestamp = await limiter.slide_window("test", ts)
    assert count == 0
    assert least_recent_timestamp is None


@pytest.mark.asyncio
async def test_slide_window_four(client):
    limiter = RateLimiter(
        client=client, window_length=60, n=5
    )

    # make sure that the least recent timestamp is accurate over time.

    start = time.time()
    await limiter.add_timestamp("test", start)

    fifteen = start + 15
    await limiter.add_timestamp("test", fifteen)

    thirty = start + 30
    await limiter.add_timestamp("test", thirty)

    fourty_five = start + 45
    await limiter.add_timestamp("test", fourty_five)

    # check results of slide_window at the 45th second.
    count, least_recent_timestamp = await limiter.slide_window("test", fourty_five)
    assert count == 4
    assert least_recent_timestamp == start

    # Confirm that at the 60th second, the first ts is no longer in the window,
    # thus making the count 3, and the least recent timestamp to be fifteen.
    sixty = start + 60
    count, least_recent_timestamp = await limiter.slide_window("test", sixty)
    assert count == 3
    assert least_recent_timestamp == fifteen

    seventy_five = start + 75
    count, least_recent_timestamp = await limiter.slide_window("test", seventy_five)
    assert count == 2
    assert least_recent_timestamp == thirty

    ninety = start + 90
    count, least_recent_timestamp = await limiter.slide_window("test", ninety)
    assert count == 1
    assert least_recent_timestamp == fourty_five


@pytest.mark.asyncio
async def test_rate_limit(client):
    limiter = RateLimiter(
        client=client, window_length=60, n=3
    )

    start = time.time()
    await limiter.rate_limit("test", start)

    # 15 seconds later
    fifteen = start + 15
    await limiter.rate_limit("test", fifteen)

    # 30 seconds later
    thirty = start + 30
    await limiter.rate_limit("test", thirty)

    # 45 seconds later
    forty_five = start + 45
    with pytest.raises(RateLimitError) as exc_info:
        await limiter.rate_limit("test", forty_five)

    retry_after = exc_info.value.retry_after
    assert retry_after.seconds == 15
    assert retry_after.date == datetime.fromtimestamp(forty_five + 15)

    # 59 seconds later
    fifty_nine = start + 59
    with pytest.raises(RateLimitError) as exc_info:
        await limiter.rate_limit("test", fifty_nine)
    
    retry_after = exc_info.value.retry_after
    assert retry_after.seconds == 1
    assert retry_after.date == datetime.fromtimestamp(fifty_nine + 1)

    # 60 seconds later
    sixty = start + 60
    await limiter.rate_limit("test", sixty)

    # 65 seconds later
    sixty_five = start + 65
    with pytest.raises(RateLimitError) as exc_info:
        await limiter.rate_limit("test", sixty_five)

    retry_after = exc_info.value.retry_after
    assert retry_after.seconds == 10
    assert retry_after.date == datetime.fromtimestamp(sixty_five + 10)
