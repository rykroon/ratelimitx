import asyncio
from dataclasses import dataclass
import time

from redis.asyncio import Redis


@dataclass
class RateLimiter:

    client: Redis
    identifier: str
    duration: int
    n: int
    prefix: str = "ratelimit"
    delimiter: str = "|"

    @property
    def key(self) -> str:
        """
            Returns the key that will be used to store the data into Redis.
        """
        return self.delimiter.join([self.prefix, self.identifier, str(self.duration)])

    @property
    def retry_after(self) -> int:
        """
            Return the amount of time in seconds before a retry is allowed.
        """
        return getattr(self, '_retry_after', 0)

    async def __call__(self, timestamp: float | None = None) -> bool:
        """
            Perform all parts of the rate limiting algorithm.
        """
        if timestamp is None:
            timestamp = time.time()

        success = await self.slide_window(timestamp)
        if success:
            await self.add_timestamp(timestamp)

        return success

    async def slide_window(self, timestamp: float | None = None) -> bool:
        """
            Remove expired timestamps (slides the window).
            Calculate retry_after.
            Return True if the rate-limited feature can proceed.
        """
        if timestamp is None:
            timestamp = time.time()

        pipeline = self.client.pipeline()

        # Remove expired timestamps.
        pipeline.zremrangebyscore(self.key, min=0, max=timestamp - self.duration)

        # Count the amount of timestamps remaining.
        pipeline.zcount(self.key, min=0, max=timestamp)

        # Get the lowest ranked score.
        pipeline.zrange(self.key, start=0, end=0)

        _, count, scores = await pipeline.execute()
        if count < self.n:
            self._retry_after = 0
            return True

        lowest_score = float(scores[0])
        self._retry_after = self.duration - (timestamp - lowest_score)
        return False

    async def add_timestamp(self, timestamp: float | None = None):
        """
            Adds a timestamp and updates the expiration.
        """
        if timestamp is None:
            timestamp = time.time()

        pipeline = self.client.pipeline()
        pipeline.zadd(self.key, {timestamp: timestamp})
        pipeline.expire(self.key, self.duration)
        await pipeline.execute()


@dataclass
class MultiRateLimiter:
    rate_limiters: list[RateLimiter]

    @classmethod
    def from_mapping(cls, client: Redis, identifier: str, mapping: dict[int, int]):
        """
            Creates a new multi-ratelimiter by creating new rate limiter objects
            from a mapping containing durations for keys and units for values.
        """
        rate_limiters = [
            RateLimiter(client, identifier, duration, n) for duration, n in mapping.items()
        ]
        return cls(rate_limiters)

    @classmethod
    def new(
        cls,
        client: Redis,
        identifier: str,
        per_second: int | None = None,
        per_minute: int | None = None,
        per_hour: int | None = None,
        per_day: int | None = None
    ):
        """
            Creates a new multi-ratelimiter by specifying the number
            of units for each unit of time.
        """
        assert any([per_second, per_minute, per_hour, per_day])
        mapping = {}
        if per_second is not None:
            mapping[1] = per_second
        
        if per_minute is not None:
            mapping[60] = per_minute

        if per_hour is not None:
            mapping[60*60] = per_hour

        if per_day is not None:
            mapping[24*60*60] = per_day

        return cls.from_mapping(client, identifier, mapping)

    @property
    def retry_after(self) -> int:
        return getattr(self, '_retry_after', None)

    async def __call__(self, timestamp: float | None = None):
        if timestamp is None:
            timestamp = time.time()

        success = await self.slide_windows(timestamp)
        if success:
            await self.add_timstamps(timestamp)
        return success

    async def slide_windows(self, timestamp: float | None = None):
        if timestamp is None:
            timestamp = time.time()
        
        coroutines = [rl.slide_window(timestamp) for rl in self.rate_limiters]
        results = await asyncio.gather(*coroutines)

        if all(results):
            self._retry_after = 0
            return True
        
        self._retry_after = max([rl.retry_after for rl in self.rate_limiters])
        return False

    async def add_timstamps(self, timestamp: float | None = None):
        if timestamp is None:
            timestamp = time.time()

        coroutines = [rl.add_timestamp(timestamp) for rl in self.rate_limiters]
        await asyncio.gather(*coroutines)
