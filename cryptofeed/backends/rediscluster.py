'''
Copyright (C) 2017-2022 Bryant Moscon - bmoscon@gmail.com

Please see the LICENSE file for the terms and conditions
associated with this software.
'''
from collections import defaultdict

import aioredis_cluster
from yapic import json

from cryptofeed.backends.backend import BackendBookCallback, BackendCallback, BackendQueue


class RedisCallbackCluster(BackendQueue):
    def __init__(self, host='127.0.0.1', port=6379, socket=None, key=None, none_to='None', numeric_type=float, **kwargs):
        """
        setting key lets you override the prefix on the
        key used in redis. The defaults are related to the data
        being stored, i.e. trade, funding, etc
        """
        prefix = 'redis://'
        if socket:
            prefix = 'unix://'
            port = None

        self.redis = f"{prefix}{host}" + f":{port}" if port else ""
        self.key = key if key else self.default_key
        self.numeric_type = numeric_type
        self.none_to = none_to
        self.running = True


class RedisZSetCallbackCluster(RedisCallbackCluster):
    def __init__(self, host='127.0.0.1', port=6379, socket=None, key=None, numeric_type=float, score_key='timestamp', **kwargs):
        """
        score_key: str
            the value at this key will be used to store the data in the ZSet in redis. The
            default is timestamp. If you wish to look up the data by a different value,
            use this to change it. It must be a numeric value.
        """
        self.score_key = score_key
        super().__init__(host=host, port=port, socket=socket, key=key, numeric_type=numeric_type, **kwargs)

    async def writer(self):

        
        # conn = aioredis_cluster.from_url(self.redis)
        conn = await aioredis_cluster.create_redis_cluster([
            "redis://cryptofeed-store-redis-0001-001.nirbu0.0001.euw2.cache.amazonaws.com",
            "redis://cryptofeed-store-redis-0002-001.nirbu0.0001.euw2.cache.amazonaws.com",
            "redis://cryptofeed-store-redis-0003-001.nirbu0.0001.euw2.cache.amazonaws.com",
            "redis://cryptofeed-store-redis-0004-001.nirbu0.0001.euw2.cache.amazonaws.com"
        ])
        while self.running:
            async with self.read_queue() as updates:
                # async with conn.pipeline(transaction=False) as pipe:
                    for update in updates:
                        await conn.zadd(f"Z{self.key}-{update['exchange']}-{update['symbol']}", update[self.score_key], f"{json.dumps(update)}")
                    # await pipe.execute()

        await conn.close()
        await conn.connection_pool.disconnect()

class RedisStringCallbackCluster(RedisCallbackCluster):
    def __init__(self, host='127.0.0.1', port=6379, socket=None, key=None, numeric_type=float, score_key='timestamp', **kwargs):
        """
        score_key: str
            the value at this key will be used to store the data in the ZSet in redis. The
            default is timestamp. If you wish to look up the data by a different value,
            use this to change it. It must be a numeric value.
        """
        self.score_key = score_key
        super().__init__(host=host, port=port, socket=socket, key=key, numeric_type=numeric_type, **kwargs)

    async def writer(self):

        
        # conn = aioredis_cluster.from_url(self.redis)
        conn = await aioredis_cluster.create_redis_cluster([
            "redis://cryptofeed-store-redis-0001-001.nirbu0.0001.euw2.cache.amazonaws.com",
            "redis://cryptofeed-store-redis-0002-001.nirbu0.0001.euw2.cache.amazonaws.com",
            "redis://cryptofeed-store-redis-0003-001.nirbu0.0001.euw2.cache.amazonaws.com",
            "redis://cryptofeed-store-redis-0004-001.nirbu0.0001.euw2.cache.amazonaws.com"
        ])
        while self.running:
            async with self.read_queue() as updates:
                # async with conn.pipeline(transaction=False) as pipe:
                    for update in updates:
                        await conn.set(f"S{self.key}-{update['exchange']}-{update['symbol']}", f"{json.dumps(update)}")
                    # await pipe.execute()

        await conn.close()
        await conn.connection_pool.disconnect()


class RedisStreamCallbackCluster(RedisCallbackCluster):
    async def writer(self):
        conn = aioredis.from_url(self.redis)

        while self.running:
            async with self.read_queue() as updates:
                async with conn.pipeline(transaction=False) as pipe:
                    for update in updates:
                        if 'delta' in update:
                            update['delta'] = json.dumps(update['delta'])
                        elif 'book' in update:
                            update['book'] = json.dumps(update['book'])
                        elif 'closed' in update:
                            update['closed'] = str(update['closed'])

                        pipe = pipe.xadd(f"X{self.key}-{update['exchange']}-{update['symbol']}", update)
                    await pipe.execute()

        await conn.close()
        await conn.connection_pool.disconnect()

# trades

class TradeRedisCluster(RedisZSetCallbackCluster, BackendCallback):
    default_key = 'trades'

class TradeStreamCluster(RedisStreamCallbackCluster, BackendCallback):
    default_key = 'trades'

class TradeStringCluster(RedisStringCallbackCluster, BackendCallback):
    default_key = 'trades'


# funding

class FundingRedisCluster(RedisZSetCallbackCluster, BackendCallback):
    default_key = 'funding'

class FundingStreamCluster(RedisStreamCallbackCluster, BackendCallback):
    default_key = 'funding'


# book

class BookRedisCluster(RedisZSetCallbackCluster, BackendBookCallback):
    default_key = 'book'

    def __init__(self, *args, snapshots_only=False, snapshot_interval=1000, score_key='receipt_timestamp', **kwargs):
        self.snapshots_only = snapshots_only
        self.snapshot_interval = snapshot_interval
        self.snapshot_count = defaultdict(int)
        super().__init__(*args, score_key=score_key, **kwargs)

class BookStreamCluster(RedisStreamCallbackCluster, BackendBookCallback):
    default_key = 'book'

    def __init__(self, *args, snapshots_only=False, snapshot_interval=1000, **kwargs):
        self.snapshots_only = snapshots_only
        self.snapshot_interval = snapshot_interval
        self.snapshot_count = defaultdict(int)
        super().__init__(*args, **kwargs)

# ticker

class TickerRedisCluster(RedisZSetCallbackCluster, BackendCallback):
    default_key = 'ticker'

class TickerStreamCluster(RedisStreamCallbackCluster, BackendCallback):
    default_key = 'ticker'

class TickerStringCluster(RedisStringCallbackCluster, BackendCallback):
    default_key = 'ticker'


# open_interest

class OpenInterestRedisC(RedisZSetCallbackCluster, BackendCallback):
    default_key = 'open_interest'

class OpenInterestStreamCluster(RedisStreamCallbackCluster, BackendCallback):
    default_key = 'open_interest'


# liquidations

class LiquidationsRedisCluster(RedisZSetCallbackCluster, BackendCallback):
    default_key = 'liquidations'

class LiquidationsStreamCluster(RedisStreamCallbackCluster, BackendCallback):
    default_key = 'liquidations'


# candles

class CandlesRedisCluster(RedisZSetCallbackCluster, BackendCallback):
    default_key = 'candles'

class CandlesStreamCluster(RedisStreamCallbackCluster, BackendCallback):
    default_key = 'candles'


# order_info

class OrderInfoRedisCluster(RedisZSetCallbackCluster, BackendCallback):
    default_key = 'order_info'

class OrderInfoStreamCluster(RedisStreamCallbackCluster, BackendCallback):
    default_key = 'order_info'


# transactions

class TransactionsRedisCluster(RedisZSetCallbackCluster, BackendCallback):
    default_key = 'transactions'

class TransactionsStreamCluster(RedisStreamCallbackCluster, BackendCallback):
    default_key = 'transactions'


# balances

class BalancesRedisCluster(RedisZSetCallbackCluster, BackendCallback):
    default_key = 'balances'

class BalancesStreamCluster(RedisStreamCallbackCluster, BackendCallback):
    default_key = 'balances'


# fills

class FillsRedisCluster(RedisZSetCallbackCluster, BackendCallback):
    default_key = 'fills'

class FillsStreamCluster(RedisStreamCallbackCluster, BackendCallback):
    default_key = 'fills'
