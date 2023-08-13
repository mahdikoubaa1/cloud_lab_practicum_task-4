# !/usr/bin/env python3

import asyncio
import test_concurrent_utils as utils

if __name__ == "__main__":
    key_count = 1000
    txs_count = 100000
    thread_count = 2
    message = "Testing concurrent transactions with increment"
    asyncio.run(utils.run_increment_subtest(message, key_count, txs_count,
                                            thread_count))
