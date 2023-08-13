# !/usr/bin/env python3

import asyncio
import test_concurrent_utils as utils

if __name__ == "__main__":
    key_count = 10000
    txs_count = 500000
    thread_count = 4
    message = "Testing concurrent transactions with many increments"
    asyncio.run(utils.run_increment_subtest(message, key_count, txs_count,
                                            thread_count))

