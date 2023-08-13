# !/usr/bin/env python3

import asyncio
import sys
import random
from itertools import groupby
from operator import itemgetter
from time import sleep
from concurrent.futures.thread import ThreadPoolExecutor
from testsupport import run, async_subtest, fail, warn
from socketsupport import run_leader, run_kvs, run_ctl
from ctlsupport import ctl_response, check, InvalidResponseException

def kill_nodes(nodes) -> None:
    for i in range(len(nodes)):
        run(["kill", "-9", str(nodes[i][0].pid)])

async def run_increment_subtest(message, key_count, txs_count, thread_count) -> None:
    async with async_subtest(message):

        sleep(2)

        ctl    = run_ctl("127.0.0.1:40000", "join", "127.0.0.1:43000")
        if "OK" not in ctl:
            kill_nodes(kvs_list)
            sys.exit(1)

        ctl    = run_ctl("127.0.0.1:40000", "join", "127.0.0.1:45000")
        if "OK" not in ctl:
            kill_nodes(kvs_list)
            sys.exit(1)
        sleep(2)

        # init key-value pairs 1 - 100, value = 0
        keys = list(map(str, range(0, key_count)))
        kv_list = " ".join(list(map(lambda k: f"{k} 0", keys)))
        # init kvs
        run_ctl("127.0.0.1:40000", "tx_begin",  "init " + " ".join(keys))
        run_ctl("127.0.0.1:40000", "tx_put",    f"init {kv_list}")
        run_ctl("127.0.0.1:40000", "tx_commit", "init")

        def transaction_inc(name: str, key: int) -> tuple[int, int]:
            """
            get and increment the given key
            key is returned for tracking
            if transaction successful, second element in tuple is 1, else 0
            """
            run_ctl("127.0.0.1:40000", "tx_begin", f"{name} {key}")
            current = run_ctl("127.0.0.1:40000", "tx_get", f"{name} {key}")
            current = ctl_response.parse(current)
            if len(current.kvps.items()) != 1:
                raise InvalidResponseException(
                    f"Failed to increment key: {key}, incorrect number of keys found: "
                    f"{len(current.kvps.items())} instead of {1}"
                )
            if current.kvps[f"{key}"] is None:
                raise InvalidResponseException(f"Failed to increment key: {key}, no value for target key found")

            # TODO: Msg:    Operation timed out: Timeout waiting to lock key
            if current.kvps[f"{key}"] == "ERROR":
                raise InvalidResponseException(f"Failed to increment key: {key}, ERROR for target key found")

            current = int(current.kvps[f"{key}"])

            run_ctl("127.0.0.1:40000", "tx_put", f"{name} {key} {current + 1}")
            result = run_ctl("127.0.0.1:40000", "tx_commit", f"{name}")
            return key, 1 if ctl_response.parse(result).msg == "OK" else 0

        loop = asyncio.get_running_loop()

        jobs: list[asyncio.Future[tuple[int, int]]] = []
        try:
            with ThreadPoolExecutor(max_workers=thread_count) as executor:
                for i in range(0, txs_count):
                    # when TXS >> KEY, this should definitely generate many conflicting keys
                    key = random.randint(0, key_count - 1)
                    jobs.append(loop.run_in_executor(executor, transaction_inc, f"test_{i}", key))

            tracker: list[tuple[int, int]] = await asyncio.gather(*jobs)
            sleep(2)

            run_ctl("127.0.0.1:40000", "tx_begin", "results " + " ".join(keys))
            results = run_ctl("127.0.0.1:40000", "tx_get", f"results " + " ".join(keys))
            run_ctl("127.0.0.1:40000", "tx_commit", "results")
            results = ctl_response.parse(results)

            output = {}
            for i in range(0, key_count):
                output[str(i)] = "0"

            for k, g in groupby(sorted(tracker, key=itemgetter(0)), itemgetter(0)):
                output[str(k)] = str(sum(map(itemgetter(1), g)))

            check("Failed subtest",
                  results.kvps,
                  output,
                  lambda: kill_nodes(kvs_list)
            )

        except Exception as e:
            for j in jobs:
                j.cancel()
            warn("Failed subtest")
            fail(f"Test failed with exception: {e}")

        finally:
            kill_nodes(kvs_list)

        print("Passing third subtest")
        print("Test successful.")
        sys.exit(0)

