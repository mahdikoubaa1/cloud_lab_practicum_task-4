# !/usr/bin/env python3

import asyncio
import sys
import random
from operator import itemgetter
from time import sleep
from concurrent.futures.thread import ThreadPoolExecutor
from testsupport import run, async_subtest, fail, warn
from socketsupport import run_leader, run_kvs, run_ctl
from ctlsupport import ctl_response

def kill_nodes(nodes) -> None:
    for i in range(len(nodes)):
        run(["kill", "-9", str(nodes[i][0].pid)])

async def main() -> None:
    # control transaction count
    # make sure this number is even
    TXS_COUNT = 10000

    async with async_subtest("Testing concurrent transactions without conflicting keys"):
        """
        do 10k transactions on 30k keys
        each transaction modify 3 keys with out conflicts
        """
        leader = run_leader("127.0.0.1:40000", "127.0.0.1:41000")
        kvs1   = run_kvs("127.0.0.1:42000", "127.0.0.1:43000", "127.0.0.1:41000")
        kvs2   = run_kvs("127.0.0.1:44000", "127.0.0.1:45000", "127.0.0.1:41000")
        kvs_list = [[leader, "127.0.0.1:40000", "127.0.0.1:41000"],
                    [kvs1,   "127.0.0.1:42000", "127.0.0.1:43000"],
                    [kvs2,   "127.0.0.1:44000", "127.0.0.1:45000"],
        ]
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

        KEY_COUNT = TXS_COUNT * 3
        # init key-value pairs 0 - 29999, value = key
        numbers = list(map(str, range(0, KEY_COUNT)))
        doubled = " ".join(list(map(lambda p: f"{p[0]} {p[1]}", list(zip(numbers, numbers.copy())))))
        # init kvs
        run_ctl("127.0.0.1:40000", "tx_begin",  "init " + " ".join(numbers))
        run_ctl("127.0.0.1:40000", "tx_put",    f"init {doubled}")
        run_ctl("127.0.0.1:40000", "tx_commit", "init")

        def transaction_simple(name: str, key: int):
            value = random.randint(1, 10000)
            # modifies key, key + 10000, key + 20000
            keys = [(key + i * TXS_COUNT) % KEY_COUNT for i in range(0, 3)]
            run_ctl("127.0.0.1:40000", "tx_begin",  f"{name} {keys[0]} {keys[1]} {keys[2]}")
            run_ctl("127.0.0.1:40000", "tx_get",    f"{name} {keys[0]}")
            run_ctl("127.0.0.1:40000", "tx_put",    f"{name} {keys[0]} {value}")
            run_ctl("127.0.0.1:40000", "tx_get",    f"{name} {keys[1]}")
            run_ctl("127.0.0.1:40000", "tx_put",    f"{name} {keys[1]} {value}")
            run_ctl("127.0.0.1:40000", "tx_get",    f"{name} {keys[2]}")
            run_ctl("127.0.0.1:40000", "tx_put",    f"{name} {keys[2]} {value}")
            run_ctl("127.0.0.1:40000", "tx_commit", f"{name}")

        loop = asyncio.get_running_loop()

        try:
            jobs: list[asyncio.Future[None]] = []
            with ThreadPoolExecutor(max_workers=4) as executor:
                # a lot of transactions updating the value
                jobs = [loop.run_in_executor(executor, transaction_simple, f"test_{i}", i) for i in range(0, TXS_COUNT)]

            await asyncio.gather(*jobs)
            sleep(2)

            run_ctl("127.0.0.1:40000", "tx_begin", "results " + " ".join(numbers))
            results = run_ctl("127.0.0.1:40000", "tx_get", f"results " + " ".join(numbers))
            run_ctl("127.0.0.1:40000", "tx_commit", "results")
            results = ctl_response.parse(results)

            results = sorted(results.kvps.items(), key=lambda kv: int(kv[0]))
            for i in range(0, TXS_COUNT):
                if not (results[i][1] == results[i + TXS_COUNT][1] == results[i + 2 * TXS_COUNT][1]):
                    fail(f"Test failed with unmatched value for key {i}, {i + TXS_COUNT}, {i + 2 * TXS_COUNT}: "
                         f"{results[i][1]}, {results[i + TXS_COUNT][1]}, {results[i + 2 * TXS_COUNT][1]}")

        except Exception as e:
            for j in jobs:
                j.cancel()
            warn("Failed first subtest")
            fail(f"Test failed with exception: {e}")

        finally:
            kill_nodes(kvs_list)

        print("Passing first subtest")
        print("Test successful.")
        sys.exit(0)

if __name__ == "__main__":
    asyncio.run(main())
