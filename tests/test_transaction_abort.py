# !/usr/bin/env python3

import sys
from time import sleep
from testsupport import subtest, run
from socketsupport import run_leader, run_kvs, run_ctl
from ctlsupport import ctl_response, check

def kill_nodes(nodes) -> None:
    for i in range(len(nodes)):
        run(["kill", "-9", str(nodes[i][0].pid)])

def main() -> None:
    with subtest("Testing transaction rollback (due to abort)"):
        leader = run_leader("127.0.0.1:40000", "127.0.0.1:41000")
        kvs    = run_kvs("127.0.0.1:42000", "127.0.0.1:43000", "127.0.0.1:41000")
        kvs_list = [[leader, "127.0.0.1:40000", "127.0.0.1:41000"],
                    [kvs, "127.0.0.1:42000", "127.0.0.1:43000"]]
        sleep(2)

        ctl    = run_ctl("127.0.0.1:40000", "join", "127.0.0.1:43000")
        if "OK" not in ctl:
            kill_nodes(kvs_list)
            sys.exit(1)
        sleep(2)

        # init kvs
        run_ctl("127.0.0.1:40000", "tx_begin", "init 1 2 3")
        run_ctl("127.0.0.1:40000", "tx_put", "init 1 10 2 10 3 10")
        run_ctl("127.0.0.1:40000", "tx_commit", "init")

        # begin transaction
        run_ctl("127.0.0.1:40000", "tx_begin", "a 1 2 3")
        run_ctl("127.0.0.1:40000", "tx_put", "a 1 60 2 60 3 60")

        # abort
        run_ctl("127.0.0.1:40000", "tx_abort", "a")

        sleep(2)

        # get results
        run_ctl("127.0.0.1:40000", "tx_begin", "results 1 2 3")
        results = run_ctl("127.0.0.1:40000", "tx_get", "results 1 2 3")

        # save results
        run_ctl("127.0.0.1:40000", "tx_commit", "results")

        # check results
        results = ctl_response.parse(results)
        check("Failed first subtest",
                results.kvps,
                { "1": "10", "2": "10", "3": "10"},
                lambda: kill_nodes(kvs_list)
        )

        # clean up
        kill_nodes(kvs_list)

        print("Passing first subtest")
        print("Test successful.")
        sys.exit(0)

if __name__ == "__main__":
    main()

