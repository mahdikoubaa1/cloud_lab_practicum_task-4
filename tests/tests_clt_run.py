#!/usr/bin/env python3

import sys
from time import sleep
from testsupport import run, fail
from socketsupport import run_leader, run_kvs, run_tests_clt, kill_leftover_procs


def kill_nodes(nodes) -> None:
    for i in range(len(nodes)):
        run(["kill", "-9", str(nodes[i].pid)])


def main() -> None:
    try:
        kvs_list = []
        if len(sys.argv) != 2 or (sys.argv[1] != "1" and sys.argv[1] != "2"):
            print("Argument must be 1 or 2")
            sys.exit(1)
        arg = sys.argv[1]
        kill_leftover_procs()

        leader = run_leader(
            "127.0.0.1:40000",
            "127.0.0.1:41000"
        )
        sleep(2)

        kvs = run_kvs(
            "127.0.0.1:42000",
            "127.0.0.1:43000",
            "127.0.0.1:41000"
        )
        sleep(2)

        kvs_list = [leader, kvs]

        tests_clt = run_tests_clt([arg])
        print("tests_clt_stdout:\n", tests_clt.stdout)
        sys.exit(0)

    except Exception as e:
        fail(f"Failed with exception: {e}")

    finally:
        kill_nodes(kvs_list)


if __name__ == "__main__":
    main()
