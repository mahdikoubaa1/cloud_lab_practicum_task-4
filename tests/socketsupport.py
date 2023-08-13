import tempfile
import sys
import random
import subprocess
import os
import time

from testsupport import (
    run_project_executable,
    info,
    warn,
    find_project_executable,
    test_root,
    project_root,
    run,
    ensure_library,
)

def run_router(api_addr: str, router_addr: str):
    router = [
        find_project_executable("router-test"),
        "-a",
        api_addr,
        "-r",
        router_addr
    ]

    info("Run router")

    proc = subprocess.Popen(router, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    return proc

def run_leader(api_addr: str, clust_addr: str):
    leader = [
        find_project_executable("kvs-test"),
        "-a",
        api_addr,
        "-c",
        clust_addr,
        "-l"
    ]

    info("Run leader")

    proc = subprocess.Popen(leader, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    return proc

def run_kvs(api_addr: str, p2p_addr: str, clust_addr: str):
    kvs = [
        find_project_executable("kvs-test"),
        "-a",
        api_addr,
        "-p",
        p2p_addr,
        "-c",
        clust_addr
    ]

    info("Run kvs")

    proc = subprocess.Popen(kvs, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    return proc
    

def run_ctl(api_addr: str, op: str, arg: str = ""):
    args = ["-a", f"{api_addr}", op]
    if " " in arg:
        args += arg.split(" ")
    elif arg == "":
        pass
    else:
        args.append(arg)

    info("Run ctl")
    print(args)
    
    proc = run_project_executable("ctl-test", args, check=False)
    print(proc.stdout)
    return proc.stdout
