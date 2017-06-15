"""
This commands has two modes: proxy, and wait.

== Proxy mode ==
Run sshuttle-telepresence via SSH IP and port given on command line.

The SSH server will run on the host, so the sshuttle-telepresence talking to it
somehow needs to get the IP of the host. So we read port and an optional IP
from the command line.

* If host is macOS an IP will be given.
* If host is Linux no IP will be given, and then we fall back to IP of default
  route.

The program expects to receive a JSON-encoded object as command line argument,
with parameters:

1. "port", the port number to connect ssh to.
2. "ip", optional, the ip of the ssh server.
3. "cidrs", a list of CIDRs for sshuttle.

References:

* https://stackoverflow.com/q/22944631/7862510
* https://docs.docker.com/docker-for-mac/networking/


== Wait mode ==

Wait mode should be run in same network namespace as the proxy. It will do the
'hellotelepresence' loop used to correct DNS on the k8s proxy, and to detect
when the proxy is working.

When the process exits with exit code 100 that means the proxy is active.
"""

import sys
import os
from json import loads
from subprocess import check_output
from socket import gethostbyname, gaierror
from time import time, sleep


def main():
    command = sys.argv[1]
    if command == "proxy":
        proxy(loads(sys.argv[2]))
    elif command == "wait":
        wait()


def proxy(config):
    """Start sshuttle proxy to Kubernetes."""
    port = config["port"]
    if "ip" in config:
        # Typically host is macOS:
        ip = config["ip"]
    else:
        # Typically host is Linux, use default route:
        for line in str(check_output(["route"]), "ascii").splitlines():
            parts = line.split()
            if parts[0] == "default":
                ip = parts[1]
                break
    cidrs = config["cidrs"]
    os.execl(
        "/usr/bin/sshuttle-telepresence", "sshuttle-telepresence", "-v",
        "--dns", "--method", "nat", "-e", (
            "ssh -oStrictHostKeyChecking=no -oUserKnownHostsFile=/dev/null " +
            "-F /dev/null"
        ), "-r", "telepresence@{}:{}".format(ip, port), *cidrs
    )


def wait():
    """Wait for proxying to be live."""
    start = time()
    while time() - start < 10:
        try:
            gethostbyname("hellotelepresence")
            sleep(1)  # just in case there's more to startup
            sys.exit(100)
        except gaierror:
            sleep(0.1)
        else:
            sleep(0.1)
    sys.exit("Failed to connect to proxy in remote cluster.")


if __name__ == '__main__':
    main()
