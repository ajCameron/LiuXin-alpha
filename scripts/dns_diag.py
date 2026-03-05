#!/usr/bin/env python3
"""
Quick DNS diagnostics for Linux/WSL environments.

Examples:
    python3 scripts/dns_diag.py
    python3 scripts/dns_diag.py --host www.google.com --host pypi.org
"""

from __future__ import annotations

import argparse
import os
import socket
import subprocess
import sys
from pathlib import Path


DEFAULT_HOSTS = (
    "www.google.com",
    "www.googleapis.com",
    "www.amazon.com",
    "covers.openlibrary.org",
    "bigbooksearch.com",
)


def _is_wsl() -> bool:
    if os.environ.get("WSL_INTEROP"):
        return True
    try:
        return "microsoft" in Path("/proc/sys/kernel/osrelease").read_text(encoding="utf-8", errors="ignore").lower()
    except Exception:
        return False


def _read_resolv_conf() -> tuple[Path, list[str], list[str]]:
    path = Path("/etc/resolv.conf")
    try:
        resolved = path.resolve()
    except Exception:
        resolved = path

    nameservers: list[str] = []
    raw_lines: list[str] = []
    try:
        raw_lines = resolved.read_text(encoding="utf-8", errors="replace").splitlines()
    except Exception:
        return resolved, nameservers, raw_lines

    for line in raw_lines:
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        if stripped.lower().startswith("nameserver "):
            parts = stripped.split()
            if len(parts) >= 2:
                nameservers.append(parts[1])
    return resolved, nameservers, raw_lines


def _run_command(args: list[str]) -> tuple[int, str]:
    try:
        proc = subprocess.run(args, check=False, capture_output=True, text=True)
    except FileNotFoundError:
        return 127, f"command not found: {args[0]}"
    except Exception as err:
        return 1, repr(err)
    out = (proc.stdout or "").strip()
    err = (proc.stderr or "").strip()
    if out and err:
        merged = out + "\n" + err
    else:
        merged = out or err
    return proc.returncode, merged


def _getent_lookup(host: str) -> tuple[bool, str]:
    rc, out = _run_command(["getent", "hosts", host])
    if rc == 0 and out:
        first = out.splitlines()[0]
        return True, first
    return False, out or f"getent failed (rc={rc})"


def _socket_lookup(host: str) -> tuple[bool, str]:
    try:
        infos = socket.getaddrinfo(host, 443, 0, socket.SOCK_STREAM)
    except Exception as err:
        return False, repr(err)
    addrs: list[str] = []
    for _family, _socktype, _proto, _canonname, sockaddr in infos:
        ip = sockaddr[0]
        if ip not in addrs:
            addrs.append(ip)
    if not addrs:
        return False, "no addresses returned"
    return True, ", ".join(addrs[:3])


def _print_header(title: str) -> None:
    print()
    print(title)
    print("-" * len(title))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Diagnose DNS issues in Linux/WSL.")
    parser.add_argument("--host", action="append", dest="hosts", help="Host to resolve (repeatable).")
    parser.add_argument("--show-resolv", action="store_true", help="Print full resolv.conf content.")
    args = parser.parse_args(argv)

    hosts = tuple(args.hosts) if args.hosts else DEFAULT_HOSTS

    print("dns_diag")
    print(f"python: {sys.version.split()[0]}")
    print(f"wsl: {'yes' if _is_wsl() else 'no'}")

    resolv_path, nameservers, raw_resolv = _read_resolv_conf()
    _print_header("Resolver Config")
    print(f"/etc/resolv.conf -> {resolv_path}")
    print("nameservers:", ", ".join(nameservers) if nameservers else "(none)")
    if args.show_resolv:
        print("raw:")
        for line in raw_resolv:
            print("  " + line)

    _print_header("Lookup Results")
    all_failed = True
    for host in hosts:
        g_ok, g_detail = _getent_lookup(host)
        s_ok, s_detail = _socket_lookup(host)
        all_failed = all_failed and (not g_ok and not s_ok)
        print(f"{host}")
        print(f"  getent: {'OK' if g_ok else 'FAIL'} - {g_detail}")
        print(f"  socket: {'OK' if s_ok else 'FAIL'} - {s_detail}")

    _print_header("Heuristics")
    stale_wsl_dns = any(ns.startswith("10.") for ns in nameservers) and _is_wsl()
    if stale_wsl_dns:
        print("- WSL appears to be using a private DNS server from Windows/VPN state.")
    if all_failed:
        print("- All probes failed. DNS is currently non-functional in this environment.")
    else:
        print("- At least one DNS probe succeeded.")

    _print_header("Suggested Fixes")
    if _is_wsl():
        print("1. From PowerShell: `wsl --shutdown`, then restart WSL.")
        print("2. Re-check: `cat /etc/resolv.conf` and `getent hosts www.google.com`.")
        print("3. If still broken, pin resolvers in `/etc/wsl.conf` and `/etc/resolv.conf`.")
        print("   - /etc/wsl.conf:")
        print("     [network]")
        print("     generateResolvConf = false")
        print("   - /etc/resolv.conf:")
        print("     nameserver 1.1.1.1")
        print("     nameserver 8.8.8.8")
        print("4. If public DNS is blocked, use your Windows NIC DNS values from `ipconfig /all`.")
    else:
        print("1. Check `/etc/resolv.conf` nameservers.")
        print("2. Verify firewall/network allows DNS (UDP/TCP 53).")
        print("3. Try alternate resolvers and re-test.")

    return 0 if not all_failed else 2


if __name__ == "__main__":
    raise SystemExit(main())
