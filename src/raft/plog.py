#!/usr/bin/env python3
import re
import sys
from typing import TextIO
from rich.console import Console
from rich.table import Table

# hh:mm:ss.(us) [$LEVEL{4}]$ID{1}>$VOTED{1}:$ROLE{1}$TERM{2} ${LOG}
ptn = re.compile(
    r"^(\d{2}:\d{2}:\d{2}.\d{6})\s+\[(\w+)\](\d{1})>(\w{1}):(\w{1})(\d+)\s+(.*)$"
)

NO_VOTE = "n"
FOLLOWER = "F"
CANDIDATE = "C"
LEADER = "L"


def mk_row(pid: int, n_peers: int, log: str) -> list[str]:
    result = ["" for _ in range(n_peers)]
    result[pid] = log
    return result


def get_peers(stream: TextIO = sys.stdin) -> int:
    n_peers = 0
    for line in stream:
        match = ptn.match(line)
        if not match:
            return n_peers
        n_peers += 1


def parse_groups(match: re.Match[str]) -> tuple[str, str, int, str, str, str, str]:
    time, level, _pid, vote, role, term, log = match.groups()
    return time, level, int(_pid), vote, role, term, log


if __name__ == "__main__":
    n_peers = get_peers()

    last_peer: int = None
    last_leader: int = None

    table = Table(title="Raft Log", padding=0)
    table.add_column("R", justify="center", no_wrap=True)  # Role
    table.add_column("V", justify="center", no_wrap=True)  # Term
    table.add_column("T", justify="center", no_wrap=True)  # Term
    for _ in range(n_peers):
        table.add_column(f"Peer {_}", justify="left", no_wrap=False)

    for line in sys.stdin:
        match = ptn.match(line)
        if not match:
            continue
        _ts, _level, pid, vote, role, term, log = parse_groups(match)

        if last_peer is not pid:
            last_peer = pid
            table.add_section()

        if role == LEADER and last_leader is not pid:
            last_leader = pid
            table.add_section()

        rows = [role, vote, term] + mk_row(pid, n_peers, log)
        table.add_row(*rows)
    console = Console()
    console.print(table)
