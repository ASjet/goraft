#!/usr/bin/env python3
import re
import sys
from typing import TextIO
from rich.console import Console
from rich.table import Table
from lib import get_peers, parse_groups, mk_row, State, ptn, LEADER


def read_one(stream: TextIO = sys.stdin) -> bool:
    n_peers, title = get_peers(stream)
    if not n_peers:
        return False

    last_peer: int = None
    last_leader: int = None

    table = Table(title=title, padding=0)
    table.add_column("R", justify="center", no_wrap=True)  # Role
    table.add_column("V", justify="center", no_wrap=True)  # Vote
    table.add_column("T", justify="center", no_wrap=True)  # Term
    table.add_column("S", justify="center", no_wrap=True)  # Snapshot log
    table.add_column("C", justify="center", no_wrap=True)  # Commit log
    table.add_column("A", justify="center", no_wrap=True)  # Append log
    for _ in range(n_peers):
        table.add_column(f"Peer {_}", justify="left", no_wrap=False)


    for line in sys.stdin:
        if line.startswith("  ... Passed --"):
            console = Console()
            console.print(table)
            return True
        match = ptn.match(line)
        if not match:
            continue
        state = parse_groups(match)

        if last_peer is not state.pid:
            last_peer = state.pid
            table.add_section()

        if state.role == LEADER and last_leader is not state.pid:
            last_leader = state.pid
            table.add_section()

        rows = [state.role, state.vote, state.term, state.snapshot_log, state.commit_log, state.append_log] + mk_row(state.pid, n_peers, state.log)
        table.add_row(*rows)
    console = Console()
    console.print(table)
    return False

if __name__ == "__main__":
    while read_one(sys.stdin):
        pass
