#!/usr/bin/env python3
import re
import sys
from typing import TextIO
from rich.console import Console
from rich.table import Table

# hh:mm:ss.(us) [$LEVEL{4}]$ID{1}>$VOTED{1}:$ROLE{1}$TERM{2}:$SNAPSHOT/$COMMIT/$APPEND ${LOG}
ptn = re.compile(
    r"^(\d{2}:\d{2}:\d{2}.\d{6})\s+\[(\w+)\](\d+)>(\w+):(\w{1})(\d+):(\d+)/(\d+)/(\d+)\s+(.*)$"
)

NO_VOTE = "N"
FOLLOWER = "F"
CANDIDATE = "C"
LEADER = "L"
SELF_VOTE = "S"

class State:
    def __init__(self, groups: tuple[str,...]):
        assert len(groups) == 10
        self.ts = groups[0]
        self.log_level = groups[1]
        self.pid = int(groups[2])
        self.vote = groups[3]
        self.role = groups[4]
        self.term = str(int(groups[5]))
        self.snapshot_log = str(int(groups[6]))
        self.commit_log = str(int(groups[7]))
        self.append_log = str(int(groups[8]))
        self.log = groups[9]

def mk_row(pid: int, n_peers: int, log: str) -> list[str]:
    result = ["" for _ in range(n_peers)]
    result[pid] = log
    return result


def get_peers(stream: TextIO = sys.stdin) -> tuple[int, str]:
    n_peers = 0
    for line in stream:
        match = ptn.match(line)
        if not match:
            return n_peers, line.strip()
        n_peers += 1


def parse_groups(match: re.Match[str]) -> State:
    return State(match.groups())

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
