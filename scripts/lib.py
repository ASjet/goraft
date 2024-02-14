import re
import sys
from typing import TextIO

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
