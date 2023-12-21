#!/usr/bin/env python3
import sys
from rich.console import Console, ScreenContext
from rich.table import Table

from plog import ptn, get_peers, parse_groups, NO_VOTE, FOLLOWER, CANDIDATE, LEADER, SELF_VOTE


class PeerStat:
    def __init__(self):
        self.term = ""
        self.vote = ""
        self.role = ""
        self.log = ""
        self.log_index = ""
        self.updated = False


ROLE_NAME_MAP = {
    FOLLOWER: "Follower",
    CANDIDATE: "Candidate",
    LEADER: "Leader",
}


stat_history: list[Table] = []


def print_stat(screen: ScreenContext, stats: dict[int, PeerStat], ts: str):
    table = Table(title="Raft Status", show_lines=True, highlight=True)
    table.add_column(ts, justify="right", no_wrap=True)  # Status
    for index, stat in stats.items():
        table.add_column(
            f"Peer {index}",
            justify="left",
            no_wrap=False,
            style="green" if stat.updated else None,
        )
        stat.updated = False

    table.add_row(*(["Term"] + list(map(lambda x: x.term, stats.values()))))
    table.add_row(*(["Role"] + list(map(lambda x: x.role, stats.values()))))
    table.add_row(*(["Commit/Append"] + list(map(lambda x: x.log_index, stats.values()))))
    table.add_row(*(["Log"] + list(map(lambda x: x.log, stats.values()))))

    if len(stat_history) == 2:
        stat_history.pop(0)
    stat_history.append(table)
    stat_history[0].title = "Previous Status"
    stat_history[-1].title = "Current Status"
    screen.update(*stat_history)


if __name__ == "__main__":
    log_file = sys.argv[1]
    interactive = True
    target_ts: str = None
    if len(sys.argv) >= 3:
        interactive = False
        target_ts = sys.argv[2]

    console = Console()
    with open(log_file, "r") as f:
        n_peers, _ = get_peers(f)
        if not n_peers:
            console.print("No log found")
            sys.exit(1)
        peer_stats: dict[int, PeerStat] = {i: PeerStat() for i in range(n_peers)}

        with console.screen() as screen:
            for line in f:
                match = ptn.match(line)
                if not match:
                    continue

                state = parse_groups(match)

                if state.role == FOLLOWER:
                    if state.vote == NO_VOTE:
                        state.role = f"{ROLE_NAME_MAP[state.role]} without leader"
                    else:
                        state.role = f"Following {state.vote}"
                else:
                    state.role = ROLE_NAME_MAP[state.role]

                if state.vote == NO_VOTE:
                    state.vote = "No Vote"
                elif state.vote == SELF_VOTE:
                    state.vote = "Self"
                else:
                    state.vote = f"Peer {state.vote}"


                peer_stats[state.pid].term = state.term
                peer_stats[state.pid].vote = state.vote
                peer_stats[state.pid].role = state.role
                peer_stats[state.pid].log_index = f"{state.commit_log} / {state.last_log}"
                peer_stats[state.pid].log = f"[{state.log_level[0]}]" + state.log
                peer_stats[state.pid].updated = True

                print_stat(screen, peer_stats, state.ts)

                if target_ts is not None and state.ts == target_ts:
                    # Skip prompt until we reach the target timestamp
                    interactive = True

                try:
                    if interactive:
                        console.input()
                except:
                    sys.exit(0)
            console.input("Press any key to exit...")
