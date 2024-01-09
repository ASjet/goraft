#!/usr/bin/env python3
import re
import sys
from typing import TextIO

# hh:mm:ss.(us) [$LEVEL{4}][$ID{1}]$FILE:$LINE $FUNC $LOCK_OP($LOCK_TRACE_ID)
ptn = re.compile(
    r"^(\d{2}:\d{2}:\d{2}.\d{6})\s+\[(\w+)\]\[(\d+)\](.+):(\d+):\s+(.+)\s+([\w|\s]+)\((\d+)\)$"
)

class LockOp:
    def __init__(self, groups: tuple[str, ...]) -> None:
        assert len(groups) == 8
        self.ts = groups[0]
        self.log_level = groups[1]
        self.pid = int(groups[2])
        self.file = groups[3]
        self.line = int(groups[4])
        self.func = groups[5]
        self.op = groups[6]
        self.trace_id = int(groups[7])

if __name__ == "__main__":
    ops: dict[int, LockOp] = {}
    for line in sys.stdin:
        match = ptn.match(line)
        if not match:
            continue
        op = LockOp(match.groups())

        if op.op.endswith("Log"):
            if op.op == "RLockLog" or op.op == "LockLog":
                ops[op.trace_id] = op
            else:
                ops.pop(op.trace_id, None)
        else:
            if op.op == "Lock":
                ops[op.trace_id] = op
            else:
                ops.pop(op.trace_id, None)
    if len(ops) > 0:
        sortedStateOps = sorted(ops.items(), key=lambda x: x[1].trace_id)
        for _, op in sortedStateOps:
            print(f"{op.ts} [{op.pid}] unreleased {op.op} at {op.file}:{op.line} {op.func}")
