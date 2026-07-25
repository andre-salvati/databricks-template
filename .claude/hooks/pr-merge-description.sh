#!/bin/bash
# PreToolUse hook: update PR description and inject it as the merge commit message
set -euo pipefail

INPUT=$(cat)

# Use Python for all JSON parsing and output (jq has snap confinement issues in this env)
python3 - "$INPUT" <<'PYEOF'
import json, sys, os, subprocess, tempfile, shlex

raw = sys.argv[1]
try:
    data = json.loads(raw)
except json.JSONDecodeError:
    sys.exit(0)

command = data.get("tool_input", {}).get("command", "")

# Already has explicit body/subject — don't override user's intent
if any(f in command for f in ("--body", "--subject", "--body-file")):
    sys.exit(0)

# Rebase: individual commits keep their own messages
if "--rebase" in command:
    sys.exit(0)

# Extract PR ref (number, URL, or branch) — first non-flag token after 'gh pr merge'
import re
m = re.search(r'gh pr merge\s+([^\s-]\S*)', command)
pr_arg = m.group(1) if m else None

# Fetch PR metadata
try:
    cmd = ["gh", "pr", "view", "--json", "number,title,body"]
    if pr_arg:
        cmd = ["gh", "pr", "view", pr_arg, "--json", "number,title,body"]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=15)
    if result.returncode != 0:
        sys.exit(0)
    pr = json.loads(result.stdout)
except Exception:
    sys.exit(0)

number = str(pr["number"])
title  = pr["title"]
body   = pr.get("body") or ""

# Write body to temp file — avoids quoting/newline issues
with tempfile.NamedTemporaryFile(mode="w", suffix=".md", prefix="pr-body-",
                                  dir="/tmp", delete=False) as f:
    f.write(body)
    tmpfile = f.name

# Update the PR description on GitHub
try:
    subprocess.run(
        ["gh", "pr", "edit", number, "--title", title, "--body-file", tmpfile],
        capture_output=True, timeout=15
    )
except Exception:
    pass  # non-fatal

# Build updated command with --subject and --body-file
new_cmd = command + " --subject " + shlex.quote(title) + " --body-file " + tmpfile

# Return updated input to the harness
print(json.dumps({
    "hookSpecificOutput": {
        "hookEventName": "PreToolUse",
        "updatedInput": {"command": new_cmd}
    }
}))
PYEOF
