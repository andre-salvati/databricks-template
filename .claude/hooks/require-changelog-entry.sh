#!/bin/bash
# PreToolUse hook: block `gh pr merge` unless this branch adds a specs/CHANGELOG.md entry.
#
# A hook can't *write* the entry (hooks are deterministic shell commands, not authors) —
# it enforces that one exists and tells the agent to write it. The signal is the branch
# diff: if CHANGELOG.md isn't touched relative to main, no entry was added.
#
# Self-gates on the command text rather than relying on the settings.json `if:` filter,
# so it stays correct even if that filter doesn't apply.
set -euo pipefail

INPUT=$(cat)
COMMAND=$(echo "$INPUT" | jq -r '.tool_input.command // ""')

# Not a merge → nothing to enforce.
echo "$COMMAND" | grep -qE '\bgh\b\s+pr\s+merge\b' || exit 0

CHANGELOG="specs/CHANGELOG.md"

# Compare against the upstream default branch; fall back to a local main.
BASE=""
for ref in origin/main main; do
  if git rev-parse --verify --quiet "$ref" >/dev/null 2>&1; then BASE="$ref"; break; fi
done

# No main to compare against — don't block on an unanswerable question.
[ -z "$BASE" ] && exit 0

# `...` diffs against the merge-base, so this is "what this branch changed", not
# "how this branch differs from a main that moved on".
if git diff --name-only "$BASE...HEAD" -- "$CHANGELOG" | grep -q .; then
  exit 0
fi

BRANCH=$(git branch --show-current 2>/dev/null || echo "?")
python3 - "$BRANCH" <<'PYEOF'
import json, sys
branch = sys.argv[1]
print(json.dumps({
    "continue": False,
    "stopReason": (
        f"Merge blocked: this branch ({branch}) adds no entry to specs/CHANGELOG.md.\n"
        "Add one at the top before merging — append-only, never edit an existing entry.\n"
        "Header: ## [#<PR>] · " + branch + " · YYYY-MM-DD · <title>\n"
        "Body: at most 3 sentences. Replace the branch name with the PR URL after merge."
    ),
}))
PYEOF
