#!/bin/bash
# PreToolUse hook: block `gh pr merge` unless the PR description has been re-confirmed
# against the exact commit being merged.
#
# A hook cannot judge whether prose is true. What it can prove is that nobody has looked
# at the description since the branch last moved — the failure mode that actually bites,
# because pr-merge-description.sh copies the body into the merge commit message. A stale
# description does not just mislead on GitHub (where it can be edited); it becomes
# permanent git history.
#
# The contract is a sentinel line anywhere in the PR body:
#
#     <!-- description-verified: <sha> -->
#
# Whoever writes or revises the description stamps it with the branch tip they checked it
# against. If the branch advances afterwards, the sentinel no longer matches and the merge
# is blocked until someone re-reads the description and re-stamps it. Re-stamping without
# reading is possible — this enforces a deliberate act, not honesty.
#
# Self-gates on the command text rather than relying on the settings.json `if:` filter, so
# it stays correct even if that filter doesn't apply.
set -euo pipefail

INPUT=$(cat)
COMMAND=$(echo "$INPUT" | jq -r '.tool_input.command // ""')

# Not a merge → nothing to enforce.
echo "$COMMAND" | grep -qE '\bgh\b\s+pr\s+merge\b' || exit 0

# No gh, no PR, no git → don't block on an unanswerable question.
command -v gh >/dev/null 2>&1 || exit 0
HEAD_SHA=$(git rev-parse HEAD 2>/dev/null) || exit 0

# First non-flag token after `gh pr merge` is the PR ref, when one is given.
PR_ARG=$(echo "$COMMAND" | sed -nE 's/.*\bgh[[:space:]]+pr[[:space:]]+merge[[:space:]]+([^-[:space:]][^[:space:]]*).*/\1/p' | head -1)

if [ -n "$PR_ARG" ]; then
  BODY=$(gh pr view "$PR_ARG" --json body -q .body 2>/dev/null) || exit 0
else
  BODY=$(gh pr view --json body -q .body 2>/dev/null) || exit 0
fi

STAMP=$(printf '%s' "$BODY" \
  | sed -nE 's/.*<!--[[:space:]]*description-verified:[[:space:]]*([0-9a-fA-F]{7,40})[[:space:]]*-->.*/\1/p' \
  | tail -1)

# Stamp is a prefix of the tip (allows a short sha) → the description was confirmed here.
if [ -n "$STAMP" ] && [ "${HEAD_SHA#"$STAMP"}" != "$HEAD_SHA" ]; then
  exit 0
fi

# Build the "what changed since you last looked" list, when the stamp names a real commit.
NEW_COMMITS=""
if [ -n "$STAMP" ] && git cat-file -e "${STAMP}^{commit}" 2>/dev/null; then
  NEW_COMMITS=$(git log --oneline --no-decorate "${STAMP}..HEAD" 2>/dev/null | head -10)
fi

python3 - "$HEAD_SHA" "$STAMP" "$NEW_COMMITS" <<'PYEOF'
import json, sys

head, stamp, new_commits = sys.argv[1], sys.argv[2], sys.argv[3]
short = head[:7]

if not stamp:
    why = (
        "The PR description carries no `description-verified` stamp, so there is no evidence\n"
        "anyone has checked it against what is actually being merged."
    )
else:
    why = (
        f"The PR description was last verified at {stamp}, but the branch tip is now {short}.\n"
        "It describes an older state of this branch."
    )
    if new_commits:
        why += "\n\nLanded since it was verified:\n" + "\n".join(
            "  " + line for line in new_commits.splitlines()
        )

print(json.dumps({
    "continue": False,
    "stopReason": (
        "Merge blocked: the PR description has not been confirmed against this commit.\n\n"
        f"{why}\n\n"
        "This matters more than it looks: pr-merge-description.sh copies the body into the\n"
        "merge commit message, so whatever is there now becomes permanent history.\n\n"
        "To clear this:\n"
        "  1. Re-read the description against the branch as it stands "
        "(`git log --oneline origin/main..HEAD`).\n"
        "  2. Update What / Why / How / Validation / Impact in prod so they match reality —\n"
        "     including removing claims that were true when written and are not now.\n"
        f"  3. Stamp it by putting this line at the end of the body:\n"
        f"       <!-- description-verified: {head} -->\n"
        "  4. Push the updated body with `gh pr edit <n> --body-file <file>`, then merge.\n\n"
        "Full template: .github/PULL_REQUEST_TEMPLATE.md · rule: specs/workflow.md"
    ),
}))
PYEOF
