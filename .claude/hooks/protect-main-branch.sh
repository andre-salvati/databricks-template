#!/bin/bash
# PreToolUse hook: block direct commits or pushes to main
set -euo pipefail

INPUT=$(cat)
COMMAND=$(echo "$INPUT" | jq -r '.tool_input.command // ""')

# Block any git push that targets main (including HEAD:main, refs/heads/main)
if echo "$COMMAND" | grep -qE '\bgit\b.*\bpush\b' && \
   echo "$COMMAND" | grep -qE '(\s|:)main(\s|$)'; then
  printf '{"continue": false, "stopReason": "Direct push to main is blocked. Create a branch and open a PR instead."}'
  exit 0
fi

# Block git commit when currently on main
if echo "$COMMAND" | grep -qE '\bgit\b.*\bcommit\b'; then
  CURRENT_BRANCH=$(git branch --show-current 2>/dev/null || echo "")
  if [ "$CURRENT_BRANCH" = "main" ]; then
    printf '{"continue": false, "stopReason": "Direct commit to main is blocked. Create a branch first: git checkout -b <branch-name>"}'
    exit 0
  fi
fi
