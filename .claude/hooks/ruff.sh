#!/usr/bin/env bash
# PostToolUse: reject an edit that leaves a Python file unformatted or failing ruff; exit 2 hands the message back to Claude.
set -euo pipefail
command -v jq > /dev/null || { echo "ruff hook: jq not found, skipping (nox -s lint still runs it)." >&2; exit 0; }
file=$(jq -r '.tool_input.file_path // empty')
case "$file" in
  *.py) ;;
  *) exit 0 ;;
esac
[ -f "$file" ] || exit 0
if command -v ruff > /dev/null; then
  ruff_cmd=(ruff)
elif command -v uvx > /dev/null; then
  ruff_cmd=(uvx --quiet ruff)
else
  echo "ruff hook: neither ruff nor uvx found, skipping (nox -s lint still runs it)." >&2
  exit 0
fi
cd "${CLAUDE_PROJECT_DIR:-.}"
if ! out=$("${ruff_cmd[@]}" format --check "$file" 2>&1); then
  echo "ruff: $file is not formatted. Run 'nox -s format' (or 'ruff format $file') and re-check. $out" >&2
  exit 2
fi
if ! out=$("${ruff_cmd[@]}" check "$file" 2>&1); then
  echo "ruff check failed on $file: $out" >&2
  exit 2
fi
