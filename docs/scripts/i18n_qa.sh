#!/usr/bin/env bash
# Phase 4：全语言 msgfmt 完成度 + RTD 内链审计 + 可选 HTML 构建
set -euo pipefail
DOCS="$(cd "$(dirname "$0")/.." && pwd)"
cd "$DOCS"
PY=/opt/anaconda3/envs/py39/bin/python
BUILD_HTML="${BUILD_HTML:-0}"

echo "==> Phase 4 QA @ $DOCS"
echo "==> 1/3 i18n-stats"
$PY scripts/i18n_stats.py | tee /tmp/i18n_stats.txt

fail=0
while read -r line; do
  lang=$(echo "$line" | awk '{print $1}')
  pct=$(echo "$line" | awk '{print $NF}')
  case "$lang" in
    en|de|fr|es|zh_TW|ja)
      if [[ "$pct" != "100.0%" ]]; then
        echo "FAIL: $lang is $pct (expected 100.0%)"
        fail=1
      fi
      ;;
  esac
done < <(grep -E '^(en|de|fr|es|zh_TW|ja) ' /tmp/i18n_stats.txt)

echo "==> 2/3 link audit"
$PY scripts/i18n_link_audit.py || fail=1

if [[ "$BUILD_HTML" == "1" ]]; then
  echo "==> 3/3 html builds (all languages)"
  for target in html-en html-de html-fr html-es html-zh-tw html-ja; do
    echo "--- make $target"
    make "$target"
  done
else
  echo "==> 3/3 html builds skipped (set BUILD_HTML=1 to enable)"
fi

if [[ "$fail" -ne 0 ]]; then
  echo "==> Phase 4 QA FAILED"
  exit 1
fi
echo "==> Phase 4 QA PASSED"
