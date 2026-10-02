#!/usr/bin/env bash
# 日语 ja 机翻完成后的收尾（须在 qteasy/docs 目录执行）
set -euo pipefail
DOCS="$(cd "$(dirname "$0")/.." && pwd)"
cd "$DOCS"
PY=/opt/anaconda3/envs/py39/bin/python

echo "==> docs root: $DOCS"
make i18n-stats 2>&1 | grep '^ja' || true

"$PY" scripts/apply_ja_gaps.py
"$PY" scripts/pivot_translate_from_en.py --lang ja --skip-fill
"$PY" scripts/fix_ja_rtd_links.py

make i18n-stats 2>&1 | grep '^ja' || true
"$PY" -m sphinx -b html -D language=ja source build/html/ja
echo "Done. HTML: $DOCS/build/html/ja"
