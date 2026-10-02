#!/usr/bin/env bash
# 西班牙语 es 机翻完成后的收尾（须在 qteasy/docs 目录执行）
set -euo pipefail
DOCS="$(cd "$(dirname "$0")/.." && pwd)"
cd "$DOCS"
PY=/opt/anaconda3/envs/py39/bin/python

echo "==> docs root: $DOCS"
echo "==> i18n-stats (before)"
make i18n-stats es | grep '^es' || true

"$PY" scripts/apply_es_gaps.py
"$PY" scripts/apply_es_residual.py
"$PY" scripts/pivot_translate_from_en.py --lang es --skip-fill
"$PY" scripts/fix_es_rtd_links.py

echo "==> i18n-stats (after)"
make i18n-stats es | grep '^es' || true

echo "==> html-es build"
"$PY" -m sphinx -b html -D language=es source build/html/es
echo "Done. HTML: $DOCS/build/html/es"
