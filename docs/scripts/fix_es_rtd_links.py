# coding=utf-8
"""修正 es .po 中 RTD / GitHub 链接占位符与 /en/latest/ 路径。"""

from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent / 'source' / 'locale' / 'es' / 'LC_MESSAGES'

PAT_QTEASY_RTD = re.compile(
    r'https://⟦QTEASY\d*⟧\.readthedocs\.io/en/latest/', re.I
)
PAT_EN_RTD = re.compile(r'https://qteasy\.readthedocs\.io/en/latest/', re.I)
PAT_QTEASY_TOKEN = re.compile(r'⟦QTEASY\d*⟧')


def main() -> int:
    """遍历 es po 并写入修正后的链接。"""
    fixed = 0
    for path in ROOT.rglob('*.po'):
        if path.name.endswith('~'):
            continue
        text = path.read_text(encoding='utf-8')
        new = PAT_QTEASY_RTD.sub('https://qteasy.readthedocs.io/es/latest/', text)
        new = PAT_EN_RTD.sub('https://qteasy.readthedocs.io/es/latest/', new)
        new = PAT_QTEASY_TOKEN.sub('qteasy', new)
        if new != text:
            path.write_text(new, encoding='utf-8')
            fixed += 1
    print(f'fix_es_rtd_links: updated {fixed} files')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
