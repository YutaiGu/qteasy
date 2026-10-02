# coding=utf-8
"""确保 zh_TW .po 的 msgstr 中 RTD 链接指向 /zh-tw/latest/。"""

from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent / 'source' / 'locale' / 'zh_TW' / 'LC_MESSAGES'

PAT_EN = re.compile(r'https://qteasy\.readthedocs\.io/en/latest/', re.I)
PAT_ZHCN = re.compile(r'https://qteasy\.readthedocs\.io/zh-cn/latest/', re.I)


def main() -> int:
    """修正 zh_TW po 中的文档内链。"""
    fixed = 0
    for path in ROOT.rglob('*.po'):
        if path.name.endswith('~'):
            continue
        text = path.read_text(encoding='utf-8')
        new = PAT_EN.sub('https://qteasy.readthedocs.io/zh-tw/latest/', text)
        new = PAT_ZHCN.sub('https://qteasy.readthedocs.io/zh-tw/latest/', new)
        if new != text:
            path.write_text(new, encoding='utf-8')
            fixed += 1
    print(f'fix_zh_tw_rtd_links: updated {fixed} files')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
