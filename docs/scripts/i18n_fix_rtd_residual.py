# coding=utf-8
"""Phase 4：批量修正各语言 msgstr 中残留的 zh-cn/zh/latest/en 等错误 RTD 路径。"""

from __future__ import annotations

import re
from pathlib import Path

import polib

LOCALE_ROOT = Path(__file__).resolve().parent.parent / 'source' / 'locale'

LANG_RTD: dict[str, str] = {
    'en': 'en',
    'de': 'de',
    'fr': 'fr',
    'es': 'es',
    'zh_TW': 'zh-tw',
    'ja': 'ja',
}

WRONG_PREFIXES = [
    re.compile(r'https://qteasy\.readthedocs\.io/zh-cn/latest/', re.I),
    re.compile(r'https://qteasy\.readthedocs\.io/zh/latest/', re.I),
    re.compile(r'https://qteasy\.readthedocs\.io/en/latest/', re.I),
    re.compile(r'https://qteasy\.readthedocs\.io/de/latest/', re.I),
    re.compile(r'https://qteasy\.readthedocs\.io/fr/latest/', re.I),
    re.compile(r'https://qteasy\.readthedocs\.io/es/latest/', re.I),
    re.compile(r'https://qteasy\.readthedocs\.io/ja/latest/', re.I),
    re.compile(r'https://qteasy\.readthedocs\.io/zh-tw/latest/', re.I),
]
QTEASY_PH = re.compile(r'⟦QTEASY\d*⟧')


def fix_msgstr(text: str, rtd_code: str) -> str:
    """将单条 msgstr 中的 RTD 链接统一为目标语言。"""
    correct = f'https://qteasy.readthedocs.io/{rtd_code}/latest/'
    out = QTEASY_PH.sub('qteasy', text)
    for pat in WRONG_PREFIXES:
        out = pat.sub(correct, out)
    return out


def main() -> int:
    """仅修改各 .po 条目的 msgstr，不触碰 msgid。"""
    total_entries = 0
    total_files = 0
    for lang, rtd in LANG_RTD.items():
        root = LOCALE_ROOT / lang / 'LC_MESSAGES'
        if not root.is_dir():
            continue
        for po_path in root.rglob('*.po'):
            if po_path.name.endswith('~'):
                continue
            po = polib.pofile(str(po_path))
            changed = False
            for entry in po:
                if entry.obsolete or not entry.msgstr:
                    continue
                new = fix_msgstr(entry.msgstr, rtd)
                if new != entry.msgstr:
                    entry.msgstr = new
                    total_entries += 1
                    changed = True
            if changed:
                po.save(str(po_path))
                total_files += 1
                print(f'  fixed {lang}/{po_path.relative_to(root)}')
    print(f'i18n_fix_rtd_residual: {total_entries} entries in {total_files} files')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
