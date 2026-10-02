# coding=utf-8
"""Phase 4：扫描各语言 .po 的 msgstr 中错误的 RTD 内链与翻译占位符残留。"""

from __future__ import annotations

import re
import sys
from pathlib import Path
from typing import Dict, List, Tuple

import polib

_SCRIPTS = Path(__file__).resolve().parent
LOCALE_ROOT = _SCRIPTS.parent / 'source' / 'locale'

# 各语言 msgstr 中应使用的 RTD 路径前缀（小写比较）
EXPECTED_RTD: Dict[str, str] = {
    'en': '/en/latest/',
    'de': '/de/latest/',
    'fr': '/fr/latest/',
    'es': '/es/latest/',
    'zh_TW': '/zh-tw/latest/',
    'ja': '/ja/latest/',
}

RTD_ANY = re.compile(
    r'https://qteasy\.readthedocs\.io/([a-z-]+)/latest/',
    re.I,
)
QTEASY_PLACEHOLDER = re.compile(r'⟦QTEASY\d*⟧')
ZHCN_IN_MSGSTR = re.compile(
    r'https://qteasy\.readthedocs\.io/zh-cn/latest/',
    re.I,
)


def audit_language(lang: str) -> Tuple[List[str], int]:
    """审计单一语言目录。

    Parameters
    ----------
    lang : str
        语言代码，如 ``de``、``zh_TW``。

    Returns
    -------
    Tuple[List[str], int]
        (问题描述列表, 扫描的 msgstr 条数)。
    """
    issues: List[str] = []
    root = LOCALE_ROOT / lang / 'LC_MESSAGES'
    if not root.is_dir():
        return [f'missing locale dir: {root}'], 0

    expected = EXPECTED_RTD.get(lang, '')
    scanned = 0

    for po_path in sorted(root.rglob('*.po')):
        if po_path.name.endswith('~'):
            continue
        rel = po_path.relative_to(root)
        po = polib.pofile(str(po_path))
        for entry in po:
            if entry.obsolete or not entry.msgid or not entry.msgstr:
                continue
            scanned += 1
            text = entry.msgstr
            if QTEASY_PLACEHOLDER.search(text):
                issues.append(
                    f'{lang}/{rel}: QTEASY placeholder in msgstr: {entry.msgid[:40]!r}'
                )
            if ZHCN_IN_MSGSTR.search(text):
                issues.append(
                    f'{lang}/{rel}: zh-cn link in msgstr (expected {expected!r})'
                )
            for m in RTD_ANY.finditer(text):
                found = f'/{m.group(1).lower()}/latest/'
                if expected and found != expected.lower():
                    issues.append(
                        f'{lang}/{rel}: RTD {found!r} in msgstr, expected {expected!r} '
                        f':: {entry.msgid[:36]!r}'
                    )
    return issues, scanned


def main() -> int:
    """扫描 DEFAULT 语言并打印报告；有问题时退出码 1。"""
    langs = sys.argv[1:] if len(sys.argv) > 1 else list(EXPECTED_RTD.keys())
    total_issues = 0
    print(f'[i18n_link_audit] locale root: {LOCALE_ROOT.resolve()}\n')
    for lang in langs:
        issues, scanned = audit_language(lang)
        status = 'OK' if not issues else f'FAIL ({len(issues)} issues)'
        print(f'{lang:<6} scanned_msgstr={scanned:>6}  {status}')
        for line in issues[:20]:
            print(f'  - {line}')
        if len(issues) > 20:
            print(f'  ... and {len(issues) - 20} more')
        total_issues += len(issues)
    print()
    if total_issues:
        print(f'[i18n_link_audit] total issues: {total_issues}')
        return 1
    print('[i18n_link_audit] all checks passed')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
