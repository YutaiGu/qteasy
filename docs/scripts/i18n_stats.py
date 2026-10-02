# coding=utf-8
# ======================================
# File: i18n_stats.py
# Author: Jackie PENG
# Contact: jackie.pengzhao@gmail.com
# Created: 2026-05-23
# Desc:
# 扫描 docs/source/locale 下各语言 .po 翻译完成度，输出基线报告。
# ======================================
"""扫描 qteasy 文档各语言 .po 翻译完成度。"""

from __future__ import annotations

import glob
import os
import re
import subprocess
import sys
from typing import Dict, List, Tuple

LOCALE_ROOT = os.path.join(os.path.dirname(__file__), '..', 'source', 'locale')
DEFAULT_LANGS = ['en', 'de', 'fr', 'es', 'zh_TW', 'ja']
MSGFMT = os.environ.get('MSGFMT', 'msgfmt')


def msgfmt_statistics(po_path: str) -> Tuple[int, int, int]:
    """调用 msgfmt --statistics 解析单个 .po 文件。

    Parameters
    ----------
    po_path : str
        .po 文件路径。

    Returns
    -------
    Tuple[int, int, int]
        (translated, untranslated, fuzzy) 条目数。
    """
    result = subprocess.run(
        [MSGFMT, '--statistics', '-o', os.devnull, po_path],
        capture_output=True,
        text=True,
        check=False,
    )
    stderr = result.stderr.strip()
    translated = untranslated = fuzzy = 0
    if match := re.search(r'(\d+) translated', stderr):
        translated = int(match.group(1))
    if match := re.search(r'(\d+) untranslated', stderr):
        untranslated = int(match.group(1))
    if match := re.search(r'(\d+) fuzzy', stderr):
        fuzzy = int(match.group(1))
    return translated, untranslated, fuzzy


def scan_language(lang: str) -> Dict[str, object]:
    """汇总单一语言的翻译统计。

    Parameters
    ----------
    lang : str
        语言代码，如 ``en``、``de``。

    Returns
    -------
    Dict[str, object]
        含 files、translated、untranslated、fuzzy、percent、sections 等键。
    """
    po_files = sorted(glob.glob(
        os.path.join(LOCALE_ROOT, lang, 'LC_MESSAGES', '**', '*.po'),
        recursive=True,
    ))
    total_trans = total_untrans = total_fuzzy = 0
    complete = partial = empty = 0
    sections: Dict[str, List[int]] = {}

    for po_path in po_files:
        trans, untrans, fuzzy = msgfmt_statistics(po_path)
        total_trans += trans
        total_untrans += untrans
        total_fuzzy += fuzzy

        total = trans + untrans
        if untrans == 0 and total > 0:
            complete += 1
        elif trans == 0 and total > 0:
            empty += 1
        elif total > 0:
            partial += 1

        rel = os.path.relpath(po_path, os.path.join(LOCALE_ROOT, lang, 'LC_MESSAGES'))
        section = rel.split(os.sep)[0] if os.sep in rel else '(root)'
        if section not in sections:
            sections[section] = [0, 0]
        sections[section][0] += trans
        sections[section][1] += untrans

    grand_total = total_trans + total_untrans
    percent = 100.0 * total_trans / grand_total if grand_total else 0.0

    return {
        'files': len(po_files),
        'translated': total_trans,
        'untranslated': total_untrans,
        'fuzzy': total_fuzzy,
        'percent': percent,
        'complete_files': complete,
        'partial_files': partial,
        'empty_files': empty,
        'sections': sections,
    }


def print_report(langs: List[str]) -> None:
    """打印各语言及分目录完成度报告。

    Parameters
    ----------
    langs : List[str]
        待扫描语言代码列表。
    """
    print(f"{'lang':<6} {'files':>6} {'trans':>8} {'untrans':>8} {'fuzzy':>7} {'pct':>7}")
    print('-' * 55)
    for lang in langs:
        stats = scan_language(lang)
        print(
            f"{lang:<6} {stats['files']:>6} {stats['translated']:>8} "
            f"{stats['untranslated']:>8} {stats['fuzzy']:>7} {stats['percent']:>6.1f}%"
        )
        print(
            f"       complete={stats['complete_files']} partial={stats['partial_files']} "
            f"empty={stats['empty_files']}"
        )

    for breakdown_lang in langs:
        if breakdown_lang not in ('en', 'de'):
            continue
        print(f'\n--- Section breakdown ({breakdown_lang}) ---')
        lang_stats = scan_language(breakdown_lang)
        sections = lang_stats['sections']
        for section, (trans, untrans) in sorted(
            sections.items(),
            key=lambda item: item[1][0] / (item[1][0] + item[1][1] + 1e-9),
        ):
            total = trans + untrans
            pct = 100.0 * trans / total if total else 0.0
            print(f"  {section:<25} {pct:5.1f}%  ({trans}/{total} msgs)")


def main() -> int:
    """入口：扫描 DEFAULT_LANGS 并打印报告。

    Returns
    -------
    int
        进程退出码，0 表示成功。
    """
    langs = sys.argv[1:] if len(sys.argv) > 1 else DEFAULT_LANGS
    print(f"[i18n_stats] locale root: {os.path.abspath(LOCALE_ROOT)}\n")
    print_report(langs)
    failed = False
    for lang in langs:
        stats = scan_language(lang)
        if stats['untranslated'] or stats['fuzzy']:
            failed = True
    return 1 if failed else 0


if __name__ == '__main__':
    raise SystemExit(main())
