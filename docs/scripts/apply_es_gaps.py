# coding=utf-8
"""补齐 es .po 中因 msgid 与 en 不一致或短 token 导致的空条目。"""

from __future__ import annotations

import re
import sys
from pathlib import Path

import polib
from deep_translator import GoogleTranslator

_SCRIPTS = Path(__file__).resolve().parent
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

from fill_en_po import is_copy_as_is
from pivot_translate_from_en import load_cache, save_cache, translate_text

ES_ROOT = _SCRIPTS.parent / 'source' / 'locale' / 'es' / 'LC_MESSAGES'
EN_ROOT = _SCRIPTS.parent / 'source' / 'locale' / 'en' / 'LC_MESSAGES'

RTD_IN_MSGID = re.compile(r'https://qteasy\.readthedocs\.io/(?:zh-cn|en)/latest/', re.I)


def norm_msgid(msgid: str) -> str:
    """归一化 msgid 便于跨语言 po 对齐（仅 URL 差异）。"""
    return RTD_IN_MSGID.sub('https://qteasy.readthedocs.io/RTD/latest/', msgid)


def build_en_indexes(en_po: polib.POFile) -> tuple[dict[str, str], dict[str, str]]:
    """构建 en msgid 与归一化 msgid 索引。"""
    exact: dict[str, str] = {}
    normalized: dict[str, str] = {}
    for e in en_po:
        if e.obsolete or not e.msgid or not e.msgstr:
            continue
        exact[e.msgid] = e.msgstr
        normalized[norm_msgid(e.msgid)] = e.msgstr
    return exact, normalized


def main() -> int:
    """补齐所有 es 空 msgstr。"""
    cache = load_cache('es')
    translator = GoogleTranslator(source='en', target='es')
    filled_copy = filled_tr = 0

    for es_path in sorted(ES_ROOT.rglob('*.po')):
        if es_path.name.endswith('~'):
            continue
        en_path = EN_ROOT / es_path.relative_to(ES_ROOT)
        if not en_path.is_file():
            continue
        es_po = polib.pofile(str(es_path))
        en_po = polib.pofile(str(en_path))
        exact, normalized = build_en_indexes(en_po)
        changed = False

        for entry in es_po:
            if entry.obsolete or not entry.msgid or entry.msgstr:
                continue
            en_s = exact.get(entry.msgid) or normalized.get(norm_msgid(entry.msgid), '')
            if not en_s:
                print(f'  skip no-en: {es_path.name}: {entry.msgid[:50]!r}')
                continue
            if is_copy_as_is(entry.msgid):
                entry.msgstr = en_s
                filled_copy += 1
            else:
                entry.msgstr = translate_text(en_s, translator, cache, 'es')
                filled_tr += 1
            if 'fuzzy' in entry.flags:
                entry.flags.remove('fuzzy')
            changed = True

        if changed:
            es_po.metadata['PO-Revision-Date'] = '2026-05-24 14:00+0800'
            es_po.save(str(es_path))

    save_cache('es', cache)
    print(f'apply_es_gaps: copy={filled_copy} translated={filled_tr}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
