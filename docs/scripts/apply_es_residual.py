# coding=utf-8
"""补译 es 中与 en 相同但应为西班牙语的残留条目（短词、指数名等）。"""

from __future__ import annotations

import sys
from pathlib import Path

import polib

ROOT = Path(__file__).resolve().parent.parent / 'source' / 'locale' / 'es' / 'LC_MESSAGES'

# msgid -> msgstr（西班牙语）
RESIDUAL: dict[str, str] = {
    '贡献代码': 'Contribuir al código',
    '备注': 'Nota',
    '说明：': 'Nota:',
    '说明': 'Nota',
    'FT   : 期货': 'FT   : futuros',
    '回测': 'Backtest',
    '生成器': 'Generador',
    '维度': 'Dimensión',
    '参数名': 'Nombre del parámetro',
    '中': 'Medio',
    '元素': 'Elemento',
    '策略名称': 'Nombre de la estrategia',
    '5. 跳转导航': '5. Navegación',
    '插图': 'Figura',
    '地域': 'Región',
    '姓名': 'Nombre',
    '岗位': 'Puesto',
    '名称': 'Nombre',
    '代码': 'Código',
    '管理人': 'Gestor',
    '拼音': 'Pinyin',
    '振幅': 'Amplitud',
    '未完待续': 'Continuará',
    'Properties': 'Propiedades',
    '方向': 'Dirección',
    '概念': 'Concepto',
    '常见问题': 'Preguntas frecuentes',
    '**注意**：': '**Nota**:',
    '富时中国A50指数 (富时A50)': 'Índice FTSE China A50 (FTSE A50)',
    '恒生科技指数': 'Hang Seng Tech Index',
    '道琼斯工业指数': 'Dow Jones Industrial Average',
    '纳斯达克指数': 'NASDAQ Composite',
    '富时100指数': 'FTSE 100',
    '日经225指数': 'Nikkei 225',
    '韩国综合指数': 'KOSPI Composite',
    '澳大利亚标普200指数': 'S&P/ASX 200',
    'STOXX欧洲50指数': 'STOXX Europe 50',
    '港股通（上海）': 'Stock Connect (Shanghai)',
    '港股通（深圳）': 'Stock Connect (Shenzhen)',
    '息税前利润': 'EBIT',
    '息税折旧摊销前利润': 'EBITDA',
    '**空**': '**vacío**',
    'HistoryPanel（历史数据面板）': 'HistoryPanel (panel de datos históricos)',
    '日期': 'Fecha',
    '描述': 'Descripción',
    '成交量': 'Volumen',
    '所在省份': 'Provincia',
    '市盈率TTM': 'PER TTM',
    '请注意：': 'Tenga en cuenta:',
    '模式': 'Modo',
    '建议': 'Sugerencia',
    '接口': 'Interfaz',
    '审计': 'Auditoría',
    '表达式': 'Expresión',
}


def main() -> int:
    """应用到所有 es po 文件。"""
    filled = 0
    for po_path in sorted(ROOT.rglob('*.po')):
        if po_path.name.endswith('~'):
            continue
        po = polib.pofile(str(po_path))
        changed = False
        for entry in po:
            if entry.obsolete or not entry.msgid:
                continue
            if entry.msgid in RESIDUAL:
                entry.msgstr = RESIDUAL[entry.msgid]
                if 'fuzzy' in entry.flags:
                    entry.flags.remove('fuzzy')
                filled += 1
                changed = True
        if changed:
            po.metadata['PO-Revision-Date'] = '2026-05-24 14:00+0800'
            po.save(str(po_path))
    print(f'apply_es_residual: filled {filled} entries')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
