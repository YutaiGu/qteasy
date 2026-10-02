# Minimal makefile for Sphinx documentation
#

# You can set these variables from the command line, and also
# from the environment for the first two.
SPHINXOPTS    ?=
SPHINXBUILD   ?= sphinx-build
SOURCEDIR     = source
BUILDDIR      = build

# Put it first so that "make" without argument is like "make help".
help:
	@$(SPHINXBUILD) -M help "$(SOURCEDIR)" "$(BUILDDIR)" $(SPHINXOPTS) $(O)

.PHONY: help Makefile gettext update-po i18n-stats i18n-audit i18n-qa html-en html-de html-fr html-es html-zh-tw html-ja en de fr es zh_TW ja

# 从 zh_CN 源文档提取 gettext 模板（.pot）
gettext:
	@$(SPHINXBUILD) -b gettext "$(SOURCEDIR)" "$(BUILDDIR)/gettext" $(SPHINXOPTS) $(O)

# 将 .pot 合并到各语言 .po（保留已有 msgstr）
I18N_LANGS ?= en de fr es zh_TW ja
update-po: gettext
	sphinx-intl update -p $(BUILDDIR)/gettext -l $(I18N_LANGS)

# 翻译完成度基线报告（有 untranslated/fuzzy 时退出码 1）
i18n-stats:
	python3 scripts/i18n_stats.py

# Phase 4：RTD 内链与占位符审计
i18n-audit:
	python3 scripts/i18n_link_audit.py

# Phase 4：完成度 + 内链（BUILD_HTML=1 时额外构建六语 HTML）
i18n-qa:
	bash scripts/i18n_qa.sh

# 各语言 HTML 预览构建
html-en:
	@$(SPHINXBUILD) -b html -D language=en "$(SOURCEDIR)" "$(BUILDDIR)/html/en" $(SPHINXOPTS) $(O)

html-de:
	@$(SPHINXBUILD) -b html -D language=de "$(SOURCEDIR)" "$(BUILDDIR)/html/de" $(SPHINXOPTS) $(O)

html-fr:
	@$(SPHINXBUILD) -b html -D language=fr "$(SOURCEDIR)" "$(BUILDDIR)/html/fr" $(SPHINXOPTS) $(O)

html-es:
	@$(SPHINXBUILD) -b html -D language=es "$(SOURCEDIR)" "$(BUILDDIR)/html/es" $(SPHINXOPTS) $(O)

html-zh-tw:
	@$(SPHINXBUILD) -b html -D language=zh_TW "$(SOURCEDIR)" "$(BUILDDIR)/html/zh-tw" $(SPHINXOPTS) $(O)

html-ja:
	@$(SPHINXBUILD) -b html -D language=ja "$(SOURCEDIR)" "$(BUILDDIR)/html/ja" $(SPHINXOPTS) $(O)

# 避免语言代码被 catch-all 误交给 Sphinx（Builder name es not registered）
en de fr es zh_TW ja:
	@:

# Catch-all target: route all unknown targets to Sphinx using the new
# "make mode" option.  $(O) is meant as a shortcut for $(SPHINXOPTS).
%: Makefile
	@$(SPHINXBUILD) -M $@ "$(SOURCEDIR)" "$(BUILDDIR)" $(SPHINXOPTS) $(O)
