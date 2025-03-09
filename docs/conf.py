"""
Configuration file for the Sphinx documentation builder.

For the full list of built-in configuration values, see the documentation:
https://www.sphinx-doc.org/en/master/usage/configuration.html
"""

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = "NEMDataTools"
copyright = "2025, Zhipeng He"
author = "Zhipeng He"

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.viewcode",
    "sphinx.ext.napoleon",
    "sphinx.ext.coverage",
    "myst_parser",
]

templates_path = ["_templates"]
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]


# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = "sphinx_rtd_theme"
html_static_path = ["_static"]

html_title = "NEMDataTools Documentation"
html_baseurl = "https://zhipenghe.me/nemdatatools"

# Configure myst-parser
myst_enable_extensions = [
    "colon_fence",
    "deflist",
]

# Add .md to source suffixes
source_suffix = {
    ".rst": "restructuredtext",
    ".md": "markdown",
}
