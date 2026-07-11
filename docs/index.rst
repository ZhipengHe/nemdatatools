NEMDataTools documentation
==========================

NEMDataTools is an MIT-licensed Python package for accessing and
preprocessing Australian Energy Market Operator (AEMO) data for the
National Electricity Market (NEM). One :func:`nemdatatools.fetch` call
serves a table over any date range by stitching AEMO's three publication
tiers — the MMSDM monthly archive, Reports ARCHIVE daily bundles, and
Reports CURRENT files — with known availability gaps failing loudly.

.. toctree::
   :maxdepth: 2
   :caption: Overview

   README

.. toctree::
   :maxdepth: 2
   :caption: User Guide

   guide/data-sources
   guide/tables
   guide/time-and-resampling
   guide/caching

.. toctree::
   :maxdepth: 2
   :caption: API Reference

   api

.. toctree::
   :maxdepth: 1
   :caption: Development

   contributing
