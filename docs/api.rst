API Reference
=============

The public surface is everything importable from ``nemdatatools``.
All date arguments accept naive datetimes, dates, or ``YYYY/MM/DD`` /
``YYYY/MM/DD HH:MM:SS`` strings, interpreted as NEM time (fixed UTC+10).

Fetching data
-------------

.. autofunction:: nemdatatools.fetch

.. autofunction:: nemdatatools.fetch_price_and_demand

.. autofunction:: nemdatatools.fetch_mmsdm_table

Discovery
---------

.. autofunction:: nemdatatools.tables

.. autofunction:: nemdatatools.availability

.. autodata:: nemdatatools.NEM_REGIONS

Transforms
----------

.. autofunction:: nemdatatools.resample

Caching
-------

.. autoclass:: nemdatatools.Cache
   :members:

Errors
------

.. autoexception:: nemdatatools.NemDataError

.. autoexception:: nemdatatools.AvailabilityGapError

.. autoexception:: nemdatatools.CoverageError
