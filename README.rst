t4Monitor
#########

.. image:: https://travis-ci.org/fernandezcuesta/t4Monitor.svg?branch=master
  :target: https://travis-ci.org/fernandezcuesta/t4Monitor
.. image:: https://coveralls.io/repos/fernandezcuesta/t4Monitor/badge.svg?branch=develop&service=github
  :target: https://coveralls.io/github/fernandezcuesta/t4Monitor?branch=develop

- Download OpenVMS statistics (T4) over sftp through optional SSH tunnels over
  a single gateway.
- T4-Format (flavours 1 and 2) CSV to Pandas dataframe. Merge all remote CSVs
  into a single, plain (not T4) CSV and a gzipped pickle file containing the
  dataframe and its associated metadata.
- Apply simple arithmetical operations to the dataframe.
- HTML (Jinja2 templates) report generation by graphing selected dataframe
  columns.
