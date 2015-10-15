# pySMSCMon

[![Build Status](https://travis-ci.org/fernandezcuesta/pySMSCMon.svg?branch=master)](https://travis-ci.org/fernandezcuesta/pySMSCMon)

- Download OpenVMS statistics (T4) over sftp through optional SSH tunnels over
a single gateway.
- T4-Format (flavours 1 and 2) CSV to Pandas dataframe. Merge all remote CSVs
into a single, plain (not T4) CSV and a gzipped pickle file containing the
dataframe and its associated metadata.
- Apply simple arithmetical operations to the dataframe.
- HTML (Jinja2 templates) report generation by graphing dataframe columns.


## Build for windows

    $ nuitka __init__.py --recurse-to=pysmscmon --recurse-to=sftpsession --recurse-to=sshtunnel --recurse-to=calculations