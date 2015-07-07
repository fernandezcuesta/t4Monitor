# pySMSCMon

T4-Format2 CSV to Pandas dataframe

- Load files over sftp through optional SSH tunnels over a single gateway.
- Merges all remote CSVs into a single, plain CSV and a gzipped pickle file
containing the dataframe.
- Create report in HTML using Jinja2 templates.


## Build for windows

    $ nuitka __init__.py --recurse-to=pysmscmon --recurse-to=sftpsession --recurse-to=sshtunnel --recurse-to=calculations