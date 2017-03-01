t4Monitor
=========
*OpenVMS statistics (T4) collector and reporting tools*

t4Monitor is a module that allows easy collection, preprocessing and reporting
of generic `OpenVMS' T4 <http://h71000.www7.hp.com/openvms/products/t4/>`_
compliant counters stored in Format-1 or Format-2 Comma Separated Values (CSV)
files.

Its features include:

- Download OpenVMS statistics (T4) over sftp through optional SSH tunnels over
  a single gateway.
- simple methods for collecting T4-compliant CSV files
- process collected statistics based in simple arithmetic functions (addition,
  multiplication, division and difference) based in collected metrics or
  scalars
- easy-to-use API for graphing and reporting statistics (to HTML using Jinja2
  templates)
- handle compressed CSV files
- T4-Format (flavors 1 and 2) CSV to Pandas dataframe conversion.
  All remote CSVs are merged into a single dataframe
- conversion to/from plain CSV (i.e. excel compliant) format
- generic remote command output retrieval
- multiprocess/multi-thread methods for fast retrieval (in parallel for each
  system)
- detailed log output in two parallel streams, on-screen for customizable
  severity logs and rotating file for detailed logging information
- comprehensive test suite


.. note::
    T4 CSV files header may come in 2 different formats: ``Format#1`` and
    ``Format#2```

**Format 1**

The first four lines are header data::

    line0: Header information containing T4 revision and system information

    line1: Collection date   (optional line)

    line2: Start time        (optional line)

    line3: Parameter Heading (comma separated)

or

**Format 2** ::

    line0: Header information containing T4 revision and system information
    line1: <delim> START COLUMN HEADERS  <delim>  where <delim> is a triple `$`
    line2: parameter headings (comma separated)
    ...

    line 'n': <delim> END COLUMN HEADERS <delim>  where <delim> is a triple `$`

The remaining lines are the comma separated values.
The first column is the sample time.
Each line represents a sample, typically 60 seconds apart.

However T4 incorrectly places an extra raw line with the column averages
almost at the end of the file. That line will be considered as a closing
hash and contents followed by it (sometimes even more samples...) is ignored.


Requirements
------------

.. note::
    embedded in ``requirements/requirements.txt``

- Python 2.7 or later
- `Jinja2 <http://jinja.pocoo.org>`_
- `matplotlib <http://matplotlib.org/>`_
- `pandas <http://pandas.pydata.org/>`_
- `paramiko <http://www.paramiko.org/>`_
- `six <https://pypi.python.org/pypi/six>`_
- `tqdm <https://github.com/tqdm/tqdm)>`_
- `sshtunnel <https://github.com/pahaz/sshtunnel>`_
- `cairocffi <https://pythonhosted.org/cairocffi/>`_ (linux only)
- `Anaconda-Miniconda <https://www.continuum.io/why-anaconda>`_ (windows only)

How to install
--------------

For Linux::

    git clone https://github.com/fernandezcuesta/t4Monitor.git
    cd t4Monitor
    pip install -r requirements/requirements.txt
    python setup.py develop

For Windows::

    git clone https://github.com/fernandezcuesta/t4Monitor.git
    cd t4Monitor
    install_windows.bat

Testing the package
-------------------

.. |Test Status| image:: https://travis-ci.org/fernandezcuesta/t4Monitor.svg?branch=master
.. _Test Status: https://travis-ci.org/fernandezcuesta/t4Monitor

.. |Coverage Status| image:: https://coveralls.io/repos/fernandezcuesta/t4Monitor/badge.svg?branch=master&service=github
.. _Coverage Status: https://coveralls.io/github/fernandezcuesta/t4Monitor?branch=master

|Test Status|_ |Coverage Status|_

Requirements:
^^^^^^^^^^^^^
> Embedded in ``requirements-test.txt``.

- `tox <https://pypi.python.org/pypi/tox>`_
- `pytest <http://pytest.org/>`_
- `pytest-cov <https://pypi.python.org/pypi/pytest-cov>`_
- `pytest-xdist <https://pypi.python.org/pypi/pytest-xdist>`_
- `flake8 <https://pypi.python.org/pypi/flake8>`_
- `mock <https://pypi.python.org/pypi/mock>`_

To run all the unit and functional tests:

- unit tests only::

    pytest -n4 test/unit_tests

- functional tests only (requires a SSH server to be up and running on
  localhost)::

    pytest -n4 test/functional_tests

- all tests in all supported python versions (requires all major versions from
  python2.7 to python3.5)::

    tox


Building documentation
----------------------

Requires:

    - `sphinx <http://sphinx-doc.org/>`_
    - `sphinxcontrib-napoleon <https://pypi.python.org/pypi/sphinxcontrib-napoleon>`_

::

    > cd docs
    > make html


License information
-------------------

2014-2016 (c) J.M. Fernández - fernandez.cuesta@gmail.com

License: The MIT License (MIT) - see `LICENSE` file
