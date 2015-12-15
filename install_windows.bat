pip install -r requirements-common.txt
pip install --no-index --find-links=local pycairo
python setup.py build
python setup.py install