pip install --no-index --find-links=local pycairo
pip install -r requirements-common.txt
python setup.py build
python setup.py install