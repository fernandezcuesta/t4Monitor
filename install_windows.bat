conda create -n t4Monitor --file requirements-conda.txt -y
call activate t4Monitor && ^
pip install --no-index --find-links=local pycairo && ^
pip install -r requirements-common.txt && ^
python setup.py build && python setup.py install
