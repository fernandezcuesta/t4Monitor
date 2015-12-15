conda create -n t4Monitor --file requirements-conda.txt -y
activate t4Monitor
pip install -r requirements-common.txt
pip install --no-index --find-links=local pycairo