conda create -n t4Monitor --file requirements/requirements-conda.txt -y
call activate t4Monitor && ^
pip install -r requirements/requirements-common.txt && ^
python setup.py develop
