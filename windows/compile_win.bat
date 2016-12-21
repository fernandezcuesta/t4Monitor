conda install tk
pip install -r ..\requirements\requirements-wincompile.txt
pyinstaller --clean ..\t4mon.spec --log-level DEBUG
