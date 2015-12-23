- <s>Fix threading bug under windows with the plots</s>

- Move from threading to multiprocessing **[Done for report renderer]**

- Let `sshtunnel` (which will rely on `paramiko`) choose the tunnel ports
instead of `collector` giving a random port, which **may be in use**.

- <s>Test functions for all submodules</s>

- Functional tests should use mocks (rely on `sshtunnel`'s)

- <s>Make reports from local stored data (PKL, CSV loses metadata)</s>

- Create ipynb for local data reporting

- <s>[SYSTEM] should not depend on T4-CSV header, system should be selected
according to `container.data.system` instead of
`[x for x in conf.sections() if x not in ['GATEWAY', 'MISC']` in `html.py`.<s>

- <s>Complete refactor of __init__</s>

- GUI (kivy?)

- CLI: move from argparse+tqdm to Click (argument parsing and progressbar)

- python3 compatibility

- move from setuptools to distutils

- minimize .EXE size for windows builds (*hint: matplotlib*)

- Fix help displayed when running a secondary mode, i.e. --local --help