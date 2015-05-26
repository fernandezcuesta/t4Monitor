- Test functions for all submodules

<s>- Make reports from local stored data (PKL, CSV loses metadata)</s> Create ipynb for local data reporting

- [SYSTEM] should not depend on T4-CSV header, system should be selected
according to `container.data.system` instead of
`[x for x in conf.sections() if x not in ['GATEWAY', 'MISC']` in `html.py`.

- GUI (kivy?)
