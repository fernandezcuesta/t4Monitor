- Test functions for all submodules

- Make reports from local stored data (PKL, CSV <s>loses metadata</s>)

- [SYSTEM] should not depend on T4-CSV header, system should be selected
according to `container.data.system` instead of
`[x for x in conf.sections() if x not in ['GATEWAY', 'MISC']` in `html.py`.

- GUI
