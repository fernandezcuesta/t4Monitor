# -*- mode: python -*-

block_cipher = None


a = Analysis(['t4mon.py'],
             pathex=['.'],
             binaries=None,
             datas=[('t4mon/conf/', 't4mon/conf')],
             hiddenimports=[],
             hookspath=[],
             runtime_hooks=[],
             excludes=[],
             win_no_prefer_redirects=False,
             win_private_assemblies=False,
             cipher=block_cipher)
pyz = PYZ(a.pure, a.zipped_data,
             cipher=block_cipher)
exe = EXE(pyz,
          a.scripts,
          exclude_binaries=True,
          name='t4mon',
          debug=False,
          strip=False,
          upx=True,
		  icon='local\\favicon.ico',
          console=True )
coll = COLLECT(exe,
               a.binaries,
               a.zipfiles,
               a.datas,
               strip=False,
               upx=True,
               name='t4mon')
