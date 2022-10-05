# -*- mode: python ; coding: utf-8 -*-
from PyInstaller.utils.hooks import collect_submodules

hiddenimports = []
hiddenimports += collect_submodules('pockets')


block_cipher = None


a = Analysis(
    ['src/gpas/cli.py'],
    pathex=[],
    binaries=[],
    datas=[('src/gpas/data', 'data')],
    hiddenimports=hiddenimports,
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
    noarchive=False,
)

to_rem = ["lib.cpython-310-darwin.so", "lib.cpython-310-x86_64-linux-gnu.so"]

for val in to_rem:
    for b in a.binaries:
          nb = b[0]
          if str(nb).endswith(val):
                print("removed  " + b[0])
                a.binaries.remove(b)

pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.zipfiles,
    a.datas,
    [],
    name='gpas',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=True,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)
