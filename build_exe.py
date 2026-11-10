"""
MineRelay Standalone Windows Executable (.exe) Builder
Run `python build_exe.py` to compile a fresh single-file `dist/MineRelay.exe`
that can be shared with any friend/member to run MineRelay with zero Python setup.
"""

import shutil
import subprocess
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent
DIST_DIR = ROOT_DIR / "dist"
BUILD_DIR = ROOT_DIR / "build"


def build_executable() -> Path:
    print("============================================================")
    print("  Building Standalone Release: dist/MineRelay.exe")
    print("============================================================")

    if sys.platform.startswith("win"):
        try:
            import psutil  # type: ignore
            for p in psutil.process_iter(["pid", "name"]):
                try:
                    if (p.info.get("name") or "").lower() in ("minerelay.exe", "minerelay.old.exe"):
                        p.terminate()
                except Exception:
                    pass
        except Exception:
            pass

        existing_exe = DIST_DIR / "MineRelay.exe"
        old_exe = DIST_DIR / "MineRelay.old.exe"
        try:
            if old_exe.exists():
                old_exe.unlink(missing_ok=True)
        except OSError:
            pass
        if existing_exe.exists():
            try:
                existing_exe.unlink()
            except OSError:
                try:
                    existing_exe.replace(old_exe)
                except OSError:
                    pass

    entry_script = ROOT_DIR / "src" / "server.py"
    public_dir = ROOT_DIR / "public"
    icon_path = public_dir / "logo.ico"
    if not icon_path.exists():
        subprocess.run([sys.executable, str(ROOT_DIR / "scripts" / "generate_logo.py")], cwd=str(ROOT_DIR), check=True)

    add_data_sep = ";" if sys.platform.startswith("win") else ":"
    cmd = [
        sys.executable,
        "-m",
        "PyInstaller",
        "--noconfirm",
        "--clean",
        "--onefile",
        "--name",
        "MineRelay",
        "--icon",
        str(icon_path),
        "--add-data",
        f"{public_dir}{add_data_sep}public",
        "--paths",
        str(ROOT_DIR),
        "--hidden-import",
        "psutil",
        "--hidden-import",
        "src.models",
        "--hidden-import",
        "src.sync_engine",
        "--hidden-import",
        "src.auth_service",
        "--hidden-import",
        "src.tunnel_manager",
        "--hidden-import",
        "src.minecraft_manager",
        "--hidden-import",
        "src.bootstrap_runtime",
        str(entry_script),
    ]

    res = subprocess.run(cmd, cwd=str(ROOT_DIR))
    if res.returncode != 0:
        raise RuntimeError(f"PyInstaller build failed with exit code {res.returncode}")

    exe_path = DIST_DIR / ("MineRelay.exe" if sys.platform.startswith("win") else "MineRelay")
    if not exe_path.exists():
        raise FileNotFoundError(f"Expected output executable not found at {exe_path}")

    if sys.platform.startswith("win"):
        try:
            import ctypes
            SHCNE_UPDATEITEM = 0x00002000
            SHCNE_ASSOCCHANGED = 0x08000000
            SHCNF_IDLIST = 0x0000
            SHCNF_PATHW = 0x0005
            SHCNF_FLUSH = 0x1000
            ctypes.windll.shell32.SHChangeNotify(SHCNE_UPDATEITEM, SHCNF_PATHW | SHCNF_FLUSH, ctypes.c_wchar_p(str(exe_path)), None)
            ctypes.windll.shell32.SHChangeNotify(SHCNE_ASSOCCHANGED, SHCNF_IDLIST | SHCNF_FLUSH, None, None)
            subprocess.run(["ie4uinit.exe", "-show"], check=False)
            subprocess.run(["ie4uinit.exe", "-ClearIconCache"], check=False)
        except Exception:
            pass

    size_mb = round(exe_path.stat().st_size / (1024 * 1024), 2)
    print("============================================================")
    print(f"  SUCCESS! Standalone executable created:")
    print(f"  Path : {exe_path}")
    print(f"  Size : {size_mb} MB")
    print("============================================================")
    return exe_path


if __name__ == "__main__":
    build_executable()
