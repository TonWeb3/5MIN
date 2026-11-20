try:
    from .rust_clob import *  # noqa: F401, F403
    from . import rust_clob as _native
except ImportError:
    # When running from the repo root (/app), this source directory shadows the
    # compiled `rust_clob` package installed in site-packages (e.g. inside Docker/Linux).
    # Load the installed compiled extension (.so / .pyd) directly from site-packages.
    import glob
    import importlib.util
    import os
    import site
    import sysconfig

    _native = None
    _search_roots = []
    for _p in (sysconfig.get_paths().get("platlib"), sysconfig.get_paths().get("purelib")):
        if _p and _p not in _search_roots:
            _search_roots.append(_p)
    try:
        for _p in site.getsitepackages():
            if _p and _p not in _search_roots:
                _search_roots.append(_p)
    except Exception:
        pass

    for _root in _search_roots:
        _patterns = (
            os.path.join(_root, "rust_clob", "rust_clob*.so"),
            os.path.join(_root, "rust_clob", "rust_clob*.pyd"),
            os.path.join(_root, "rust_clob*.so"),
            os.path.join(_root, "rust_clob*.pyd"),
        )
        for _pat in _patterns:
            _matches = glob.glob(_pat)
            if _matches:
                _spec = importlib.util.spec_from_file_location("rust_clob.rust_clob", _matches[0])
                if _spec and _spec.loader:
                    _mod = importlib.util.module_from_spec(_spec)
                    _spec.loader.exec_module(_mod)
                    _native = _mod
                    break
        if _native is not None:
            break

    if _native is None:
        raise

    _public = getattr(_native, "__all__", None) or [k for k in dir(_native) if not k.startswith("_")]
    for _k in _public:
        globals()[_k] = getattr(_native, _k)

__doc__ = getattr(_native, "__doc__", None)
if hasattr(_native, "__all__"):
    __all__ = _native.__all__
