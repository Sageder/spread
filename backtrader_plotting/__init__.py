# Fix numpy compatibility issues with newer versions (numpy 2.0+)
import numpy as np

# Monkeypatch numpy to add back removed aliases
_NUMPY_COMPAT_ATTRS = {
    'bool8': np.bool_,
    'bool': np.bool_,
    'int0': np.intp,
    'float': np.float64,
    'complex': np.complex128,
    'object': np.object_,
    'str': np.str_,
    'int': np.int_,
}

_np_getattr_original = getattr(np, '__getattr__', None)

def _np_getattr(attr):
    if attr in _NUMPY_COMPAT_ATTRS:
        return _NUMPY_COMPAT_ATTRS[attr]
    if _np_getattr_original:
        return _np_getattr_original(attr)
    raise AttributeError(f"module 'numpy' has no attribute '{attr}'")

np.__getattr__ = _np_getattr

# Note: Bokeh 3.0+ compatibility is handled in individual modules
# Panel (widgets) -> TabPanel (models) import fallback

from backtrader_plotting.bokeh.bokeh import Bokeh

from backtrader_plotting.bokeh.optbrowser import OptBrowser

# initialize analyzer tables
from backtrader_plotting.analyzer_tables import inject_datatables
inject_datatables()
