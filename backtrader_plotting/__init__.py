# Fix numpy compatibility issues with newer versions
import numpy as np
if not hasattr(np, 'bool8'):
    np.bool8 = np.bool_
if not hasattr(np, 'int0'):
    np.int0 = np.int_
if not hasattr(np, 'float_'):
    np.float_ = np.float64

from backtrader_plotting.bokeh.bokeh import Bokeh

from backtrader_plotting.bokeh.optbrowser import OptBrowser

# initialize analyzer tables
from backtrader_plotting.analyzer_tables import inject_datatables
inject_datatables()
