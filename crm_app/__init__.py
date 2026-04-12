import sys
from .__version__ import __version__

# Fix for double module path issue
sys.modules["crm_app.crm_app"] = sys.modules[__name__]
