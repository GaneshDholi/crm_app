__version__ = "0.0.1"
import sys

# Fix for double module path issue
sys.modules["crm_app.crm_app"] = sys.modules[__name__]
