import os
from django.core.wsgi import get_wsgi_application
__author__ = 'Charan'

"""
Code to sample data from datasets
"""

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "rowvsdocstore.settings")

application = get_wsgi_application()
