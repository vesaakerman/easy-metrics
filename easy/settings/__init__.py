from __future__ import absolute_import

from easy.settings.defaults import *  # noqa

try:
    from easy.settings.local import *  # noqa
except ImportError as error:
    pass
