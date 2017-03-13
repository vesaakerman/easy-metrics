from __future__ import absolute_import

from parthenos.settings.defaults import *  # noqa

try:
    from parthenos.settings.local import *  # noqa
except ImportError as error:
    pass
