from django.conf.urls.defaults import *
from nimbus.io.main.views import *

urlpatterns = patterns('',
    (r'^$', index),
)
