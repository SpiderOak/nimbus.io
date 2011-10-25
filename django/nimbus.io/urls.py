from django.conf.urls.defaults import *
admin.autodiscover()

urlpatterns = patterns('',
    (r'^webadmin/', include(admin.site.urls)),
    (r'', include('nimbus.io.main.urls')),
)

