# Django settings for faq project.
import os
import sys

DEBUG = (os.getenv('CC_BILLING_PRODUCTION', '') != 'true')
TEMPLATE_DEBUG = DEBUG

LOCAL_DEV = os.name == 'nt'

PROJECT_ROOT = os.path.dirname(__file__)
PROJECT_DIR =  os.path.abspath(PROJECT_ROOT)

ADMINS = (
    ('David Hain', 'dhain@spideroak.com'),
    ('Bryon Roche', 'bryon@spideroak.com'),
    ('Chip', 'chip@spideroak.com'),
    ('Ben Zimmerman', 'benz@spideroak.com'),
    ('Alan Fairless', 'alan@spideroak.com'),
    ('Benny', 'benny@spideroak.com'),
)
DEFAULT_FROM_EMAIL = SERVER_EMAIL = 'pandora+fcgi.main@spideroak.com'

MANAGERS = ADMINS

CACHE_BACKEND = 'memcached://%s/' % os.environ['MEMCACHED_HOST']

DATABASE_ENGINE = 'postgresql_psycopg2'           # 'postgresql_psycopg2', 'postgresql', 'mysql', 'sqlite3' or 'oracle'.
DATABASE_NAME = 'pandora'             # Or path to database file if using sqlite3.
DATABASE_USER = 'nimbus'             # Not used with sqlite3.
DATABASE_PASSWORD = os.environ['PANDORA_DB_PW_nimbus']         # Not used with sqlite3.
DATABASE_HOST = os.getenv('PANDORA_DATABASE_HOST', '')           # Set to empty string for localhost. Not used with sqlite3.
DATABASE_PORT = ''             # Set to empty string for default. Not used with sqlite3.

# Local time zone for this installation. Choices can be found here:
# http://en.wikipedia.org/wiki/List_of_tz_zones_by_name
# although not all choices may be available on all operating systems.
# On Unix systems, a value of None will cause Django to use the same
# timezone as the operating system.
# If running in a Windows environment this must be set to the same as your
# system time zone.
TIME_ZONE = 'America/Chicago'

# Language code for this installation. All choices can be found here:
# http://www.i18nguy.com/unicode/language-identifiers.html
LANGUAGE_CODE = 'en-us'

SITE_ID = 1

# If you set this to False, Django will make some optimizations so as not
# to load the internationalization machinery.
USE_I18N = True

# Absolute path to the directory that holds media.
# Example: "/home/media/media.lawrence.com/"
MEDIA_ROOT = os.path.join(PROJECT_DIR, '../static/')

# URL that handles the media served from MEDIA_ROOT. Make sure to use a
# trailing slash if there is a path component (optional in other cases).
# Examples: "http://media.lawrence.com", "http://example.com/media/"
MEDIA_URL = '/static/'

# URL prefix for admin media -- CSS, JavaScript and images. Make sure to use a
# trailing slash.
# Examples: "http://foo.com/media/", "/media/".
ADMIN_MEDIA_PREFIX = '/static/django_admin/media/'
ADMIN_MEDIA_ROOT = os.path.join(PROJECT_DIR, '../../webpy/spideroak.com/static/django_admin/media')

# Make this unique, and don't share it with anybody.
SECRET_KEY = '%7nz^t9g%_@i-s7rb-cek%00g2c9r=9t+j2$ybgw4st_xg2@v='

# List of callables that know how to import templates from various sources.
TEMPLATE_LOADERS = (
    'django.template.loaders.filesystem.load_template_source',
    'django.template.loaders.app_directories.load_template_source',
#     'django.template.loaders.eggs.load_template_source',
)

MIDDLEWARE_CLASSES = (
    'django.middleware.common.CommonMiddleware',
    'django.contrib.sessions.middleware.SessionMiddleware',
    'django.contrib.auth.middleware.AuthenticationMiddleware',
)

ROOT_URLCONF = 'urls'

TEMPLATE_DIRS = (
    '%s/templates' % PROJECT_DIR,
)

INSTALLED_APPS = (
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'django.contrib.sessions',
    'django.contrib.sites',
    'django.contrib.admin',
    'nimubs.io.main',
)

