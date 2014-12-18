try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

setup (
    name='elasticsearch_eventlet',
    version='0',
    py_modules=['elasticsearch_eventlet'],
    install_requires=['simplejson', 'erequests'],
    description='elasticsearch library which uses eventlet primitives',
    author='William Pitcock',
    author_email='nenolod@dereferenced.org',
    url='http://kaniini.dereferenced.org/elasticsearch-eventlet/'
)
