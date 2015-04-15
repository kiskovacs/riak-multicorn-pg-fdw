# Very simple setup for the module that allows us to create a foreign data wrapper in PostgreSQL to Riak.
# Based on the Multicorn and the RethinkDB FDW.
##
from distutils.core import setup

setup(
    name='riak_fdw',
    version='0.1',
    author='Stefan Kiskovac',
    author_email='stefan.kiskovac@gmail.com',
    license='Postgresql',
    packages=['riak_fdw'],
    url='https://github.com/kiskovacs/riak-multicorn-pg-fdw',
    requires=['riak']
)

