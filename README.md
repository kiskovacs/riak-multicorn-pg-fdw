# riak-multicorn-pg-fdw

Multicorn based PostgreSQL Foreign Data Wrapper for Riak

Debian dependecies install:
```sh
$ sudo apt-get install python-dev libffi-dev libssl-dev
```

Red Hat dependencies install:
```sh
$ dnf install python-devel libffi-devel libss-devel protobuf-python
```

Installation:
```sh
$ sudo pip install riak

$ sudo pgxn install multicorn

$ cd riak-multicorn-pg-fdw

$ sudo python setup.py install
```
```sql
create extension multicorn;

create server riak foreign data wrapper multicorn options (wrapper 'riak_fdw.riak_fdw.RiakFDW');

create foreign table riak_medium_bucket (id varchar, data varchar) server riak options (bucket 'medium', nodes 'http://127.0.0.1:8098,pbc://127.0.0.1:8087');

insert into riak_medium_bucket values ('first','first data');

select * from riak_medium_bucket;
```
