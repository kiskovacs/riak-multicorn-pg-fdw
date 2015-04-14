# riak-multicorn-pg-fdw

Multicorn based PostgreSQL Foreign Data Wrapper for Riak

```sh
$ sudo apt-get install python-dev libffi-dev libssl-dev

$ sudo pip install riak

$ sudo pgxn install multicorn

$ cd riak-multicorn-pg-fdw

$ sudo python setup.py install
```
```sql
create extension multicorn

create server riak foreign data wrapper multicorn options (wrapper 'riak_fdw.riak_fdw.RiakFDW');

create foreign table riak_medium_bucket (
    id uuid
) server riak options (bucket 'medium');
```
