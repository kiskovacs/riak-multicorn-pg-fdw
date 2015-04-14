## This is the implementation of the Multicorn ForeignDataWrapper class for Riak

from collections import OrderedDict
import json

from multicorn import ForeignDataWrapper
from multicorn.utils import log_to_postgres, ERROR, WARNING, DEBUG

import riak as r

## The Foreign Data Wrapper Class:
class RiakFDW(ForeignDataWrapper):

    """
    Riak FDW for PostgreSQL
    """

    def __init__(self, options, columns):

        super(RiakFDW, self).__init__(options, columns)

        log_to_postgres('options:  %s' % options, DEBUG)
        log_to_postgres('columns:  %s' % columns, DEBUG)


        if options.has_key('port'):
            self.port = options['port']
        else:
            self.port = '8087'
            log_to_postgres('Using Default port: 8087.', WARNING)

        if options.has_key('bucket'):
            self.table = options['bucket']
        else:
            log_to_postgres('bucket parameter is required.', ERROR)


        self.columns = columns


    # SQL SELECT:
    def execute(self, quals, columns):

        log_to_postgres('Query Columns:  %s' % columns, DEBUG)
        log_to_postgres('Query Filters:  %s' % quals, DEBUG)

        return
 

    # SQL INSERT:
    def insert(self, new_values):

        log_to_postgres('Insert Request - new values:  %s' % new_values, DEBUG)

        return

    # SQL UPDATE:
    def update(self, old_values, new_values):

        log_to_postgres('Update Request - new values:  %s' % new_values, DEBUG)

        if not old_values.has_key('id'):

             log_to_postgres('Update request requires old_values ID (PK).  Missing From:  %s' % old_values, ERROR)

        return

    # SQL DELETE
    def delete(self, old_values):

        log_to_postgres('Delete Request - old values:  %s' % old_values, DEBUG)

        if not old_values.has_key('id'):

            log_to_postgres('Update request requires old_values ID (PK).  Missing From:  %s' % old_values, ERROR)

        # try to connect
        try:
            client = r.RiakClient(pb_port=self.port)
            bucket = client.bucket(self.bucket)

        except Exception, e:

            log_to_postgres('Connection Falure:  %s' % e, ERROR)


        try:

            fetched = bucket.get(old_values.id)
            fetched.delete()

        except Exception, e:

            log_to_postgres('Riak error:  %s' %e, ERROR)

        return



