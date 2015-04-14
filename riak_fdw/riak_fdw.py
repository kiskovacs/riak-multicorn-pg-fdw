## This is the implementation of the Multicorn ForeignDataWrapper class for Riak

from contextlib import closing
from PIL import Image
from StringIO import StringIO
from multicorn import ForeignDataWrapper
from multicorn.utils import log_to_postgres, ERROR, WARNING, DEBUG
from operatorFunctions import unknownOperatorException, getOperatorFunction
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
            self.bucket = options['bucket']
        else:
            log_to_postgres('bucket parameter is required.', ERROR)


        self.columns = columns
        self.row_id_column = columns.keys()[0]

    @property
    def rowid_column(self):
        """
        Returns:
            A column name which will act as a rowid column,
            for delete/update operations.

            One can think of it as a primary key.

            This can be either an existing column name, or a made-up one.
            This column name should be subsequently present in every
            returned resultset.
        """
        return self.row_id_column

    def connect(self):
        # try to connect
        try:
            client = r.RiakClient(pb_port=self.port)
            bucket = client.bucket(self.bucket)
            client.set_decoder("image/jpeg", lambda data: Image.open(StringIO(data)))

        except Exception, e:

            log_to_postgres('Connection Falure:  %s' % e, ERROR)

        return client, bucket

    # SQL SELECT:
    def execute(self, quals, columns):

        log_to_postgres('Query Columns:  %s' % columns, WARNING)
        log_to_postgres('Query Filters:  %s' % quals, WARNING)

        (client, bucket) = self.connect()
        results = []
        with closing(client.stream_keys(bucket)) as keys:
            for key_list in keys:
                line = {}
                for key in key_list:
                    line['id'] = 'id %s' % key
                    line['data'] = 'data %s' % bucket.get(key).data
                results.append(line)

        for qual in quals:

            try:
                operatorFunction = getOperatorFunction(qual.operator)
            except unknownOperatorException, e:
                log_to_postgres(e, ERROR)

            filter(lambda line: operatorFunction(line[qual.field_name], qual.value), results)

        for line in results:
            yield line

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

        (client, bucket) = self.connect()


        try:

            bucket.delete(old_values.id)

        except Exception, e:

            log_to_postgres('Riak error:  %s' %e, ERROR)

        return



