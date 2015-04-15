# Implementation of the Multicorn ForeignDataWrapper class for Riak

from contextlib import closing
from PIL import Image
from StringIO import StringIO
from multicorn import ForeignDataWrapper
from multicorn.utils import log_to_postgres, ERROR, DEBUG
from operatorFunctions import get_operator_function, UnknownOperatorException
import riak as r


def apply_filters(line, filters):
    for func in filters:
        if not func(line):
            log_to_postgres('Line filtered out: %s' % line, DEBUG)
            return False
    return True

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
            log_to_postgres('Using Default port: 8087.')

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

        log_to_postgres('Query Columns:  %s' % columns, DEBUG)
        log_to_postgres('Query Filters:  %s' % quals, DEBUG)

        (client, bucket) = self.connect()
        filters = []
        for qual in quals:
            try:
                operator_function = get_operator_function(qual.operator)
            except UnknownOperatorException, e:
                log_to_postgres(e, ERROR)
            filters.append(lambda x: operator_function(x[qual.field_name], qual.value))

        results = []
        with closing(client.stream_keys(bucket)) as keys:
            for key_list in keys:
                line = {}
                for key in key_list:
                    line['id'] = key
                    line['data'] = bucket.get(key).data
                if apply_filters(line, filters):
                    results.append(line)

        return results

    # SQL INSERT:
    def insert(self, _value):

        log_to_postgres('Insert Request - new values:  %s' % _value, DEBUG)

        if 'id' not in _value:
            log_to_postgres('Insert request requires new_values ID (PK).  Missing From:  %s' % _value, ERROR)

        (client, bucket) = self.connect()

        if 'file' in _value:
            new_data = bucket.new_from_file(_value['id'], _value['file'])
        else:
            new_data = bucket.new(_value['id'], data=_value['data'])
        new_data.store()
        return

    # SQL UPDATE:
    def update(self, _key, _value):

        log_to_postgres('Update Request - key:  %s' % _key, DEBUG)
        log_to_postgres('Update Request - value:  %s' % _value, DEBUG)

        (client, bucket) = self.connect()

        updated_data = bucket.get(_key)
        updated_data.data = _value['data']
        updated_data.store()
        return

    # SQL DELETE
    def delete(self, _key):

        log_to_postgres('Delete Request - id:  %s' % _key, DEBUG)

        (client, bucket) = self.connect()

        try:
            bucket.delete(_key)
        except Exception, ex:
            log_to_postgres('Riak error:  %s' % ex, ERROR)

        return



