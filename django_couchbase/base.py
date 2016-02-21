from django.core.exceptions import ImproperlyConfigured
from django.db.backends.base.base import BaseDatabaseWrapper
from django.db.backends.base.client import BaseDatabaseClient
from django.db.backends.base.creation import BaseDatabaseCreation
from django.db.backends.base.features import BaseDatabaseFeatures
from django.db.backends.base.introspection import BaseDatabaseIntrospection, FieldInfo, TableInfo
from django.db.backends.base.operations import BaseDatabaseOperations
from django.db.backends.base.validation import BaseDatabaseValidation
from django.db.utils import DataError, OperationalError, IntegrityError, InternalError, ProgrammingError, NotSupportedError, DatabaseError, InterfaceError, Error

from couchbase.bucket import Bucket
from couchbase.exceptions import CouchbaseError
from couchbase.n1ql import N1QLQuery


class Database(object):
    DataError = DataError
    OperationalError = OperationalError
    IntegrityError = IntegrityError
    InternalError = InternalError
    ProgrammingError = ProgrammingError
    NotSupportedError = NotSupportedError
    DatabaseError = DatabaseError
    InterfaceError = InterfaceError
    Error = Error


class DatabaseFeatures(BaseDatabaseFeatures):
    supports_transactions = False


class DatabaseOperations(BaseDatabaseOperations):
    def quote_name(self, name):
        if name.startswith('`') and name.endswith('`'):
            return name  # Quoting once is enough
        return '`%s`' % name


class DatabaseCreation(BaseDatabaseCreation):
    pass


class DatabaseClient(BaseDatabaseClient):
    executable_name = '/opt/couchbase/bin/cbq'

    def runshell(self):
        args = [self.executable_name]
        subprocess.call(args)


class DatabaseIntrospection(BaseDatabaseIntrospection):
    def get_table_list(self, cursor):
        cursor.execute('SELECT DISTINCT name, using FROM system:indexes')
        return [TableInfo(row['name'], row['using']) for row in cursor.fetchall()]


class DatabaseWrapper(BaseDatabaseWrapper):
    vendor = 'couchbase'

    operators = {
        'exact': '= %s',
        'iexact': "LIKE %s",
        'contains': "LIKE %s",
        'icontains': "LIKE %s",
        'gt': '> %s',
        'gte': '>= %s',
        'lt': '< %s',
        'lte': '<= %s',
        'startswith': "LIKE %s",
        'endswith': "LIKE %s",
        'istartswith': "LIKE %s",
        'iendswith': "LIKE %s",
    }

    Database = Database

    def __init__(self, *args, **kwargs):
        super(DatabaseWrapper, self).__init__(*args, **kwargs)

        self.features = DatabaseFeatures(self)
        self.ops = DatabaseOperations(self)
        self.client = DatabaseClient(self)
        self.creation = DatabaseCreation(self)
        self.introspection = DatabaseIntrospection(self)
        self.validation = BaseDatabaseValidation(self)

    def get_connection_params(self):
        for key in ('NAME', 'HOST'):
            if not self.settings_dict[key]:
                from django.core.exceptions import ImproperlyConfigured
                raise ImproperlyConfigured(
                    "settings.DATABASES is improperly configured. "
                    "Please supply the %s value." % key)
                
        conn_string = 'couchbase://%s/%s' % (self.settings_dict['HOST'], self.settings_dict['NAME'])
        return conn_string

    def get_new_connection(self, conn_params):
        return Connection(conn_params)

    def init_connection_state(self):
        pass

    def create_cursor(self):
        return self.connection.cursor()

    def is_usable(self):
        return True

    def _set_autocommit(self, autocommit):
        pass


class Cursor(object):
    request = None

    def __init__(self, conn):
        self.connection = conn

    def execute(self, query, params=None):
        q = N1QLQuery(query, *params or [])

        self.request = self.connection.bucket.n1ql_query(q)

    def fetchall(self):
        return self.request

    def fetchone(self):
        return self.request.get_single_result()


class Connection(object):
    def __init__(self, connection_string):
        self.bucket = Bucket(connection_string)

    def cursor(self):
        return Cursor(self)

    def close(self):
        self.bucket._close()
