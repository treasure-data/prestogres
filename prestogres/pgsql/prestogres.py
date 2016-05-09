import plpy
import presto_client
from collections import namedtuple
from copy import copy
import time
import json
import re

# Maximum length for identifiers (e.g. table names, column names, function names)
# defined in pg_config_manual.h
PG_NAMEDATALEN = 64

JSON_TYPE_PATTERN = re.compile("^(?:row|array|map)(?![a-zA-Z])", re.IGNORECASE)

# See the document about system column names: http://www.postgresql.org/docs/9.3/static/ddl-system-columns.html
SYSTEM_COLUMN_NAMES = set(["oid", "tableoid", "xmin", "cmin", "xmax", "cmax", "ctid"])

# convert Presto query result field types to PostgreSQL types
def _pg_result_type(presto_type):
    if presto_type == "varchar":  # for old Presto
        return "varchar(255)"
    elif presto_type == "varbinary":
        return "bytea"
    elif presto_type == "double":
        return "double precision"
    elif JSON_TYPE_PATTERN.match(presto_type):
        return "json"  # TODO record or anyarray???
    else:
        # assuming Presto and PostgreSQL use the same SQL standard name
        return presto_type

# convert Presto table column types to PostgreSQL types
def _pg_table_type(presto_type):
    if presto_type == "varchar":  # for old Presto
        return "varchar(255)"
    elif presto_type == "varbinary":
        return "bytea"
    elif presto_type == "double":
        return "double precision"
    elif JSON_TYPE_PATTERN.match(presto_type):
        return "json"  # TODO record or anyarray???
    else:
        # assuming Presto and PostgreSQL use the same SQL standard name
        return presto_type

# queries can include same column name twice but tables can't.
def _rename_duplicated_column_names(column_names, where):
    renamed = []
    used_names = copy(SYSTEM_COLUMN_NAMES)
    for original_name in column_names:
        name = original_name
        while name in used_names:
            name += "_"
        if name != original_name:
            if name in SYSTEM_COLUMN_NAMES:
                plpy.warning("Column %s is renamed to %s because the name in %s conflicts with PostgreSQL system column names" % \
                        (plpy.quote_ident(original_name), plpy.quote_ident(name), where))
            else:
                plpy.warning("Column %s is renamed to %s because the name appears twice in %s" % \
                        (plpy.quote_ident(original_name), plpy.quote_ident(name), where))
        used_names.add(name)
        renamed.append(name)
    return renamed

# build CREATE TEMPORARY TABLE statement
def _build_create_temp_table_sql(table_name, column_names, column_types):
    create_sql = ["create temporary table %s (\n  " % plpy.quote_ident(table_name)]

    first = True
    for column_name, column_type in zip(column_names, column_types):
        if first:
            first = False
        else:
            create_sql.append(",\n  ")

        create_sql.append(plpy.quote_ident(column_name))
        create_sql.append(" ")
        create_sql.append(column_type)

    create_sql.append("\n)")
    return ''.join(create_sql)

# build CREATE TABLE statement
def _build_create_table(schema_name, table_name, column_names, column_types, not_nulls):
    alter_sql = ["create table %s.%s (\n  " % (plpy.quote_ident(schema_name), plpy.quote_ident(table_name))]

    first = True
    for column_name, column_type, not_null in zip(column_names, column_types, not_nulls):
        if first:
            first = False
        else:
            alter_sql.append(",\n  ")

        alter_sql.append("%s %s" % (plpy.quote_ident(column_name), column_type))

        if not_null:
            alter_sql.append(" not null")

    alter_sql.append("\n)")
    return ''.join(alter_sql)

def _get_session_time_zone():
    rows = plpy.execute("show timezone")
    return rows[0].values()[0]

def _get_session_search_path_array():
    rows = plpy.execute("select ('{' || current_setting('search_path') || '}')::text[]")
    return rows[0].values()[0]

NULL_PATTERN = dict({'\0':None})

def remove_null(bs):
    if isinstance(bs, str):
        return bs.translate(None, '\0')
    elif isinstance(bs, unicode):
        return bs.translate(NULL_PATTERN)
    else:
        return bs

class QueryAutoClose(object):
    def __init__(self, query):
        self.query = query
        self.column_names = None
        self.column_types = None

    def __del__(self):
        self.query.close()

class QueryAutoCloseIterator(object):
    def __init__(self, gen, query_auto_close):
        self.gen = gen
        self.query_auto_close = query_auto_close

    def __iter__(self):
        return self

    def next(self):
        row = next(self.gen)
        for i, v in enumerate(row):
            row[i] = remove_null(v)
        return row

class QueryAutoCloseIteratorWithJsonConvert(QueryAutoCloseIterator):
    def __init__(self, gen, query_auto_close, json_columns):
        QueryAutoCloseIterator.__init__(self, gen, query_auto_close)
        self.json_columns = json_columns

    def next(self):
        row = next(self.gen)
        for i, v in enumerate(row):
            row[i] = remove_null(v)
        for i in self.json_columns:
            row[i] = json.dumps(row[i])
        return row

class SessionData(object):
    def __init__(self):
        self.query_auto_close = None
        self.variables = {}

session = SessionData()

def start_presto_query(presto_server, presto_user, presto_catalog, presto_schema, function_name, original_query):
    try:
        # preserve search_path if explicitly set
        search_path = _get_session_search_path_array()
        if search_path != ['$user', 'public'] and len(search_path) > 0:
            # search_path is changed explicitly. use the first schema
            presto_schema = search_path[0]

        # start query
        client = presto_client.Client(server=presto_server, user=presto_user, catalog=presto_catalog, schema=presto_schema, time_zone=_get_session_time_zone(), session=session.variables)
        if 'set session show' in original_query:
            original_query = 'show session'


        query = client.query(original_query)
        session.query_auto_close = QueryAutoClose(query)
        try:
            # result schema
            column_names = []
            column_types = []
            for column in query.columns():
                column_names.append(column.name)
                column_types.append(_pg_result_type(column.type))

            column_names = _rename_duplicated_column_names(column_names, "a query result")
            session.query_auto_close.column_names = column_names
            session.query_auto_close.column_types = column_types

            # CREATE TABLE for return type of the function
            type_name = function_name + "_type"
            create_type_sql = _build_create_temp_table_sql(type_name, column_names, column_types)

            # CREATE FUNCTION
            create_function_sql = \
                """
                create or replace function pg_temp.%s()
                returns setof pg_temp.%s as $$
                    import prestogres
                    return prestogres.fetch_presto_query_results()
                $$ language plpythonu
                """ % \
                (plpy.quote_ident(function_name), plpy.quote_ident(type_name))

            drop_sql = "drop table if exists pg_temp.%s cascade" % \
                   (plpy.quote_ident(type_name))
            # run statements
            plpy.execute(drop_sql)
            plpy.execute(create_type_sql)
            plpy.execute(create_function_sql)

            match = re.search('^\s*set\s+session\s+([a-z_\.]+)\s*=\s*(.+)$', original_query, re.IGNORECASE)
            if match:
                field = match.group(1)
                value = match.group(2)
                if value[0] == value[-1] and value.startswith(('"', "'")):
                    value = value[1:-1]
                session.variables[field] = value
            query = None

        finally:
            if query is not None:
                # close query
                session.query_auto_close = None

    except (plpy.SPIError, presto_client.PrestoException) as e:
        # PL/Python converts an exception object in Python to an error message in PostgreSQL
        # using exception class name if exc.__module__ is either of "builtins", "exceptions",
        # or "__main__". Otherwise using "module.name" format. Set __module__ = "__module__"
        # to generate pretty messages.
        e.__class__.__module__ = "__main__"
        raise

def fetch_presto_query_results():
    try:
        # TODO should throw an exception?
        #if session.query_auto_close is None:

        query_auto_close = session.query_auto_close
        session.query_auto_close = None  # close of the iterator closes query

        results = query_auto_close.query.results()
        json_columns = []
        for i, t in enumerate(query_auto_close.column_types):
            if t == "json":
                json_columns.append(i)

        if json_columns:
            return QueryAutoCloseIteratorWithJsonConvert(results, query_auto_close, json_columns)
        else:
            return QueryAutoCloseIterator(results, query_auto_close)

    except (plpy.SPIError, presto_client.PrestoException) as e:
        e.__class__.__module__ = "__main__"
        raise

Column = namedtuple("Column", ("name", "type", "nullable"))

def setup_system_catalog(presto_server, presto_user, presto_catalog, presto_schema, access_role):
    search_path = _get_session_search_path_array()
    if search_path == ['$user', 'public']:
        # search_path is default value.
        plpy.execute("set search_path to %s" % plpy.quote_ident(presto_schema))

    client = presto_client.Client(server=presto_server, user=presto_user, catalog=presto_catalog, schema='default')

    # get table list
    sql = "select table_schema, table_name, column_name, is_nullable, data_type" \
          " from information_schema.columns"
    columns, rows = client.run(sql)
    if rows is None:
        rows = []

    schemas = {}

    for row in rows:
        schema_name = row[0]
        table_name = row[1]
        column_name = row[2]
        is_nullable = row[3]
        column_type = row[4]

        if schema_name == "sys" or schema_name == "information_schema":
            # skip system schemas
            continue

        if len(schema_name) > PG_NAMEDATALEN - 1:
            plpy.warning("Schema %s is skipped because its name is longer than %d characters" % \
                    (plpy.quote_ident(schema_name), PG_NAMEDATALEN - 1))
            continue

        tables = schemas.setdefault(schema_name, {})

        if len(table_name) > PG_NAMEDATALEN - 1:
            plpy.warning("Table %s.%s is skipped because its name is longer than %d characters" % \
                    (plpy.quote_ident(schema_name), plpy.quote_ident(table_name), PG_NAMEDATALEN - 1))
            continue

        columns = tables.setdefault(table_name, [])

        if len(column_name) > PG_NAMEDATALEN - 1:
            plpy.warning("Column %s.%s.%s is skipped because its name is longer than %d characters" % \
                    (plpy.quote_ident(schema_name), plpy.quote_ident(table_name), \
                     plpy.quote_ident(column_name), PG_NAMEDATALEN - 1))
            continue

        columns.append(Column(column_name, column_type, is_nullable))

    # drop all schemas excepting prestogres_catalog, information_schema and pg_%
    sql = "select n.nspname as schema_name from pg_catalog.pg_namespace n" \
          " where n.nspname not in ('prestogres_catalog', 'information_schema')" \
          " and n.nspname not like 'pg_%'"
    for row in plpy.cursor(sql):
        plpy.execute("drop schema %s cascade" % plpy.quote_ident(row["schema_name"]))

    # create schema and tables
    for schema_name, tables in sorted(schemas.items(), key=lambda (k,v): k):
        try:
            plpy.execute("create schema %s" % (plpy.quote_ident(schema_name)))
        except:
            # ignore error?
            pass

        for table_name, columns in sorted(tables.items(), key=lambda (k,v): k):
            column_names = []
            column_types = []
            not_nulls = []

            if len(columns) >= 1600:
                plpy.warning("Table %s.%s contains more than 1600 columns. Some columns will be inaccessible" % (plpy.quote_ident(schema_name), plpy.quote_ident(table_name)))

            for column in columns[0:1600]:
                column_names.append(column.name)
                column_types.append(_pg_table_type(column.type))
                not_nulls.append(not column.nullable)

            # change columns
            column_names = _rename_duplicated_column_names(column_names,
                    "%s.%s table" % (plpy.quote_ident(schema_name), plpy.quote_ident(table_name)))
            create_sql = _build_create_table(schema_name, table_name, column_names, column_types, not_nulls)
            plpy.execute(create_sql)

        # grant access on the schema to the restricted user so that
        # pg_table_is_visible(reloid) used by \d of psql command returns true
        plpy.execute("grant usage on schema %s to %s" % \
                (plpy.quote_ident(schema_name), plpy.quote_ident(access_role)))
        # this SELECT privilege is unnecessary because queries against those tables
        # won't run on PostgreSQL. causing an exception is good if Prestogres has
        # a bug sending a presto query to PostgreSQL without rewriting.
        # TODO however, it's granted for now because some BI tools might check
        #      has_table_privilege. the best solution is to grant privilege but
        #      actually selecting from those tables causes an exception.
        plpy.execute("grant select on all tables in schema %s to %s" % \
                (plpy.quote_ident(schema_name), plpy.quote_ident(access_role)))

    # fake current_database() to return Presto's catalog name to be compatible with some
    # applications that use db.schema.table syntax to identify a table
    if plpy.execute("select pg_catalog.current_database()")[0].values()[0] != presto_catalog:
        plpy.execute("delete from pg_catalog.pg_proc where proname='current_database'")
        plpy.execute("create function pg_catalog.current_database() returns name as $$begin return %s::name; end$$ language plpgsql stable strict" % \
                plpy.quote_literal(presto_catalog))

