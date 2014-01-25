import plpy
import presto_client
from collections import namedtuple
import time

# convert Presto query result type to PostgreSQL type
def _pg_result_type(presto_type):
    if presto_type == "varchar":
        return "text"
    elif presto_type == "bigint":
        return "bigint"
    elif presto_type == "boolean":
        return "boolean"
    elif presto_type == "double":
        return "double precision"
    else:
        raise Exception, "unknown result column type: " + plpy.quote_ident(presto_type)

# convert Presto type to PostgreSQL type
def _pg_table_type(presto_type):
    if presto_type == "varchar":
        return "varchar(100)"
    elif presto_type == "bigint":
        return "bigint"
    elif presto_type == "boolean":
        return "boolean"
    elif presto_type == "double":
        return "double precision"
    else:
        raise Exception("unknown table column type: " + plpy.quote_ident(presto_type))

# build CREATE TEMPORARY TABLE statement
def _build_create_temp_table_sql(table_name, column_names, column_types, not_nulls=None):
    create_sql = "create temporary table %s (\n  " % plpy.quote_ident(table_name)

    first = True
    for column_name, column_type in zip(column_names, column_types):
        if first:
            first = False
        else:
            create_sql += ",\n  "

        create_sql += plpy.quote_ident(column_name)
        create_sql += " "
        create_sql += column_type

        # TODO not null
        #if not column.nullable:
        #    create_sql += " not null"

    create_sql += "\n)"
    return create_sql

# build INSERT INTO statement and string format to build VALUES (..), ...
def _build_insert_into_sql(table_name, column_names):
    insert_sql = "insert into %s (\n  " % plpy.quote_ident(table_name)

    first = True
    for column_name in column_names:
        if first:
            first = False
        else:
            insert_sql += ",\n  "

        insert_sql += plpy.quote_ident(column_name)

    insert_sql += "\n) values\n"

    values_sql_format = "(%s)" % (", ".join(["${}"] * len(column_names)))

    return (insert_sql, values_sql_format)

# create a prepared statement for batch INSERT
def _plan_batch(insert_sql, values_sql_format, column_types, batch_size):
    # format string 'values ($1, $2), ($3, $4) ...'
    values_sql = (", ".join([values_sql_format] * batch_size)).format(*range(1, batch_size * len(column_types) + 1))
    batch_insert_sql = insert_sql + values_sql
    return plpy.prepare(batch_insert_sql, column_types * batch_size)

# run batch INSERT
def _batch_insert(insert_sql, values_sql_format, batch_size, column_types, rows):
    full_batch_plan = None

    batch = []
    for row in rows:
        batch.append(row)
        batch_len = len(batch)
        if batch_len >= batch_size:
            if full_batch_plan is None:
                full_batch_plan = _plan_batch(insert_sql, values_sql_format, column_types, batch_len)
            plpy.execute(full_batch_plan, [item for sublist in batch for item in sublist])
            del batch[:]

    if batch:
        plan = _plan_batch(insert_sql, values_sql_format, column_types, len(batch))
        plpy.execute(plan, [item for sublist in batch for item in sublist])

class SchemaCache(object):
    def __init__(self):
        self.server = None
        self.user = None
        self.catalog = None
        self.schema_names = None
        self.statements = None
        self.expire_time = None

    def is_cached(self, server, user, catalog, current_time):
        return self.server == server and self.user == user and self.catalog == catalog \
               and self.statements is not None and current_time < self.expire_time

    def set_cache(self, server, user, catalog, schema_names, statements, expire_time):
        self.server = server
        self.user = user
        self.catalog = catalog
        self.schema_names = schema_names
        self.statements = statements
        self.expire_time = expire_time

OidToTypeNameMapping = {}

def _load_oid_to_type_name_mapping(oids):
    oids = filter(lambda oid: oid not in OidToTypeNameMapping, oids)
    if oids:
        sql = ("select oid, typname" \
               " from pg_catalog.pg_type" \
               " where oid in (%s)") % (", ".join(map(str, oids)))
        for row in plpy.execute(sql):
            OidToTypeNameMapping[int(row["oid"])] = row["typname"]

    return OidToTypeNameMapping

Column = namedtuple("Column", ("name", "type", "nullable"))

SchemaCacheEntry = SchemaCache()

def run_presto_as_temp_table(server, user, catalog, schema, result_table, query):
    try:
        client = presto_client.Client(server=server, user=user, catalog=catalog, schema=schema)

        create_sql = "create temporary table %s (\n  " % plpy.quote_ident(result_table)
        insert_sql = "insert into %s (\n  " % plpy.quote_ident(result_table)
        values_types = []

        q = client.query(query)
        try:
            # result schema
            column_names = []
            column_types = []
            for column in q.columns():
                column_names.append(column.name)
                column_types.append(_pg_result_type(column.type))

            # build SQL
            create_sql = _build_create_temp_table_sql(result_table, column_names, column_types)
            insert_sql, values_sql_format = _build_insert_into_sql(result_table, column_names)

            # run CREATE TABLE
            plpy.execute("drop table if exists " + plpy.quote_ident(result_table))
            plpy.execute(create_sql)

            # run INSERT
            _batch_insert(insert_sql, values_sql_format, 10, column_types, q.results())
        finally:
            q.close()

    except (plpy.SPIError, presto_client.PrestoException) as e:
        # PL/Python converts an exception object in Python to an error message in PostgreSQL
        # using exception class name if exc.__module__ is either of "builtins", "exceptions",
        # or "__main__". Otherwise using "module.name" format. Set __module__ = "__module__"
        # to generate pretty messages.
        e.__class__.__module__ = "__main__"
        raise

def run_system_catalog_as_temp_table(server, user, catalog, result_table, query):
    try:
        client = presto_client.Client(server=server, user=user, catalog=catalog, schema="default")

        # create SQL statements which put data to system catalogs
        if SchemaCacheEntry.is_cached(server, user, catalog, time.time()):
            schema_names = SchemaCacheEntry.schema_names
            statements = SchemaCacheEntry.statements

        else:
            # get table list
            sql = "select table_schema, table_name, column_name, is_nullable, data_type" \
                  " from information_schema.columns"
            columns, rows = client.run(sql)

            schemas = {}

            if rows is None:
                rows = []

            for row in rows:
                schema_name = row[0]
                table_name = row[1]
                column_name = row[2]
                is_nullable = row[3]
                column_type = row[4]

                tables = schemas.setdefault(schema_name, {})
                columns = tables.setdefault(table_name, [])
                columns.append(Column(column_name, column_type, is_nullable))

            # generate SQL statements
            statements = []
            schema_names = []

            for schema_name, tables in schemas.items():
                if schema_name == "sys" or schema_name == "information_schema":
                    # skip system schemas
                    continue

                schema_names.append(schema_name)

                for table_name, columns in tables.items():
                    # table schema
                    column_names = []
                    column_types = []
                    not_nulls = []
                    for column in columns:
                        column_names.append(column.name)
                        column_types.append(_pg_table_type(column.type))
                        not_nulls.append(not column.nullable)

                    create_sql = _build_create_temp_table_sql(table_name, column_names, column_types, not_nulls)
                    statements.append(create_sql)

            # cache expires after 10 seconds
            SchemaCacheEntry.set_cache(server, user, catalog, schema_names, statements, time.time() + 10)

        # enter subtransaction to rollback tables right after running the query
        subxact = plpy.subtransaction()
        subxact.enter()
        try:
            # delete all schemas excepting prestogres_catalog
            sql = "select n.nspname as schema_name from pg_catalog.pg_namespace n" \
                  " where n.nspname not in ('prestogres_catalog', 'pg_catalog', 'information_schema', 'public')" \
                  " and n.nspname !~ '^pg_toast'"
            for row in plpy.cursor(sql):
                plpy.execute("drop schema %s cascade" % plpy.quote_ident(row["schema_name"]))

            # delete all tables in prestogres_catalog
            # relkind = 'r' takes only tables and skip views, indexes, etc.
            sql = "select n.nspname as schema_name, c.relname as table_name from pg_catalog.pg_class c" \
                  " left join pg_catalog.pg_namespace n on n.oid = c.relnamespace" \
                  " where c.relkind in ('r')" \
                  " and n.nspname in ('prestogres_catalog')" \
                  " and n.nspname !~ '^pg_toast'"
            for row in plpy.cursor(sql):
                plpy.execute("drop table %s.%s" % (plpy.quote_ident(row["schema_name"]), plpy.quote_ident(row["table_name"])))

            # create schemas
            for schema_name in schema_names:
                try:
                    plpy.execute("create schema %s" % plpy.quote_ident(schema_name))
                except:
                    # ignore error
                    pass

            # create tables
            for statement in statements:
                plpy.execute(statement)

            # run the actual query
            metadata = plpy.execute(query)
            result = map(lambda row: row.values(), metadata)

        finally:
            # rollback subtransaction
            subxact.exit("rollback subtransaction", None, None)

        # table schema
        oid_to_type_name = _load_oid_to_type_name_mapping(metadata.coltypes())
        column_names = metadata.colnames()
        column_types = map(oid_to_type_name.get, metadata.coltypes())

        create_sql = _build_create_temp_table_sql(result_table, column_names, column_types)
        insert_sql, values_sql_format = _build_insert_into_sql(result_table, column_names)

        # run CREATE TABLE and INSERT
        plpy.execute("drop table if exists " + plpy.quote_ident(result_table))
        plpy.execute(create_sql)
        _batch_insert(insert_sql, values_sql_format, 10, column_types, result)

    except (plpy.SPIError, presto_client.PrestoException) as e:
        # Set __module__ = "__module__" to generate pretty messages.
        e.__class__.__module__ = "__main__"
        raise

