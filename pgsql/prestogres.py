from presto_client import *
import plpy
from collections import namedtuple

def run_presto_as_temp_table(server, user, catalog, schema, table_name, query):
    session = ClientSession(server=server, user=user, catalog=catalog, schema=schema)

    create_sql = 'create temp table ' + plpy.quote_ident(table_name) + ' (\n  '
    insert_sql = 'insert into ' + plpy.quote_ident(table_name) + ' (\n  '
    values_types = []

    try:
        q = Query.start(session, query)

        first = True
        for column in q.columns():
            if column.type == "varchar":
                pg_column_type = "text"
            elif column.type == "bigint":
                pg_column_type = "bigint"
            elif column.type == "boolean":
                pg_column_type = "boolean"
            elif column.type == "double":
                pg_column_type = "double precision"
            else:
                raise Exception, "unknown column type: " + plpy.quote_ident(column.type)

            if first:
                first = False
            else:
                create_sql += ",\n  "
                insert_sql += ",\n  "

            create_sql += plpy.quote_ident(column.name) + ' ' + pg_column_type
            insert_sql += plpy.quote_ident(column.name)
            values_types.append(pg_column_type)

    except Exception as e:
        plpy.error(str(e))

    create_sql += '\n)'
    #if trait:
    #    create_sql += ' '
    #    create_sql += trait
    create_sql += ';'

    insert_sql += '\n) values\n'
    values_sql_format = '(' + ', '.join(['${}'] * len(q.columns())) + ')'
    column_num = len(q.columns())

    #plpy.execute("drop table if exists "+plpy.quote_ident(table_name))
    plpy.execute(create_sql)

    batch = []
    for row in q.results():
        batch.append(row)
        if len(batch) > 10:
            batch_len = len(batch)
            # format string 'values ($1, $2), ($3, $4) ...'
            values_sql = (', '.join([values_sql_format] * batch_len)).format(*range(1, batch_len * column_num + 1))
            batch_insert_sql = insert_sql + values_sql
            # flatten rows into an array
            params = [item for sublist in batch for item in sublist]
            plpy.execute(plpy.prepare(batch_insert_sql, values_types * batch_len), params)
            del batch[:]

    if batch:
        batch_len = len(batch)
        # format string 'values ($1, $2), ($3, $4) ...'
        values_sql = (', '.join([values_sql_format] * batch_len)).format(*range(1, batch_len * column_num + 1))
        batch_insert_sql = insert_sql + values_sql
        # flatten rows into an array
        params = [item for sublist in batch for item in sublist]
        plpy.execute(plpy.prepare(batch_insert_sql, values_types * batch_len), params)

Column = namedtuple('Column', ('name', 'type', 'nullable'))

def presto_create_tables(server, user, catalog):
    session = ClientSession(server=server, user=user, catalog=catalog, schema="default")

    try:
        schemas = {}

        q = Query.start(session, "select table_schema, table_name, column_name, is_nullable, data_type from information_schema.columns")

        rows = q.results();

        if rows is None:
            return

        for row in rows:
            schema_name = row[0]
            table_name = row[1]
            column_name = row[2]
            is_nullable = row[3]
            column_type = row[4]

            tables = schemas.setdefault(schema_name, {})
            columns = tables.setdefault(table_name, [])
            columns.append(Column(column_name, column_type, is_nullable))

        for schema_name, tables in schemas.items():
            if schema_name == "sys" or schema_name == "information_schema":
                # skip system schemas
                continue

            # create schema
            try:
                plpy.execute("create schema %s" % plpy.quote_ident(schema_name))
            except:
                # ignore error
                pass

            for table_name, columns in tables.items():
                create_sql = "create table %s.%s (\n  " % (plpy.quote_ident(schema_name), plpy.quote_ident(table_name))

                first = True
                for column in columns:
                    if column.type == "varchar":
                        pg_column_type = "text"
                    elif column.type == "bigint":
                        pg_column_type = "bigint"
                    elif column.type == "boolean":
                        pg_column_type = "boolean"
                    elif column.type == "double":
                        pg_column_type = "double precision"
                    else:
                        raise Exception("unknown column type: " + plpy.quote_ident(column.type))

                    if first:
                        first = False
                    else:
                        create_sql += ",\n  "

                    create_sql += plpy.quote_ident(column.name) + " " + pg_column_type
                    if not column.nullable:
                        create_sql += " not null"

                create_sql += "\n)"

                plpy.execute("drop table if exists %s.%s" % (plpy.quote_ident(schema_name), plpy.quote_ident(table_name)))
                plpy.execute(create_sql)

    except Exception as e:
        plpy.error(str(e))

