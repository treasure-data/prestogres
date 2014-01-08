from presto_client import *
import plpy

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

def presto_create_tables(server, user, catalog):
    session = ClientSession(server=server, user=user, catalog=catalog, schema="default")

    try:
        #q = Query.start(session, "select table_schema, table_name, column_name, is_nullable, data_type from information_schema.columns")

        q = Query.start(session, "show schemas")
        schemas = map(lambda row: row[0], q.results())

        for schema in schemas:
            q = Query.start(session, "show tables from " + plpy.quote_ident(schema))
            tables = map(lambda row: row[0], q.results())

            if schema == "sys" or schema == "information_schema":
                continue

            plpy.execute("create schema "+plpy.quote_ident(schema))

            for table in tables:
                q = Query.start(session, "describe " + plpy.quote_ident(schema) + "." + plpy.quote_ident(table))

                sql = "create table " + plpy.quote_ident(schema) + "." + plpy.quote_ident(table) + " (\n  ";

                first = True
                for row in q.results():
                    column_name = row[0]
                    column_type = row[1]
                    nullable = row[2]

                    if column_type == "varchar":
                        pg_column_type = "text"
                    elif column_type == "bigint":
                        pg_column_type = "bigint"
                    elif column_type == "boolean":
                        pg_column_type = "boolean"
                    elif column_type == "double":
                        pg_column_type = "double precision"
                    else:
                        raise Exception("unknown column type: " + plpy.quote_ident(column_type))

                    if first:
                        first = False
                    else:
                        sql += ",\n  "

                    sql += plpy.quote_ident(column_name) + " " + pg_column_type
                    if not nullable:
                        sql += " not null"

                sql += "\n)"

                plpy.execute(sql)

    except Exception as e:
        plpy.error(str(e))

