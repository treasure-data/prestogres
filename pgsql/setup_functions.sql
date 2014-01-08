
create or replace function run_presto_as_temp_table(
    "server" TEXT,
    "user" TEXT,
    "catalog" TEXT,
    "schema" TEXT,
    "table_name" TEXT,
    "query" TEXT)
returns void as $$
import presto_pggw
presto_pggw.run_presto_as_temp_table(server, user, catalog, schema, table_name, query)
$$ language plpythonu;

create or replace function presto_create_tables(
    "server" TEXT,
    "user" TEXT,
    "catalog" TEXT)
returns void as $$
import presto_pggw
presto_pggw.presto_create_tables(server, user, catalog)
$$ language plpythonu;
