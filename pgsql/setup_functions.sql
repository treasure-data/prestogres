
create or replace function run_presto_as_temp_table(
    "server" text,
    "user" text,
    "catalog" text,
    "schema" text,
    "table_name" text,
    "query" text)
returns void as $$
import prestogres
prestogres.run_presto_as_temp_table(server, user, catalog, schema, table_name, query)
$$ language plpythonu;

create or replace function presto_create_tables(
    "server" text,
    "user" text,
    "catalog" text)
returns void as $$
import prestogres
prestogres.presto_create_tables(server, user, catalog)
$$ language plpythonu;

create or replace function raise_error("message" text)
returns void as $$
begin
raise exception '%', message;
end;
$$
language plpgsql;

