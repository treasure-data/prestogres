
create extension dblink;

create or replace function prestogres_init_database(access_role text, target_db text, target_db_conninfo text)
returns text as $CREATE$
begin
    return dblink_exec(target_db_conninfo, E'do $INIT$ begin
        perform pg_advisory_lock(3235398540741723243);  -- prevent "tuple concurrently updated" error

        if not exists (select * from pg_language where lanname = \'plpythonu\') then
            create language plpythonu;
        end if;

        if not exists (select * from pg_namespace where nspname = \'prestogres_catalog\') then
            create schema prestogres_catalog;
        end if;

        create or replace function prestogres_catalog.start_presto_query(
            presto_server text, presto_user text, presto_catalog text, presto_schema text,
            function_name text, query text)
        returns void as $$
            import prestogres
            prestogres.start_presto_query(
                presto_server, presto_user, presto_catalog, presto_schema, function_name, query)
        $$ language plpythonu
        security definer;

        create or replace function prestogres_catalog.setup_system_catalog(
            presto_server text, presto_user text, presto_catalog text, presto_schema text,
            access_role text)
        returns void as $$
            import prestogres
            prestogres.setup_system_catalog(
                presto_server, presto_user, presto_catalog, presto_schema, access_role)
        $$ language plpythonu
        security definer;

        revoke temporary on database "' || target_db || E'" from public;  -- reject CREATE TEMPORARY TABLE
        grant usage on schema prestogres_catalog to "' || access_role || E'";
        grant execute on all functions in schema prestogres_catalog to "' || access_role || E'";

    end $INIT$ language plpgsql');
end
$CREATE$ language plpgsql;

