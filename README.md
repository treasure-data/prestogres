# Prestogres - PostgreSQL protocol gateway for Presto

Prestogres is a gateway server that allows PostgreSQL clients to run queries on Presto.

* [Presto, a distributed SQL query engine for big data](https://github.com/facebook/presto)

With Prestogres, you can use PostgreSQL clients to run queries on Presto:

* `psql` command
* [PostgreSQL ODBC driver](http://psqlodbc.projects.pgfoundry.org/)
* [PostgreSQL JDBC driver](http://jdbc.postgresql.org/)
* other PostgreSQL client libraries

## How it works?

```
       PostgreSQL protocol                       Presto protocol (HTTP)
            /                                       /
           /  +-----------+      +------------+   /  +--------+
  client ---> | pgpool-II | ---> | PostgreSQL | ---> | Presto |
              +-----------+      +------------+      +--------+
                       \                   \
                    rewrite queries       run custom functions
            |                                   |
            +-----------------------------------+
                   Prestogres
```

1. pgpool-II recives a query from clients pgpool-II is patched.
2. pgpool-II rewrites the query to `SELECT run_presto_as_temp_table(..., '...original SELECT query...')`
2. `run_presto_as_temp_table` function implemented in PostgreSQL runs the query on Presto

Prestogres package installs patched pgpool-II but doesn't install PostgreSQL.
You need to install PostgreSQL (with python support) separately.

## Prerequirements

* Ruby and RubyGems
* PostgreSQL with Python support
* toolchain to build pgpool-II

## Install

```sh
# 1. clone prestogres repository:
git clone https://github.com/treasure-data/prestogres.git
cd prestogres

# 2. install bundler gem and run it:
gem install bundler
bundle

# 3. create a gem package:
$ bundle exec rake

# 4. install the created package:
$ gem install pkg/prestogres-0.1.0.gem
```

## Run

```sh
# 1. run setup command to create data directory:
prestogres -D pgdata setup

# 2. run patched pgpool-II:
$ prestogres -D pgdata pgpool

# 3. run patched PostgreSQL:
$ prestogres -D pgdata pg_ctl start
```

Usage of `prestogres` command:

```
usage: prestogres -D <data dir> <command>
commands:
  setup                 setup <data dir>
  pgpool                start pgpool as a daemon process
  pgpool stop           stop  pgpool daemon process
  pgpool -n             start pgpool as a foreground process
  pg_ctl start          start postgres server as a daemon process
  pg_ctl stop           stop  postgres server daemon process
  postgres              start postgres server as a foreground process
```

