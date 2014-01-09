# ![Prestogres](https://gist.github.com/frsyuki/8328440/raw/6c3a19b7132fbbf975155669f308854f70fff1e8/prestogres.png)
## PostgreSQL protocol gateway for Presto

**Prestogres** is a gateway server that allows clients to use PostgreSQL protocol to run Presto queries.

* [Presto, a distributed SQL query engine for big data](https://github.com/facebook/presto)

With Prestogres, you can use PostgreSQL clients to run queries on Presto:

* `psql` command
* [PostgreSQL ODBC driver](http://psqlodbc.projects.pgfoundry.org/)
* [PostgreSQL JDBC driver](http://jdbc.postgresql.org/)
* other PostgreSQL client libraries

## How it works?

```
       PostgreSQL protocol                     Presto protocol (HTTP)
            /                                      /
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
2. PostgreSQL runs the custom function `run_presto_as_temp_table` and it runs the query on Presto

Prestogres package installs patched pgpool-II but doesn't install PostgreSQL.
You need to install PostgreSQL (with python support) separately.

## Limitation

Prestogres is still alpha quality.

* Presto server address is not configurable. It's hard-coded at `pgpool2/pool_query_context.c`
* Selecting from system catalogs (such as \dt command) is very slow

## Prerequirements

* Ruby and RubyGems
* PostgreSQL with Python support
* toolchain to build pgpool-II

## Install

```sh
# 1. clone prestogres repository:
$ git clone https://github.com/treasure-data/prestogres.git
$ cd prestogres

# 2. install bundler gem and run it:
$ gem install bundler
$ bundle

# 3. create a gem package:
$ bundle exec rake

# 4. install the created package:
$ gem install --no-ri --no-rdoc pkg/prestogres-0.1.0.gem
```

## Run

```sh
# 1. run setup command to create data directory:
$ prestogres -D pgdata setup

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

