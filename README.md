# PostgreSQL protocol gateway for Presto

presto-pggw is a gateway server to allow users to use PostgreSQL protocol to run
queries on Presto.

Presto, a distributed SQL query engine for big data:
https://github.com/facebook/presto

With presto-pggw, you can use `psql` command, [PostgreSQL ODBC driver](http://psqlodbc.projects.pgfoundry.org/), [PostgreSQL JDBC driver](http://jdbc.postgresql.org/), or other PostgreSQL client libraries to run queries on Presto.

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
                         presto-pggw
```

1. pgpool-II recives queries from clients pgpool-II is patched.
2. pgpool-II rewrites query to to 'SELECT run_presto_as_temp_table(..., 'SELECT <original query>')
2. `run_presto_as_temp_table` function implemented in PostgreSQL runs the query on Presto

## Prerequirements

* Ruby and RubyGems
* PostgreSQL with Python support
* toolchain to build pgpool-II

## Install

1. clone presto-pggw repository:

```sh
git clone https://github.com/treasure-data/presto-pggw.git
cd presto-pggw
```

2. install bundler gem:

```sh
$ gem install bundler
```

3. run `bundle`:

```sh
$ bundle
```

4. create a gem package:

```sh
$ bundle exec rake
```

5. install the created package:

```sh
$ gem install pkg/presto-pggw-0.1.0.gem
```

## Run

1. run setup command to create data directory:
```sh
$ presto-pggw -D pggw setup
```
2. run patched pgpool-II:
```sh
$ presto-pggw -D pggw pgpool
```
3. run patched PostgreSQL:
```sh
$ presto-pggw -D pggw pg_ctl start
```

Usage of `presto-pggw` command:

```
usage: presto-pggw -D <data dir> <command>
commands:
  setup                 setup <data dir>
  pgpool                start pgpool as a daemon process
  pgpool stop           stop  pgpool daemon process
  pgpool -n             start pgpool as a foreground process
  pg_ctl start          start postgres server as a daemon process
  pg_ctl stop           stop  postgres server daemon process
  postgres              start postgres server as a foreground process
```
