# ![Prestogres](https://gist.github.com/frsyuki/8328440/raw/6c3a19b7132fbbf975155669f308854f70fff1e8/prestogres.png)
## PostgreSQL protocol gateway for Presto

**Prestogres** is a gateway server that allows clients to use PostgreSQL protocol to run Presto queries.

* [Presto, a distributed SQL query engine for big data](https://github.com/facebook/presto)

With Prestogres, you can use PostgreSQL clients to run queries on Presto:

* `psql` command
* [PostgreSQL ODBC driver](http://psqlodbc.projects.pgfoundry.org/)
* [PostgreSQL JDBC driver](http://jdbc.postgresql.org/)
* other PostgreSQL client libraries

Prestogres also offers password-based authorization (Presto doesn't have authorization by default).

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

1. **Client** sends a query to a patched pgpool-II
2. **pgpool-II** rewrites the query to `SELECT run_presto_as_temp_table(..., '...original SELECT query...')` + `SELECT * FROM presto_result`.
3. **PostgreSQL** runs `run_presto_as_temp_table`. This custom function runs a query on Presto and writes results into a temporary table `presto_result`.

## Limitation

* Extended query is not supported ([PostgreSQL Frontend/Backend Protocol](http://www.postgresql.org/docs/9.3/static/protocol.html))
  * ODBC driver needs to set **UseServerSidePrepare=0** (Server side prepare: no) property
  * JDBC driver needs to set **protocolVersion=2** property
* DECLARE/FETCH is not supported

## Installation

```
$ gem install prestogres --no-ri --no-rdoc
```

Prestogres package installs patched pgpool-II but doesn't install PostgreSQL. You need to install PostgreSQL server (with python support) separately.

* If you don't have **gem** command, install Ruby >= 1.9.0 first
* If installation failed, you may need to install following packages using apt or yum:
  * basic toolchain (gcc, make, etc.)
  * OpenSSL (Debian/Ubuntu: **libssl-dev**, RedHat/CentOS: **openssl-dev**)
  * PostgreSQL server (Debian/Ubuntu: **postgresql-server-dev**, RedHat/CentOS: **postgresql-devel**)

## Runing servers

You need to run 2 server programs: pgpool-II and PostgreSQL. You can use `prestogres` command to setup & run them.

1. Create a data directory:
```
$ prestogres -D pgdata setup
```

2. Configure **presto_server** and **presto_catalog** parameters at least in pgpool.conf file:
```
$ vi ./pgdata/pgpool/pgpool.conf
```

3. Run patched pgpool-II:
```
$ prestogres -D pgdata pgpool
```

4. Run PostgreSQL:
```
$ prestogres -D pgdata pg_ctl start
```

5. Finally, you can connect to pgpool-II using `psql` command:
```
$ psql -h localhost -p 9900 -U pg postgres
```

If configuration is correct, you can run `SELECT * FROM sys.node;` query. Otherwise, see log files in **./pgdata/log/** directory.

* Note: on Mac OS X, you need to run following commands before running PostgreSQL:
```sh
$ sudo sysctl -w kern.sysv.shmmax=1073741824
$ sudo sysctl -w kern.sysv.shmall=1073741824
```

## Configuration

### pgool.conf file

Please read [pgpool-II documentation](http://www.pgpool.net/docs/latest/pgpool-en.html) for most of parameters.
Following parameters are unique to Prestogres:

* **presto_server**: Default address:port of Presto server.
* **presto_catalog**: Default catalog (connector) name of Presto such as `hive-cdh4`, `hive-hadoop1`, etc.
* **presto_schema**: Default schema name of Presto. You can read other schemas by qualified name like `FROM myschema.table1`
* **presto_external_auth_prog**: Default path to an external authentication program used by `prestogres_external` authentication moethd. See following Authentication section for details.

You can overwrite these parameters for each connecting users. See also following *pool_hba.conf* section.

### pool_hba.conf file

By default configuration, Prestogres accepts all connections from localhost without password and rejects any other connections. You can change this behavior by updating **\<data_dir\>/pgpool2/pool_hba.conf** file.

See [sample pool_hba.conf file](https://github.com/treasure-data/prestogres/blob/master/config/pool_hba.conf) for details. Basic syntax is:

```conf
# TYPE   DATABASE   USER   CIDR-ADDRESS                  METHOD                OPTIONS
host     all        all    127.0.0.1/32                  trust
host     all        all    127.0.0.1/32,192.168.0.0/16   prestogres_md5
host     altdb      pg     0.0.0.0/0                     prestogres_md5        server:localhost:8190,user:prestogres
host     all        all    0.0.0.0/0                     prestogres_external   auth_prog:/opt/prestogres/auth.py
```

#### prestogres_md5 method

This authentication method uses a password file **\<data_dir\>/pgpool2/pool_passwd** to authenticate an user. You can use `prestogres passwd` command to add an user to this file:

```sh
$ prestogres -D pgdata passwd myuser
password: (enter password here)
```

In pool_hba.conf file, you can set following options to OPTIONS field:

* **server**: Address:port of Presto server, which overwrites `presto_servers` parameter in pgpool.conf.
* **catalog**: Catalog (connector) name of Presto, which overwrites `presto_catalog` parameter in pgpool.conf.
* **schema**: Schema name of Presto, which overwrites `presto_schema` parameter in pgpool.conf.
* **user**: User name to run queries on Presto. By default, Prestogres uses the same user name used to login to pgpool-II.


#### prestogres_external method

This authentication method uses an external file to authentication an user.

- Note: This method is still experimental (because performance is slow). Interface could be changed.
- Note: This method requires clients to send password in clear text. It's recommended to enable SSL in pgpool.conf.

You need to set `presto_external_auth_prog` parameter in pgpool.conf or `auth_prog` option in pool_hba.conf. Prestogres runs the program every time when an user connects. The program receives following data through STDIN:

```
user:USER_NAME
password:PASSWORD
database:DATABASE
address:IPADDR

```

If you want to allow this connection, the program optionally prints following lines to STDOUT, and exists with status code 0:

```
server:PRESTO_SERVER_ADDRESS
catalog:PRESTO_CATALOG_NAME
schema:PRESTO_SCHEMA_NAME

```

If you want to reject this connection, the program exists with non-0 status code.


### prestogres command

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
  passwd <USER NAME>    add new md5 password entry for an user
```

## Development

To install git HEAD, use following command to build:

```sh
# 1. clone prestogres repository:
$ git clone https://github.com/treasure-data/prestogres.git
$ cd prestogres

# 2. install bundler gem and run it:
$ gem install bundler
$ bundle
# if you don't have gem command, you need to install Ruby first

# 3. create a gem package:
$ bundle exec rake

# 4. install the created package:
$ gem install --no-ri --no-rdoc pkg/prestogres-0.1.0.gem
# if this command failed, you may need to install toolchain (gcc, etc.) to build pgpool-II
```

