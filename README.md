# ![Prestogres](https://gist.github.com/frsyuki/8328440/raw/6c3a19b7132fbbf975155669f308854f70fff1e8/prestogres.png)
## PostgreSQL protocol gateway for Presto

**Prestogres** is a gateway server that allows clients to use PostgreSQL protocol to run queries on Presto.

* [Presto, a distributed SQL query engine for big data](https://github.com/facebook/presto)

You can use any PostgreSQL clients (see also *Limitation* section):

* `psql` command
* [PostgreSQL ODBC driver](http://psqlodbc.projects.pgfoundry.org/)
* [PostgreSQL JDBC driver](http://jdbc.postgresql.org/)
* other PostgreSQL client libraries

Prestogres also offers password-based authentication and SSL.

## Documents

* [How it works?](#how-it-works)
* [Limitation](#limitation)
* [Installation](#installation)
  * [1. Install PostgreSQL >= 9.3](#1-install-postgresql--93)
  * [2. Install Prestogres](#2-install-prestogres)
* [Running servers](#running-servers)
  * [Setting shmem max parameter](#setting-shmem-max-parameter)
* [Configuration](#configuration)
  * [pgpool.conf file](#pgpoolconf-file)
  * [pool_hba.conf file](#pool_hbaconf-file)
     * [prestogres_md5 method](#prestogres_md5-method)
     * [prestogres_external method](#prestogres_external-method)
  * [prestogres command](#prestogres-command)
* [Development](#development)

---

## How it works?

Prestogres uses modified **[pgpool-II](http://www.pgpool.net/)** to rewrite queries before sending them to PostgreSQL.
pgpool-II is originally an open-source middleware to provide connection pool and other features to PostgreSQL. For regular SQL query, patched pgpool-II wraps it in **run_presto_as_temp_table(..., 'SELECT ... FROM ...')** function. This function sends the query to Presto:

![Regular SQL](https://gist.github.com/frsyuki/9012980/raw/2782f51cd3c708254ac64c4c30b7bff0e7894640/figure1.png)

If the query selects records from system catalog (e.g. `\\d` command by psql), patched pgpool-II wraps the query in `run_system_catalog_as_temp_table` function. It gets table list from Presto, and actually runs **CREATE TABLE** to create records in system catalog on PostgreSQL. Then the original query runs as usual on PostgreSQL:

![System catalog access](https://gist.github.com/frsyuki/9012980/raw/f869bd3dd59ab22f439165972c29d122ea560694/figure2.png)

In fact there're some other tricks. See [pgsql/prestogres.py](pgsql/prestogres.py) for the real behavior.

---

## Limitation

* Extended query is not supported ([PostgreSQL Frontend/Backend Protocol](http://www.postgresql.org/docs/9.3/static/protocol.html))
  * ODBC driver needs to set:
     * **UseServerSidePrepare=0** (Server side prepare: no) property
     * **UseDeclareFetch=0** (Use Declare/Fetch: no) property
     * **Unicode** mode
  * JDBC driver needs to set:
     * **protocolVersion=2** property
* Cursor (DECLARE/FETCH) is not supported

---

## Installation

### 1. Install PostgreSQL >= 9.3

You need to install PostgreSQL separately. Following commands install PostgreSQL 9.3 from postgresql.org:

**Ubuntu/Debian:**

```
# add apt source
wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | sudo apt-key add -
sudo apt-get update
# install PostgreSQL
sudo apt-get install postgresql-9.3 postgresql-server-dev-9.3 postgresql-plpython-9.3
# install other dependencies
sudo apt-get install gcc make libssl-dev libpcre3-dev
sudo apt-get install ruby ruby-dev
```

**RedHat/CentOS:**

```
# add yum source
sudo yum install http://yum.postgresql.org/9.3/redhat/rhel-6-x86_64/pgdg-redhat93-9.3-1.noarch.rpm
# install PostgreSQL
sudo yum install postgresql93-server postgresql93-contrib postgresql93-devel postgresql93-plpython
# install other dependencies
sudo yum install gcc make openssl-devel pcre-devel
sudo yum install ruby ruby-devel
```

**Mac OS X:**

```
# install Homebrew
ruby -e "$(curl -fsSL https://raw.github.com/Homebrew/homebrew/go/install)"
# install PostgreSQL
brew install postgresql
```

### 2. Install Prestogres

```
$ sudo gem install prestogres --no-ri --no-rdoc
```

---

## Running servers

You need to run 2 server programs: pgpool-II and PostgreSQL.
You can use `prestogres` command to setup & run them as following:

```sh
# 1. Create a data directory:
$ prestogres -D pgdata setup

# 2. Configure presto_server and presto_catalog parameters at least:
$ vi ./pgdata/pgpool/pgpool.conf

# 3. Run patched pgpool-II:
$ prestogres -D pgdata pgpool

# 4. Run PostgreSQL:
$ prestogres -D pgdata pg_ctl start

# 5. Finally, you can connect to pgpool-II using `psql` command:
$ psql -h localhost -p 9900 -U pg postgres
> SELECT * FROM sys.node;
```

If configuration is correct, you can run `SELECT * FROM sys.node;` query. Otherwise, see log files in **./pgdata/log/** directory.

#### Setting shmem max parameter

Probably above command fails first time! Error message is:

```
FATAL:  could not create shared memory segment: Cannot allocate memory
DETAIL:  Failed system call was shmget(key=6432001, size=3809280, 03600).
HINT:  This error usually means that PostgreSQL's request for a shared memory segment exceeded
available memory or swap space, or exceeded your kernel's SHMALL parameter.  You can either
reduce the request size or reconfigure the kernel with larger SHMALL.  To reduce the request
size (currently 3809280 bytes), reduce PostgreSQL's shared memory usage, perhaps by reducing
shared_buffers or max_connections.
```

You need to set 2 kernel parameters to run PostgreSQL.

**Linux:**

```
sudo bash -c "echo kernel.shmmax = 17179869184 >> /etc/sysctl.conf"
sudo bash -c "echo kernel.shmall = 4194304 >> /etc/sysctl.conf"
sudo sysctl -p /etc/sysctl.conf
```

**Mac OS X:**

```
$ sudo sysctl -w kern.sysv.shmmax=1073741824
$ sudo sysctl -w kern.sysv.shmall=1073741824
```

---

## Configuration

### pgpool.conf file

Please read [pgpool-II documentation](http://www.pgpool.net/docs/latest/pgpool-en.html) for most of parameters.
Following parameters are unique to Prestogres:

* **presto_server**: Default address:port of Presto server.
* **presto_catalog**: Default catalog (connector) name of Presto such as `hive-cdh4`, `hive-hadoop1`, etc.
* **presto_external_auth_prog**: Default path to an external authentication program used by `prestogres_external` authentication moethd. See following Authentication section for details.

You can overwrite these parameters for each connecting users. See also following *pool_hba.conf* section.

### pool_hba.conf file

By default configuration, Prestogres accepts all connections from localhost without password and rejects any other connections. You can change this behavior by updating **\<data_dir\>/pgpool2/pool_hba.conf** file.

See [sample pool_hba.conf file](https://github.com/treasure-data/prestogres/blob/master/config/pool_hba.conf) for details. Basic syntax is:

```conf
# TYPE   DATABASE   USER   CIDR-ADDRESS                  METHOD                OPTIONS
host     postgres   pg     127.0.0.1/32                  prestogres_trust      pg_database:postgres,pg_user:pg
host     postgres   pg     127.0.0.1/32,192.168.0.0/16   prestogres_md5        pg_database:postgres,pg_user:pg
host     altdb      pg     0.0.0.0/0                     prestogres_md5        pg_database:postgres,pg_user:pg,server:localhost:8190,
host     all        all    0.0.0.0/0                     prestogres_external   pg_database:postgres,pg_user:pg,auth_prog:/opt/prestogres/auth.py
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
* **schema**: Default schema name of Presto. By default, Prestogres uses the same name with the database name used to login to pgpool-II. Following `pg_database` parameter doesn't overwrite affect this parameter.
* **user**: User name to run queries on Presto. By default, Prestogres uses the same user name used to login to pgpool-II. Following `pg_user` parameter doesn't overwrite affect this parameter.
* **pg_database**: Overwrite database to connect to PostgreSQL. The value should be `postgres` in most of cases.
* **pg_user**: Overwrite user name to connect to PostgreSQL. This value should be `pg` in most of cases.


#### prestogres_external method

This authentication method uses an external program to authentication an user.

- Note: This method is still experimental (because performance is slow). Interface could be changed.
- Note: This method requires clients to send password in clear text. It's recommended to enable SSL in pgpool.conf.

You need to set `presto_external_auth_prog` parameter in pgpool.conf or `auth_prog` option in pool_hba.conf. Prestogres runs the program every time when an user connects. The program receives following data through STDIN:

```
user:USER_NAME
password:PASSWORD
database:DATABASE
address:IPADDR

```

If you want to allow this connection, the program optionally prints parameters as following to STDOUT, and exists with status code 0:

```
server:PRESTO_SERVER_ADDRESS
catalog:PRESTO_CATALOG_NAME
schema:PRESTO_SCHEMA_NAME
user:USER_NAME
pg_database:DATABASE
pg_user:USER_NAME

```

See *pgool.conf file* section for available parameters.

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
  show_init_sql         display statements to initialize a new PostgreSQL database
```

---

## Development

To install git HEAD, use following commands to build:

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
$ gem install --no-ri --no-rdoc pkg/prestogres-*.gem
# if this command failed, you may need to install toolchain (gcc, etc.) to build pgpool-II
```

___

    Prestogres is licensed under Apache License, Version 2.0.
    Copyright (C) 2014 Sadayuki Furuhashi

