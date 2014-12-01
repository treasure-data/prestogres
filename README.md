# ![Prestogres](https://gist.githubusercontent.com/frsyuki/8328440/raw/6c3a19b7132fbbf975155669f308854f70fff1e8/prestogres.png)
## PostgreSQL protocol gateway for Presto

**Prestogres** is a gateway server that allows clients to use PostgreSQL protocol to run queries on Presto.

You can use any PostgreSQL clients (see also *Limitation* section):

* `psql` command
* [PostgreSQL ODBC driver](http://psqlodbc.projects.pgfoundry.org/)
* [PostgreSQL JDBC driver](http://jdbc.postgresql.org/)

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
* [Authentication](#authentication)
  * [md5 method](#md5-method)
  * [external method](#external-method)
* [Development](#development)

---

## How it works?

Prestogres uses modified version of **[pgpool-II](http://www.pgpool.net/)** to rewrite queries before sending them to PostgreSQL.
pgpool-II is originally a middleware to provide connection pool and load balancing to PostgreSQL. Prestogres hacked it as following:

* When a client connects to pgpool-II, the modified pgpool-II runs **SELECT setup\_system\_catalog(...)** statement on PostgreSQL.
  * This function is implemented on PostgreSQL using PL/Python.
  * It gets list of tables from Presto, and runs CREATE TABLE for each tables.
  * Those created tables are empty, but clients can get the table schemas.
* When the client runs a regular SELECT statement, the modified pgpool-II rewrites the query to run **SELECT * FROM fetch\_presto\_query\_results(...)** statement.
  * This function runs the original query on Presto and returns the results.
* If the statement is not regular SELECT (such as SET, SELECT from system catalogs, etc.), pgpool-II simply forwards the statement to PostgreSQL without rewriting.

In fact, there're some more tricks. See [prestogres/pgsql/prestogres.py](prestogres/pgsql/prestogres.py) for the real behavior.

---

## Limitation

* Extended query is not supported ([PostgreSQL Frontend/Backend Protocol](http://www.postgresql.org/docs/9.3/static/protocol.html))
  * ODBC driver needs to set:
     * **Server side prepare = no** property (UseServerSidePrepare=0 at .ini file)
     * **Use Declare/Fetch = no** property (UseDeclareFetch=0 at .ini file)
     * **Level of rollback on errors = Nop** property (Protocol=7.4-0 or Protocol=6.4 at .ini file)
     * **Unicode** mode
  * JDBC driver needs to set:
     * **protocolVersion=2** property
* Cursor (DECLARE/FETCH) is not supported
* Temporary table is not supported
* Transaction is not supported

---

## Installation

### 1. Install PostgreSQL >= 9.3

You need to install PostgreSQL separately. Following commands install PostgreSQL 9.3 from postgresql.org:

**Ubuntu/Debian:**

```sh
# add apt source
$ wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | sudo apt-key add -
$ sudo apt-get update
# install PostgreSQL
$ sudo apt-get install postgresql-9.3 postgresql-server-dev-9.3 postgresql-plpython-9.3
# install other dependencies
$ sudo apt-get install gcc make libssl-dev libpcre3-dev
```

**RedHat/CentOS:**

```sh
# add yum source
$ sudo yum install http://yum.postgresql.org/9.3/redhat/rhel-6-x86_64/pgdg-redhat93-9.3-1.noarch.rpm
# install PostgreSQL
$ sudo yum install postgresql93-server postgresql93-contrib postgresql93-devel postgresql93-plpython
# install other dependencies
$ sudo yum install gcc make openssl-devel pcre-devel
```

**Mac OS X:**

You can install PostgreSQL using [Homebrew](http://brew.sh/).

```sh
brew install postgresql
```

### 2. Install Prestogres

Download the latest release from [releases](https://github.com/treasure-data/prestogres/releases) or clone the [git repository](https://github.com/treasure-data/prestogres). You can install the binary as following:

```
$ ./configure --program-prefix=prestogres-
$ make
$ sudo make install
```

You can find **prestogres-ctl** command:

```
$ prestogres-ctl --help
```

---

## Running servers

You need to run 2 server programs: pgpool-II and PostgreSQL.
You can use `prestogres-ctl` command to setup & run them as following:

```sh
# 1. Configure configuration file (at least, presto_server and presto_catalog parameters):
$ vi /usr/local/etc/prestogres.conf

# 2. Create a data directory:
$ prestogres-ctl create pgdata
# vi pgdata/postgresql.conf  # edit configuration if necessary

# 3. Start PostgreSQL
$ sudo prestogres-ctl postgres -D pgdata

# 4. Open another shell, and initialize the database to install PL/Python functions
$ prestogres-ctl migrate

# 5. Start pgpool-II:
$ sudo prestogres-ctl pgpool

# 6. Finally, you can connect to pgpool-II using psql command:
$ psql -h 127.0.0.1 -p 5439 -U presto sys
```

If configuration is correct, you can run `SELECT * FROM sys.node;` query. Otherwise, see log messages.

#### Setting shmem max parameter

Above command fails first time on most of environments! Error message is:

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

Please read [pgpool-II documentation](http://www.pgpool.net/docs/latest/pgpool-en.html) for most of parameters used in prestogres.conf file.
Following parameters are unique to Prestogres:

* **presto_server**: Default address:port of Presto server.
* **presto_catalog**: Default catalog name of Presto such as `hive`, etc.
* **presto_external_auth_prog**: Default path to an external authentication program used by `external` authentication moethd. See following Authentication section for details.

You can overwrite these parameters for each connecting users (and databases) using prestogres\_hba.conf file. See also following *Authentication* section.

## Authentication

By default, Prestogres accepts all connections from 127.0.0.1 without password and rejects any other connections. You can change this behavior by updating **/etc/prestogres\_hba.conf** file.

See [sample prestogres_hba.conf file](prestogres/config/prestogres_hba.conf) for details. Basic syntax is:

```conf
# TYPE   DATABASE   USER   CIDR-ADDRESS                  METHOD                OPTIONS
host     postgres   pg     127.0.0.1/32                  trust
host     postgres   pg     127.0.0.1/32,192.168.0.0/16   md5
host     altdb      pg     0.0.0.0/0                     md5                   presto_server:localhost:8190,
host     all        all    0.0.0.0/0                     external              auth_prog:/opt/prestogres/auth.py
```

### md5 method

This authentication method uses a password file (**$prefix/etc/prestogres\_passwd**) to authenticate an user. You can use `prestogres passwd` command to add an user to this file:

```sh
$ prestogres-pg_md5 -pm -u myuser
password: (enter password here)
```

In prestogres\_hba.conf file, you can set following options to the OPTIONS field:

* **presto_server**: Address:port of Presto server, which overwrites `presto_servers` parameter in prestogres.conf.
* **presto_catalog**: Catalog name of Presto, which overwrites `presto_catalog` parameter in prestogres.conf.
* **presto_schema**: Default schema name of Presto. By default, Prestogres uses the same name with the database name used to login to pgpool-II. Following `pg_database` parameter doesn't overwrite affect this parameter.
* **presto_user**: User name to run queries on Presto. By default, Prestogres uses the same user name used to login to pgpool-II. Following `pg_user` parameter doesn't overwrite affect this parameter.
* **pg_database**: Overwrite database to connect to PostgreSQL. The value should be `postgres` in most of cases.
* **pg_user**: Overwrite user name to connect to PostgreSQL. This value should be `pg` in most of cases.


### external method

This authentication method uses an external program to authentication an user.

- Note: This method is still experimental (because performance is slow). Interface could be changed.
- Note: This method requires clients to send password in clear text. It's recommended to enable SSL in prestogres.conf.

You need to set `presto_external_auth_prog` parameter in prestogres.conf or `auth_prog` option in prestogres_hba.conf. Prestogres runs the program every time when an user connects. The program receives following data through STDIN:

```
user:USER_NAME
password:PASSWORD
database:DATABASE
address:IPADDR

```

If you want to allow this connection, the program optionally prints parameters as following to STDOUT, and exists with status code 0:

```
presto_server:PRESTO_SERVER_ADDRESS
presto_catalog:PRESTO_CATALOG_NAME
presto_schema:PRESTO_SCHEMA_NAME
presto_user:USER_NAME
pg_database:DATABASE
pg_user:USER_NAME

```

If you want to reject this connection, the program exists with non-0 status code.

___

    Prestogres is licensed under Apache License, Version 2.0.
    Copyright (C) 2014 Sadayuki Furuhashi

