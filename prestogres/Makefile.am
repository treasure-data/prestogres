
bin/run: bin/run.in
	sed -E 's|@@DATADIR@@|$(datarootdir)/prestogres|' < bin/run.in > bin/run

bin_SCRIPTS = bin/run

sysconf_DATA = config/prestogres-postgres.conf \
			   config/prestogres.conf \
			   config/prestogres_hba.conf \
			   config/prestogres_passwd

pkgdata_DATA = pgsql/presto_client.py \
			   pgsql/prestogres.py \
			   pgsql/setup.sql

