
bin/ctl: bin/ctl.in
	sed -e 's|@@DATADIR@@|$(datarootdir)/prestogres|' -e 's|@@SYSCONFDIR@@|$(sysconfdir)|' < bin/ctl.in > bin/ctl

bin_SCRIPTS = bin/ctl

sysconf_DATA = config/prestogres.conf \
			   config/prestogres_hba.conf \
			   config/prestogres_passwd

pkgdata_DATA = pgsql/presto_client.py \
			   pgsql/prestogres.py \
			   pgsql/setup.sql \
			   config/postgresql.conf

EXTRA_DIST = bin/ctl.in \
			 $(pkgdata_DATA) \
			 $(sysconf_DATA) \
			 LICENSE \
			 NOTICE \
			 ../README.md

