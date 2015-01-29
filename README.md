# perl-xbase2pg

## Simple GO interface to postgresql database and SQL templater ##

### Introduction ###

No yet

### Installing ###

```bash
> sudo cpan XBase Encode Getopt::Long
>	git clone https://github.com/iostrovok/perl-xbase2pg.git
```

### Quick start ###

```perl
perl xbase2pg.pl --schema=mydbschema --from=cp866 --split=100000 --com="COMMENT FOR DB." | psql -U $PGUSER --host=$PGHOST --port=$PGPORT --dbname=$PGDATABASE
```
