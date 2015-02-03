# perl-xbase2pg #

## The command line program converts from "dbf/dbase" file to postgresql sql. ##

### Installing ###

```bash
sudo cpan XBase Encode Getopt::Long
git clone https://github.com/iostrovok/perl-xbase2pg.git
```

### Quick start ###

```perl
perl xbase2pg.pl --schema=mydbschema --from=cp866 --split=100000 --com="COMMENT FOR DB." | psql -U $PGUSER --host=$PGHOST --port=$PGPORT --dbname=$PGDATABASE
```
### Usage ###

> perl xbase2pg.pl [-m SCHEMA] [-f ENCODING] [-f NUMBER] [-c COMMENT] DBFFILE

Options controlling the dbf format:

  -m SCHEMA,   --schema=SCHEMA where table will created
  
  -f ENCODING, --from=ENCODING the encoding of the DBFFILE
  
  -s NUMBER,   --split=NUMBER of row for each INSERT statment
  
  -c COMMENT,  --com=COMMENT for table
  
### Example ###

See example 
```bash
  cd perl-xbase2pg/example
  perl fias.pl -h
```


