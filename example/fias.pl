#!/usr/bin/perl

use strict;
use warnings;
use autodie;
use utf8;
use File::Pid;
use File::Temp qw/ tempfile tempdir /;
use open qw(:std :utf8);
use Getopt::Long;
use Cwd;

my $IdFile          = "";
my $CHECK_DAYS_BACK = 0;
my $help            = 0;
GetOptions(
    "date=s" => \$IdFile,
    "days=n" => \$CHECK_DAYS_BACK,
    "help"   => \$help,
    "d=s"    => \$IdFile,
    "b=n"    => \$CHECK_DAYS_BACK,
    "h"      => \$help,
);

if ($help) {
    help();
    exit();
}

#-- get current directory
my $pwd = cwd();

our $old_pid = 0;
our $pidfile = File::Pid->new( { file => './file.pid', pid => $$, } );
if ( -f $pidfile->_get_pidfile ) {
    if ( $old_pid = $pidfile->running ) {
        die "$0 is already running in PID $old_pid not $$\n";
    }
}

$pidfile->write;

END {
    $pidfile->remove() if !$old_pid || $old_pid == $$;
}

my $PGHOST     = $ENV{PGHOST};
my $PGUSER     = $ENV{PGUSER};
my $PGPASSWORD = $ENV{PGPASSWORD};
my $PGPORT     = $ENV{PGPORT};
my $PGDATABASE = $ENV{PGDATABASE};

my $Id = db_id();

# Check new fias db
my $dir = exists_new_dbf($Id);
my $i   = 1;
until ( $IdFile ne '' || $dir || $i > $CHECK_DAYS_BACK ) {
    $Id = db_id($i);
    $dir = exists_new_dbf($Id);
    $i++;
}

exit() unless $dir;

my $schema = "fias_$Id";

system("cd $dir; unrar l ./fias_dbf.rar > ./list.txt");

my $perl
    = "perl $pwd/xbase2pg.pl --schema=$schema --from=cp866 --split=100000 --com=\"VERSION_$Id\" ";
my $sql_send
    = "psql -U $PGUSER --host=$PGHOST --port=$PGPORT --dbname=$PGDATABASE";

# Create new schema

my @schema_ex = (
    "CREATE SCHEMA $schema AUTHORIZATION $PGUSER;",
    "GRANT ALL ON SCHEMA $schema TO postgres;",
    "GRANT ALL ON SCHEMA $schema TO public;",
    "COMMENT ON SCHEMA $schema IS 'Fias date is $Id';",
);

foreach (@schema_ex) {
    my $line = "echo \"$_\" | $sql_send \n";
    system($line);
}

my @files;
open FILE, "$dir/list.txt";
while (<FILE>) {
    my @list = grep {$_} split( /\s+/, "$_" );
    my $file = $list[0];
    next unless $file =~ m/\.DBF$/i;
    push @files, $file;
}
close FILE;

foreach my $file ( reverse sort @files ) {
    system("cd $dir; unrar e ./fias_dbf.rar $file");
    my $e = join( ' | ', "$perl $dir/$file", $sql_send );
    system("$e\n");
    unlink("$dir/$file");
}

exit();

sub exists_new_dbf {
    my $db_id = shift;
    my $dir = tempdir( CLEANUP => 1 );

    system(
        "cd $dir; wget http://fias.nalog.ru/Public/Downloads/$db_id/fias_dbf.rar"
    );

    my $s = -s "$dir/fias_dbf.rar";

    return $s > 30000000 ? $dir : undef;
}

sub db_id {
    return $IdFile if $IdFile;

    my $delta = int( shift || 0 );

    my ( undef, undef, undef, $mday, $mon, $year )
        = localtime( time - 60 * 60 * 24 * $delta );
    return sprintf( "%d%02d%02d", $year + 1900, $mon + 1, $mday );
}

sub help {
    print <<END
Usage: 
>

> export PGHOST=host; export PGPORT=port; export PGDATABASE=PGDATABASE;
> export PGUSER=user; export PGPASSWORD=PGPASSWORD;
> perl fias.pl [-m SCHEMA] [-f ENCODING] [-f NUMBER] [-c COMMENT] DBFFILE

Is example how to use xbase2pg.pl.

FIAS 
---
RUS: ФИАС Федеральная информационная адресная система.
---

Options controlling the dbf format:
  -d DATE,   --date=DATE Date of fias where table will created
  -b NUMBER, --days=NUMBER of day for check newst date

Informative output:
  --help                      display this help and exit

Install:

sudo cpan File::Pid File::Temp Getopt::Long Cwd

Example:

# It checks update for selected date
perl fias.pl -d 20150126
# It checks last 10 days
perl fias.pl -b 10

Report bugs to <ostrovok\@gmail.com>.

ENGLISH:
FIAS - Federal Information Addressable System of Russia. Details on http://fias.nalog.ru.

This program is designed to automatically download data files FIAS
and providing them Postgresql format.

This program is an example of using the program xbase2pg.pl (https://github.com/iostrovok/perl-xbase2pg)

RUSSIA:
ФИАС - Федеральная информационная адресная система. Подробности на http://fias.nalog.ru.

Данная программа предназначена для автоматического скачивания файлов данных ФИАС
и предоставления их в формате Postgresql.

Данная программа - пример использования программы xbase2pg.pl (https://github.com/iostrovok/perl-xbase2pg) 

---

END
}

