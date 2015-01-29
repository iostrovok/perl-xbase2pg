#!/usr/bin/perl

=head QuickStart
    > perl ./Xbase2Pg.pl ./data/fias_delta_dbf/ADDROBJ.DBF | iconv -c -f CP866 -t UTF-8) | psql -U pgsql -d dbname

perl ./Xbase2Pg.pl ./data/fias_delta_dbf/ADDROBJ.DBF | iconv -c -f CP866 -t UTF-8) | psql --host=127.0.0.1 --port=5432 --dbname=pqgotest -U postgres

=cut

use strict;
use warnings;
use autodie;
use utf8;
use XBase;
use Encode;
use open qw(:std :utf8);
use Getopt::Long;

my $file = pop @ARGV || die "No file name\n";
die "Not found $file\n" unless -f $file;

my $EncodFrom  = "";
my $SplitCount = 0;
my $Comment    = '';
GetOptions(
    "from=s"  => \$EncodFrom,
    "split=n" => \$SplitCount,
    "com=n"   => \$Comment,
);

print "----START\n";

my $table_name = $file;
die "File have to finish '.DBF' []\n" unless $table_name =~ s/\.DBF$//i;

$table_name =~ s/^.+\///i;

my $table = new XBase $file or die XBase->errstr;

print "---- FINISH 'new XBase $file'\n";

my @names    = $table->field_names;
my @types    = $table->field_types;
my @lengths  = $table->field_lengths;
my @decimals = $table->field_decimals;

print "DROP TABLE IF EXISTS $table_name;\n";
print "CREATE TABLE $table_name () WITH (OIDS=FALSE);\n";
if ( $Comment ne '' ) {
    $Comment =~ s/'/''/gios;
    print "COMMENT ON TABLE $table_name () IS '$Comment';\n";
}

my $type = {
    'C' => \&set_C,
    'D' => \&set_D,
    'N' => \&set_N,
    'L' => \&set_L,
    'M' => \&set_M,
    '@' => \&set_T,
    'I' => \&set_I,
    '+' => \&set_I,
    'F' => \&set_F,
    'G' => \&set_G,
    'O' => \&set_O,
};

my $quote = {
    'C' => \&quote_C,
    'D' => \&quote_D,
    'N' => \&quote_N,
    'L' => \&quote_L,
    'M' => \&quote_M,
    '@' => \&quote_T,
    'I' => \&quote_I,
    '+' => \&quote_I,
    'F' => \&quote_F,
    'G' => \&quote_G,
    'O' => \&quote_O,
};

my $info = {};

print "---- START READ COLUMNS\n";
for my $i ( 0 .. $#names ) {
    print "ALTER TABLE $table_name ADD COLUMN $names[$i] "
        . $type->{ $types[$i] }->( $lengths[$i], $decimals[$i] ) . ";\n";
    $info->{ $names[$i] } = {
        names    => $names[$i],
        types    => $types[$i],
        lengths  => $lengths[$i],
        decimals => $decimals[$i],
    };
}

my $sql = "INSERT INTO $table_name (" . join( ',', @names ) . ") VALUES ";
my $total = $table->last_record;

my $SplitCountTmp = $SplitCount;
my @Tmp;
foreach my $n ( 0 .. $total ) {
    my $line = one_record( $table, $n, $info );

    unless ($SplitCount) {
        print $sql . $line, ";\n";
        next;
    }

    push( @Tmp, $line );

    next if --$SplitCountTmp > 0;

    print $sql . join( ",\n", @Tmp ), ";\n";
    $SplitCountTmp = $SplitCount;
    @Tmp           = ();
}

if ( $SplitCount && @Tmp ) {
    print $sql . join( ",\n", @Tmp ), ";\n";
}

$table->close();

print "\n";
exit;

sub one_record {
    my ( $table, $n, $info ) = @_;
    my (%data) = $table->get_record_as_hash($n);

    next if $data{_DELETED};

    my @vals
        = map { $quote->{ $info->{$_}{types} }->( $data{$_}, $info->{$_} ) }
        @names;

    my $line = '(' . join( ', ', @vals ) . ')';
    if ($EncodFrom) {
        $line = decode( 'cp866', $line );
    }

    return $line;
}

sub set_C {
    my ( $lengths, $decimals ) = @_;
    return "text" unless $lengths > 0;
    return "character varying($lengths)";
}

sub quote_C {
    my ( $val, $info ) = @_;
    $val =~ s/'/''/gios;
    return "'$val'";
}

sub set_D {
    my ( $lengths, $decimals ) = @_;
    return "date";
}

sub quote_D {
    my ( $val, $info ) = @_;
    $val =~ s/(\d\d\d\d)(\d\d)(\d\d)/$1-$2-$3/g;
    return "'$val'";
}

sub set_N {
    my ( $lengths, $decimals ) = @_;
    return "int";
}

sub quote_N {
    my ( $val, $info ) = @_;
    return int( $val || 0 );
}

sub set_L {
    my ( $lengths, $decimals ) = @_;
    return "boolean";
}

sub quote_L {
    my ( $val, $info ) = @_;
    return $val ? 'TRUE' : 'FALSE';
}

sub set_M {
    my ( $lengths, $decimals ) = @_;

    # TODO ARRAY
    return "bigint";
}

sub quote_M {
    my ( $val, $info ) = @_;
    return int( $val || 0 );
}

sub set_T {
    my ( $lengths, $decimals ) = @_;
    return "bigint";
}

sub quote_T {

# @   Timestamp   8 bytes - two longs, first for date, second for time.  The date is the number of days since  01/01/4713 BC.
# Time is hours * 3600000L + minutes * 60000L + Seconds * 1000L
    my ( $val, $info ) = @_;
    return int( $val || 0 );
}

sub set_I {
    my ( $lengths, $decimals ) = @_;
    return "int";
}

sub quote_I {
    my ( $val, $info ) = @_;
    return int( $val || 0 );
}

sub set_F {
    my ( $lengths, $decimals ) = @_;
    return "real";
}

sub quote_F {
    my ( $val, $info ) = @_;
    $val =~ s/[^-0-9,\.]//gios;
    return $val;
}

sub set_O {
    my ( $lengths, $decimals ) = @_;
    return "bigint";
}

sub quote_O {
    my ( $val, $info ) = @_;
    return int( $val || 0 );
}

sub set_G {
    my ( $lengths, $decimals ) = @_;

    # TODO ARRAY
    return "real";
}

sub quote_G {
    my ( $val, $info ) = @_;
    $val =~ s/[^-0-9,\.]//gios;
    return $val;
}

#return @values;

=headB  Binary, a string    10 digits representing a .DBT block number. The number is stored as a string, right justified and padded with blanks.
C   Character   All OEM code page characters - padded with blanks to the width of the field.
D   Date    8 bytes - date stored as a string in the format YYYYMMDD.
N   Numeric     Number stored as a string, right justified, and padded with blanks to the width of the field. 
L   Logical     1 byte - initialized to 0x20 (space) otherwise T or F.
M   Memo, a string  10 digits (bytes) representing a .DBT block number. The number is stored as a string, right justified and padded with blanks.
@   Timestamp   8 bytes - two longs, first for date, second for time.  The date is the number of days since  01/01/4713 BC. Time is hours * 3600000L + minutes * 60000L + Seconds * 1000L
I   Long    4 bytes. Leftmost bit used to indicate sign, 0 negative.
+   Autoincrement   Same as a Long
F   Float   Number stored as a string, right justified, and padded with blanks to the width of the field. 
O   Double  8 bytes - no conversions, stored as a double.
G   OLE     10 digits (bytes) representing a .DBT block number. The number is stored as a string, right justified and padded with blanks.
=cut
