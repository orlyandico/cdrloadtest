#!/usr/bin/perl -w

use strict;
use POSIX;
use Digest::MD5;
use DBI;
use Time::HiRes;
use IO::Handle;
use warnings;

# define table insert mode: PICK ONLY ONE OF THESE!
my $TABLE_PER_HOUR     = 0;
my $PARTITION_PER_HOUR = 1;

# define database host and connection handle
my $dbhost = "myaurora10.cluster-cyligymfhdvt.ap-southeast-1.rds.amazonaws.com";
my $dbname = "myaurora10";
my $dbuser = "orly";
my $dbpassword = "welcome1";

# number of rows per batch insert
# too large a batch size is not good; numbers between 1000 - 5000 seem OK
# larger batch sizes usually result in better records/second but increase replica lag
my $batchSize = 6000;

# number of parallel tasks to do; too many causes thrashing: you normally want these to be <= the number of vCPU on the database
my $numWorkers = 16;

### YOU DON'T NEED TO TOUCH ANYTHING BELOW THIS LINE ###

# create the sub-processes
my $pc = 0;
my @pids;
my $mypid;
while ( $pc < $numWorkers ) {
    $mypid = fork();
    if ( $mypid == 0 ) {
        last;
    }
    else {
        push @pids, $mypid;
    }
    $pc++;
}

# reap the children to avoid zombies
if ( $mypid != 0 ) {
    for (@pids) {
        my $thispid = $_;
        waitpid( $thispid, WNOHANG );
    }
    exit(0);
}

# randomly sleep to prevent "thundering herd" problem
sleep( int( rand(15) + 1 ) );
my $pid = POSIX::getpid();

my $dbh;

# open an append-only connection to the log
open( LOG, ">>/tmp/insert_pg.log" ) or die;
LOG->autoflush(1);

# create parent tables (cdrdata and cdrdata_part)
# create a table of the format cdrdata_YYYYMMDDHH
sub create_parent_tables {

    ########## NON-partitioned parent table ##########

    my $table_name = "cdrdata";

    # first check if the table already exists
    my $sth = $dbh->prepare(
"select table_name from information_schema.tables where table_name = '$table_name'"
    );
    my $tab = 0;
    $sth->execute;
    $sth->bind_col( 1, \$tab );
    $sth->fetch;
    $sth->finish;
    if ( lc($tab) eq lc($table_name) ) {
        print LOG "[$pid] Table $table_name already exists, skipping create\n";
    }
    else {
        my $k1 = "cdrdatapk_k1";
        my $k2 = "cdrdatapk_k2";

        my $sql = qq{
	create table if not exists $table_name (
		cdrType INT2,
		callId INT4,
		aNumber VARCHAR(16) not null,
		bNumber VARCHAR(16) not null,
		origCauseLocation INT2,
		origCauseValue INT2,
		destCauseLocation INT2,
		destCauseValue INT2,
		dateTimeConnect TIMESTAMP not null,
		dateTimeDisconnect TIMESTAMP not null,
		duration INT2 not null,
		pkid VARCHAR(200)
	);

	create index if not exists $k1 on $table_name(aNumber);
	create index if not exists $k2 on $table_name(bNumber);
	};

# we have to catch this, otherwise the application fails
# note that there are many workers, and all the rest die (on the first succeeds)
        eval {
            my $sth = $dbh->prepare($sql);
            $sth->execute;
        };

        if ($@) {

            # do nothing, but sleep a random small amount
            print LOG "[$pid] Error received creating table: $@\n";
            Time::HiRes::usleep( int( rand(15) + 1 ) * 100000 );
        }
        else {
            print LOG "[$pid] Created table:\n$sql\n";
        }
    }
}

sub create_parent_partition {
    ########## partitioned parent table ##########

    my $table_name = "cdrdata_part";

    # first check if the table already exists
    my $sth = $dbh->prepare(
"select table_name from information_schema.tables where table_name = '$table_name'"
    );
    my $tab = 0;
    $sth->execute;
    $sth->bind_col( 1, \$tab );
    $sth->fetch;
    $sth->finish;
    if ( lc($tab) eq lc($table_name) ) {
        print LOG "[$pid] Table $table_name already exists, skipping create\n";
    }
    else {
        my $sql = qq{
			create table if not exists $table_name (
				cdrType INT2,
				callId INT4,
				aNumber VARCHAR(16) not null,
				bNumber VARCHAR(16) not null,
				origCauseLocation INT2,
				origCauseValue INT2,
				destCauseLocation INT2,
				destCauseValue INT2,
				dateTimeConnect TIMESTAMP not null,
				dateTimeDisconnect TIMESTAMP not null,
				duration INT2 not null,
				pkid VARCHAR(200)
			) PARTITION BY RANGE(dateTimeConnect);
		};

# we have to catch this, otherwise the application fails
# note that there are many workers, and all the rest die (on the first succeeds)
        eval {
            $sth = $dbh->prepare($sql);
            $sth->execute;
        };

        if ($@) {

            # do nothing, but sleep a random small amount
            print LOG "[$pid] Error received creating table: $@\n";
            Time::HiRes::usleep( int( rand(15) + 1 ) * 100000 );
        }
        else {
            print LOG "[$pid] Created parent partitioned table:\n$sql\n";
        }
    }
}

# create a table of the format cdrdata_YYYYMMDDHH
sub create_table {
    my $ts = POSIX::strftime( "%Y%m%d%H", localtime() );
    my $table_name = "cdrdata_" . $ts;

    $table_name = lc($table_name);

    # first check if the table already exists
    my $sth = $dbh->prepare(
"select table_name from information_schema.tables where table_name = '$table_name'"
    );
    my $tab = 0;
    $sth->execute;
    $sth->bind_col( 1, \$tab );
    $sth->fetch;
    $sth->finish;
    if ( lc($tab) eq lc($table_name) ) {
        print LOG "[$pid] Table $table_name already exists, skipping create\n";
        return;
    }

    my $k1 = "cdrdatapk_" . $ts . "_k1";
    my $k2 = "cdrdatapk_" . $ts . "_k2";

    my $sql = qq{
create table if not exists $table_name (
	cdrType INT2,
	callId INT4,
	aNumber VARCHAR(16) not null,
	bNumber VARCHAR(16) not null,
	origCauseLocation INT2,
	origCauseValue INT2,
	destCauseLocation INT2,
	destCauseValue INT2,
	dateTimeConnect TIMESTAMP not null,
	dateTimeDisconnect TIMESTAMP not null,
	duration INT2 not null,
	pkid VARCHAR(200)
);

create index if not exists $k1 on $table_name(aNumber);
create index if not exists $k2 on $table_name(bNumber);
};

# we have to catch this, otherwise the application fails
# note that there are many workers, and all the rest die (on the first succeeds)
    eval {
        my $sth = $dbh->prepare($sql);
        $sth->execute;
    };

    if ($@) {

        # do nothing, but sleep a random small amount
        print LOG "[$pid] Error received creating table: $@\n";
        Time::HiRes::usleep( int( rand(15) + 1 ) * 100000 );
    }
    else {
        print LOG "[$pid] Created table:\n$sql\n";
    }
}

# create a table of the format cdrdata_part_YYYYMMDDHH as a partition of main table
sub create_partition {
    my (%part_ts) = @_;

    my @tables_to_create;

    # loop over the unique tables via timestamp and check if each table exists
    foreach my $this_ts ( sort keys %part_ts ) {
        my $table_name = "cdrdata_part_" . $this_ts;

        # first check if the table already exists
        my $sth = $dbh->prepare(
"select table_name from information_schema.tables where table_name = '$table_name'"
        );
        my $tab = 0;
        $sth->execute;
        $sth->bind_col( 1, \$tab );
        $sth->fetch;
        $sth->finish;
        if ( lc($tab) eq lc($table_name) ) {

# do nothing, printing this produces lots of noise
#print LOG "[$pid] Partition table $table_name already exists, skipping create\n";
        }
        else {
            push @tables_to_create, $table_name;
        }
    }

    # loop over all unique timestamps;
    foreach my $this_table ( sort @tables_to_create ) {
        my $k1 = $this_table . "_k1";
        my $k2 = $this_table . "_k2";

        # partition values for this table (remove all non-digits)
        if ( $this_table =~ /(\d{4})(\d{2})(\d{2})(\d{2})/ ) {
            my $table_name = $this_table;
            my $y4         = $1;
            my $m2         = $2;
            my $d2         = $3;
            my $h2         = $4;

            my $ts1 = $y4 . "-" . $m2 . "-" . $d2 . "T" . $h2 . ":00:00";
            my $ts2 = $y4 . "-" . $m2 . "-" . $d2 . "T" . $h2 . ":59:59.999";

            my $sql = qq{
		create table if not exists $table_name PARTITION OF cdrdata_part(dateTimeConnect DEFAULT '$ts1') FOR VALUES FROM ('$ts1') TO ('$ts2');

		create index if not exists $k1 on $table_name(aNumber);
		create index if not exists $k2 on $table_name(bNumber);
		};

# we have to catch this, otherwise the application fails
# note that there are many workers, and all the rest die (on the first succeeds)
            eval {
                my $sth = $dbh->prepare($sql);
                $sth->execute;
            };

            if ($@) {

                # do nothing, but sleep a random small amount
                print LOG "[$pid] Error received creating table: $@\n";
                print LOG "[$pid] Create table SQL: $sql\n";
                Time::HiRes::usleep( int( rand(15) + 1 ) * 100000 );
            }
            else {
                print LOG "[$pid] Created table:\n$sql\n";
            }
        }    # end if ($this_table..)
    }    # end foreach my $this_table
}

# create a timestamp from a UNIX time
sub mktimestamp {
    my ($unix) = @_;

    my $ts = POSIX::strftime( "%Y-%m-%dT%H:%M:%S", localtime($unix) );

    return ($ts);
}

# create a fake Philippines MIN
sub fake_min {
    my @prefixes = (
        "0813",  "0817",  "0905",  "0906",  "0907", "0908",
        "0909",  "0910",  "0912",  "0915",  "0916", "0917",
        "09173", "09175", "09176", "09178", "0918", "0919",
        "0920",  "0921",  "0922",  "0923",  "0925", "0926",
        "0927",  "0928",  "0929",  "0930",  "0932", "0933",
        "0934",  "0935",  "0936",  "0937",  "0938", "0939",
        "0942",  "0943",  "0946",  "0947",  "0948", "0949",
        "0973",  "0974",  "0975",  "0977",  "0979", "0989",
        "0994",  "0995",  "0996",  "0997",  "0998", "0999"
    );

    # select a random prefix for the fake MIN
    my $min = $prefixes[ int( rand($#prefixes) ) ];

    my $n = int( rand(7999999) + 2000000 );
    $min .= sprintf( "%7d", $n );

    return ($min);
}

# connect to database
print LOG "[$pid] Connecting to DB.. ";
$dbh = DBI->connect( "DBI:Pg:dbname=$dbname;host=$dbhost",
    $dbuser, $dbpassword, { RaiseError => 1, AutoCommit => 0 } );

print LOG "ok\n";

# blindly create parent tables
if ($PARTITION_PER_HOUR) {
    create_parent_partition();
}
else {
    create_parent_tables();
}

# repeat until interruption
my $master_count = 0;

my $batch  = 0;
my $tstart = Time::HiRes::time;

while (1) {
    my $count = 0;

    my $tstart2 = Time::HiRes::time;
    my $ts = POSIX::strftime( "%Y%m%d%H", localtime() );

    # default is to go into a single table
    my $table_name = "cdrdata";

    # if we manually have a new table per hour..
    if ($TABLE_PER_HOUR) {
        $table_name = "cdrdata_" . $ts;
    }

    if ($PARTITION_PER_HOUR) {
        $table_name = "cdrdata_part";
    }

    my $sql = qq{
INSERT INTO $table_name(cdrType, callId, aNumber, bNumber, origCauseLocation, origCauseValue,
	destCauseLocation, destCauseValue, dateTimeConnect, dateTimeDisconnect, duration, pkid)
    VALUES
};

  # because we do a batch insert, multiple partitioned tables need to be created
    my %part_timestamps;

    while ( $count < $batchSize ) {
        my $cdrType           = int( rand(3) );
        my $callId            = int( rand(32768) ) + 32767;
        my $aNumber           = fake_min();
        my $bNumber           = fake_min();
        my $origCauseLocation = int( rand(16) );
        my $destCauseLocation = int( rand(16) );
        my $origCauseValue    = int( rand(130) );
        my $destCauseValue    = int( rand(130) );

        my $TimeNumeric     = int($tstart2) + int( rand(3600) );
        my $dateTimeConnect = mktimestamp($TimeNumeric);

     # add the YYYYMMDDHH24 of $TimeNumeric to the hash (if not already present)
        my $ts_part = POSIX::strftime( "%Y%m%d%H", localtime($TimeNumeric) );
        if ( defined( $part_timestamps{$ts_part} ) ) {
            $part_timestamps{$ts_part}++;
        }
        else {
            $part_timestamps{$ts_part} = 1;
        }
        my $duration = ( int( rand(95) ) + 5 ) * 6;

        my $dateTimeDisconnect = mktimestamp( $TimeNumeric + $duration );

        # create fake UUID of 196 characters
        my $pk1 =
          Digest::MD5::md5_hex( $aNumber . $bNumber . $dateTimeConnect );
        my $pkid = $pk1 . $pk1 . $pk1 . $pk1 . $pk1 . $pk1;

        # really ugly interpolation.. SQL injection here we come!
        my $valuestr = qq{
($cdrType, $callId, '$aNumber', '$bNumber', $origCauseLocation, $origCauseValue,
 $destCauseLocation, $destCauseValue, '$dateTimeConnect', '$dateTimeDisconnect', $duration, '$pkid')
};

        # append the current value to the SQL string
        $sql .= $valuestr;

        # add trailing comma..
        if ( $count < ( $batchSize - 1 ) ) {
            $sql .= ",";
        }
        $count++;
    }

    my $sth = $dbh->prepare($sql);

    # catch errors
    my $insert_success = 0;
    while ( $insert_success == 0 ) {
        eval {
            $sth->execute;
            $dbh->commit;
        };

        # if there was a failure, try to create the table, then sleep randomly
        if ($@) {
            print LOG "[$pid] Error received inserting: $@\n";
            $dbh->rollback();
            if ($TABLE_PER_HOUR) {
                create_table();
            }

# the partition is on the dateTimeConnect table, which may or may not be "close" to the current time
            if ($PARTITION_PER_HOUR) {
                create_partition(%part_timestamps);
            }
            Time::HiRes::usleep( int( rand(15) + 1 ) * 300000 );
        }
        else {
            $insert_success++;
            $batch++;
        }
    }

    # log if it took more than one try to insert
    if ( $insert_success > 1 ) {
        print LOG "[$pid] Insert succeeded after tries: $insert_success\n";
    }

    $master_count += $count;

    # if we have 100 batches (90K rows) print a timer
    if ( $batch >= 100 ) {
        my $tTotal     = Time::HiRes::time - $tstart;
        my $rowsPerSec = ( $batch * $batchSize ) / $tTotal;
        my $ts2        = POSIX::strftime( "%Y%m%d%H%M%S", localtime() );

        print LOG "$ts2 [$pid] $tTotal ($batch * $batchSize) RPS=$rowsPerSec\n";

        $batch  = 0;
        $tstart = Time::HiRes::time;

    }

#print STDERR "[$pid] [$master_count] [$count] [$tElapsed seconds] [$rowsPerSec]\n";
}
$dbh->disconnect;
