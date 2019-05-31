#!/usr/bin/perl -w

use strict;
use POSIX;
use Digest::MD5;
use DBI;
use Time::HiRes;
use IO::Handle;



# randomly select an aNumber from the (potentially very large) number of tables in the system
# this simulates a CRM end-user looking for a specific phone number, and is a straight select across one
# table (or a group of sub-partitions or sharded tables) - this version does a UNION ALL across all tables
#my $select_mode = "TABLE";

# if $select_mode = "PARTITION" then we use the partitioned table
my $select_mode = "PARTITION";

# define database host and connection handle
my $dbhost = "myauroraor.cpxdnazmxxdl.us-west-2.rds.amazonaws.com";
my $dbname = "myauroraor";
my $dbuser = "orly";
my $dbpassword = "welcome1";


# number of parallel tasks to do; the design problem requires around 
# 100K queries per day only so that's 2 TPS and assume a peak of 20 TPS
# change these parameters to suit your own workload
my $numWorkers = 32;
my $sleepTime = 3;

# create the sub-processes
my $pc = 0;
my @pids;
my $mypid;
while ($pc < $numWorkers) {
	$mypid = fork();
	if ($mypid == 0) {
		last;
	} else {
		push @pids, $mypid;
	}
	$pc++;
}

# reap the children to avoid zombies
if ($mypid != 0) {
	for (@pids) {
		my $thispid = $_;
		waitpid($thispid, WNOHANG);
	}
	exit(0);
}

# randomly sleep to prevent "thundering herd" problem
sleep(int(rand(15)+1));
my $pid = POSIX::getpid();

my $dbh;

# open an append-only connection to the log
open (LOG, ">>/tmp/select.log") or die;
LOG->autoflush(1);

# create a fake Philippines MIN
sub fake_min {
	my @prefixes = ( 
	"0813",
	"0817",
	"0905",
	"0906",
	"0907",
	"0908",
	"0909",
	"0910",
	"0912",
	"0915",
	"0916",
	"0917",
	"09173",
	"09175",
	"09176",
	"09178",
	"0918",
	"0919",
	"0920",
	"0921",
	"0922",
	"0923",
	"0925",
	"0926",
	"0927",
	"0928",
	"0929",
	"0930",
	"0932",
	"0933",
	"0934",
	"0935",
	"0936",
	"0937",
	"0938",
	"0939",
	"0942",
	"0943",
	"0946",
	"0947",
	"0948",
	"0949",
	"0973",
	"0974",
	"0975",
	"0977",
	"0979",
	"0989",
	"0994",
	"0995",
	"0996",
	"0997",
	"0998",
	"0999" );
	
	# select a random prefix for the fake MIN
	my $min = $prefixes[int(rand($#prefixes))];

	my $n = int(rand(7999999) + 2000000);
	$min .= sprintf("%7d", $n);

	return ($min);
}

# fetch the table names and dynamically create the SQL statement
sub create_sql_table {
	my ($d, $aNum) = @_;	# must pass in the db handle

	my $sth = $d->prepare("select table_name from information_schema.tables where table_name like 'cdrdata_201%' order by table_name");
	$sth->execute();

	my $this_table;
	my $sql = "select anumber, bnumber from ";
	$sth->bind_col(1, \$this_table);
	my $count = 0;
	while ($sth->fetch) {
		if ($count == 0) {
			# first fetch
			$sql .= "$this_table where anumber='$aNum' "
		} else {
			$sql .= " union all select anumber, bnumber from $this_table where anumber='$aNum' "
		}
		$count++;
	}
	$sth->finish;
	return ($sql);
}


# connect to database
print LOG "[$pid] Connecting to DB.. ";
$dbh = DBI->connect("DBI:Pg:dbname=$dbname;host=$dbhost",
	$dbuser, $dbpassword, { RaiseError => 1, AutoCommit => 0 });

print LOG "ok\n";

# repeat until interruption
my $master_count = 0;

while (1) {
	my $tstart = Time::HiRes::time;

	my $sub_count = 0;

	# if we use the partitioned table, we can pre-prepare the statement
	my $sth;
	if ($select_mode eq "PARTITION") {
		$sth = $dbh->prepare("SELECT anumber, bnumber FROM cdrdata_part WHERE anumber = ?");
	}
	while ($sub_count < 100) {
		# create a random aNumber for the query
		my $aNumber = fake_min();

		# create the SQL statement dynamically if using the per-table explicit partitioning
		if ($select_mode eq "TABLE") {
			my $sql_text = create_sql_table($dbh, $aNumber);
			$sth = $dbh->prepare($sql_text);
			$sth->execute();
		} else {
			$sth->bind_param(1, $aNumber);
			$sth->execute();
		}
		my $rows_returned = 0;
		while (my @row = $sth->fetchrow_array()) {
			# do nothing
			$rows_returned++;
		}

		if ($select_mode eq "TABLE") {
			$sth->finish;
		}
		$sub_count++;
	}
	if ($select_mode eq "PARTITION") {
		$sth->finish;
	}
	my $tElapsed = Time::HiRes::time - $tstart;
    my $ts = POSIX::strftime("%Y%m%d%H%M%S", localtime());

	print LOG "$ts [$pid] $tElapsed ($sub_count)\n";
}
$dbh->disconnect;