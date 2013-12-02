use Test::More tests => 14;
use Test::Exception;

use FindBin;
use DBI;
use Getopt::Config::FromPod;

Getopt::Config::FromPod->set_class_default(-file => "$FindBin::Bin/../bin/dbiloader");

use_ok 'App::DBI::Loader';

sub execute
{
	pipe(FROM_PARENT, TO_CHILD);
	my $pid = fork;
	die "fork failed: $!" if ! defined($pid);
	if($pid) {
		close FROM_PARENT;
		open my $fh, '<', $_[1];
		local $/;
		my $dat = <$fh>;
		close $fh;
		print TO_CHILD $dat;
		close TO_CHILD;
		waitpid $pid, 0;
	} else {
		close TO_CHILD;
		# Need to close first, at least, on Win32
		close STDIN;
		open STDIN, "<&FROM_PARENT";
		App::DBI::Loader->run(@{$_[0]});
		close FROM_PARENT;
		exit;
	}
}


lives_ok { execute(['dbi:DBM:', 'test', '(id INTEGER PRIMARY KEY, value INTEGER)', '-'], "$FindBin::Bin/dat.csv"); }, 'load with default';

{
    my $dbh = DBI->connect('dbi:DBM:', '', '');
    is($dbh->selectrow_arrayref('SELECT value FROM test WHERE id = 5')->[0], 80, 'lookup');
}

lives_ok { execute(['-c', '-t', '\t', 'dbi:DBM:', 'test'], "$FindBin::Bin/dat.tsv"); }, 'load with -t and -c';

{
    my $dbh = DBI->connect('dbi:DBM:', '', '');
    is($dbh->selectrow_arrayref('SELECT value FROM test WHERE id = 5')->[0], 20, 'lookup');
}

lives_ok { execute(['-t', '\\\\s+', 'dbi:DBM:', 'test', '-'], "$FindBin::Bin/dat.ssv"); }, 'append with -t';

{
    my $dbh = DBI->connect('dbi:DBM:', '', '');
    is($dbh->selectrow_arrayref('SELECT value FROM test WHERE id = 4')->[0], 50, 'lookup1');
    is($dbh->selectrow_arrayref('SELECT value FROM test WHERE id = 5')->[0], 20, 'lookup2');
}

lives_ok { execute(['-c', '-t', '\\\\s+', 'dbi:DBM:', 'test', "$FindBin::Bin/dat.tsv", '-'], "$FindBin::Bin/dat.ssv"); }, 'load multiple 1 with -t and -c';

{
    my $dbh = DBI->connect('dbi:DBM:', '', '');
    is($dbh->selectrow_arrayref('SELECT value FROM test WHERE id = 4')->[0], 50, 'lookup1');
    is($dbh->selectrow_arrayref('SELECT value FROM test WHERE id = 5')->[0], 20, 'lookup2');
}

lives_ok { execute(['-c', '-t', '\\\\s+', 'dbi:DBM:', 'test', '-', "$FindBin::Bin/dat.ssv", '-'], "$FindBin::Bin/dat.tsv"); }, 'load multiple 2 with -t and -c';

{
    my $dbh = DBI->connect('dbi:DBM:', '', '');
    is($dbh->selectrow_arrayref('SELECT value FROM test WHERE id = 4')->[0], 50, 'lookup1');
    is($dbh->selectrow_arrayref('SELECT value FROM test WHERE id = 5')->[0], 20, 'lookup2');
}

# cleanup

END {
    my $dbh = DBI->connect('dbi:DBM:', '', '');
    $dbh->do('DROP TABLE test');
}
