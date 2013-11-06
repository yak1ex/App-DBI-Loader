package App::DBI::Loader;

use strict;
use warnings;

# ABSTRACT: A tiny script to load CSV/TSV contents into a database table via DBI
# VERSION

use Getopt::Std;
use Getopt::Config::FromPod;
use Pod::Usage;

use DBI;

sub run
{
	shift if @_ && eval { $_[0]->isa(__PACKAGE__) };
	local (@ARGV) = @_;

	my %opts;
	getopts(Getopt::Config::FromPod->string, \%opts);
	pod2usage(-verbose => 2) if exists $opts{h};
	pod2usage(-msg => 'At least 3 arguments MUST be specified', -verbose => 0, -exitval => 1) if @ARGV < 3;

	# FIXME: Should avoid stringy eval
	$opts{t} ||= '';
	my $sep = eval("\"$opts{t}\"") || ','; ## no critic (ProhibitStringyEval)

	my $dbstr = shift @ARGV;
	my $table = shift @ARGV;

	my $dbh = DBI->connect($dbstr) or die;
	if($ARGV[0] =~ /\(.*\)/) {
		my $schema = shift @ARGV;
		$dbh->do("DROP TABLE IF EXISTS $table");
		$dbh->do("CREATE TABLE $table $schema");
	}
	my $sth;

	$dbh->begin_work;
	while(my $file = shift @ARGV) {
		open my $fh, '<', $file or die;
		while(<$fh>) {
			s/[\r\n]+$//;
			my (@t) = $sep ? split /$sep/ : $_;
			$sth ||= $dbh->prepare('INSERT INTO '.$table.' VALUES ('.join(',', '?'x @t).')');
			$sth->execute(@t);
		}
	}
	$dbh->commit;
}

1;
__END__

=head1 SYNOPSIS

  App::DBI::Loader->run(@ARGV);

=head1 DESCRIPTION

This is an implementation module for a tiny script to load CSV/TSV contents into a database table via DBI.

=method C<run(@arg)>

Process arguments. Typically, C<@ARGV> is passed. For details, see L<dbiloader>.

=for :list
* L<dbiloader>

=cut
