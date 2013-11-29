package App::DBI::Loader;

use strict;
use warnings;

# ABSTRACT: A tiny script to load CSV/TSV contents into a database table via DBI
# VERSION

use Getopt::Std;
use Getopt::Config::FromPod;
use Pod::Usage;

use DBI;
use String::Unescape;

sub run
{
	shift if @_ && eval { $_[0]->isa(__PACKAGE__) };
	local (@ARGV) = @_;

	my %opts;
	getopts(Getopt::Config::FromPod->string, \%opts);
	pod2usage(-verbose => 2) if exists $opts{h};
	pod2usage(-msg => 'At least 3 arguments MUST be specified', -verbose => 0, -exitval => 1) if @ARGV < 3;

	$opts{t} ||= '';
	my $sep = String::Unescape->unescape($opts{t}) || ',';

	my $dbstr = shift @ARGV;
	my $table = shift @ARGV;

	my $dbh = DBI->connect($dbstr, $opts{u} || '', $opts{p} || '') or die;
	my $has_transaction = 1;
	eval { $dbh->{AutoCommit} = 0 };
	$has_transaction = 0 if $@;
	if($ARGV[0] =~ /\(.*\)/) {
		my $schema = shift @ARGV;
		$dbh->do("DROP TABLE IF EXISTS $table");
		$dbh->do("CREATE TABLE $table $schema");
	}
	if(exists $opts{c}) {
		$dbh->do("DELETE FROM $table");
	}
	my $sth;

	$dbh->begin_work if $has_transaction;
	while(my $file = shift @ARGV) {
		open my $fh, '<', $file or die;
		while(<$fh>) {
			s/[\r\n]+$//;
			my (@t) = $sep ? split /$sep/ : $_;
			$sth ||= $dbh->prepare('INSERT INTO '.$table.' VALUES ('.join(',', ('?')x @t).')');
			$sth->execute(@t);
		}
	}
	$dbh->commit if $has_transaction;
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
