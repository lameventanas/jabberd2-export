#!/usr/bin/perl
#
# Export jabberd2 data to XEP-0227 format
#
# Tested only once to migrate jabberd 2.6.1 (using MySQL) to prosody 0.10.0
# Should work with Postgesql or other storage

=head1 NAME

jabberd2-export - Export jabberd2 data to XEP 0227 format

=head1 SYNOPSIS

jabberd2-export [OPTIONS] [sm.xml] [out.xml]

=head1 OPTIONS

=over 4

=item B<--verbose>

=item B<--verbose>=x

Set verbosity level (up to 2)

=item B<--skip-domains=x>

Comma-separated list of domains to skip from export

=item B<--skip-users=x>

Comma-separated list of users to skip from export (specifiy with or without domain)

=item B<--random-passwords=yes|no|empty>

Replace passwords with random ones: yes, no or only when empty

=back

=head1 SEE ALSO

=over 4

=item L<XEP 0227|http://xmpp.org/extensions/xep-0227.html>

=back

=head1 AUTHOR

Alan Mizrahi, alan at mizrahi dot com dot ve

Feel free to contact me if you have any comments or find this program useful.

=cut

use strict;
use warnings;
use Data::Dumper;
$Data::Dumper::Sortkeys = 1;
$Data::Dumper::Indent   = 3;
use Getopt::Long;
use Pod::Usage qw(pod2usage);
use Pod::Man;
use DBI;
use XML::XPath;
use XML::XPath::Node; # for node type constants
use XML::Writer;

my $verbose        = 0;
my $skipUsers      = '';
my $skipDomains    = '';
my $sm_fn          = '/etc/jabberd2/sm.xml';
my $out_fn;
my $opt_randomPass = 'no';

GetOptions('help' => sub { $verbose=2; showUsage(0) }, 'debug|verbose:+' => \$verbose, 'skip-users=s' => \$skipUsers, 'skip-domains=s' => \$skipDomains, 'random-passwords=s' => \$opt_randomPass) or showUsage(1);

# Consume remaining args
$sm_fn  = shift(@ARGV) if $#ARGV > -1;
$out_fn = shift(@ARGV) if $#ARGV > -1;
showUsage(1) if $#ARGV > -1;

my $output = *STDOUT;
if (defined $out_fn) {
	open($output, '>', $out_fn) or die "Could not open $out_fn: $!";
}

# convert strings to hashes
$skipUsers   = { map { $_ => 1 } split(',', $skipUsers) };
$skipDomains = { map { $_ => 1 } split(',', $skipDomains) };

# validate random-passwords option
$opt_randomPass =~ /^(yes|no|empty)$/ or showUsage(1, "Invalid --random-pass setting: $opt_randomPass");

my $xp = XML::XPath->new(filename => $sm_fn);
my $driver = $xp->getNodeText('/sm/storage/driver')->value();

die "Could not find storage driver in $sm_fn:/sm/storage/driver" if $driver eq '';

$driver = 'pg' if $driver =~ /^pg/; # PostgreSQL DBD module is 'pg'

my $db;
foreach my $x ('host', 'port', 'dbname', 'user', 'pass') {
	$db->{$x} = $xp->getNodeText("/sm/storage/$driver/$x")->value();
	die "Could not find $x in $sm_fn:/sm/storage/$driver/$x" if $db->{$x} eq '';
}
print STDERR "db:\n", Dumper($db) if $verbose > 1;

my $db_dsn = sprintf('dbi:%s:%s:%s:%s', $driver, $db->{dbname}, $db->{host}, $db->{port});

# Database handles
my ($dbh,$sth,$row);

$dbh = DBI->connect($db_dsn, $db->{user}, $db->{pass}, { PrintError => 0, RaiseError => 1 });

# Workaround for my tables stored using latin1 in MySQL:
$dbh->do('SET NAMES utf8') if $db_dsn =~ /mysql/i;

# 4.2 Users

my $users;
# useful when users are stored in LDAP or some other storage other than the DB
# get active users into realm => user hash
# active: collection-owner, object-sequence, time
$sth = $dbh->prepare("select * from active");
$sth->execute;
my $active;
while ($row = $sth->fetchrow_hashref) {
	my ($user, $realm) = parseJID($row->{'collection-owner'}) or die "Could not parse jid: ".$row->{'collection-owner'};
	$users->{$realm}->{$user} = {};
}
$sth->finish;

# password info (could be stored somewhere else)

# authreg: username, realm, password, token, sequence, hash
$sth = $dbh->prepare("select * from authreg");
$sth->execute;
while (my $row = $sth->fetchrow_hashref) {
	$users->{$row->{'realm'}}->{$row->{'username'}}->{'password'} = $row->{'password'};
}
$sth->finish;

# 4.3 Rosters

my $roster_groups; # realm => user => contact => [groups]
my $rosters;

# roster-groups: collection-owner, object-sequence, jid, group
$sth = $dbh->prepare("select * from `roster-groups`");
$sth->execute;
while ($row = $sth->fetchrow_hashref) {
	my ($user, $realm) = parseJID($row->{'collection-owner'}) or die "Could not parse jid: ".$row->{'collection-owner'};
	push(@{$roster_groups->{$realm}->{$user}->{$row->{'jid'}}}, $row->{'group'});
}
$sth->finish;
print STDERR "roster-groups:\n", Dumper($roster_groups) if $verbose > 1;

# used to convert "$to$from" to subscription name
my %subsName = ('00' => 'none', '01' => 'from', '10' => 'to', '11' => 'both');
$sth = $dbh->prepare("select * from `roster-items` where ask = 0");
$sth->execute;
while ($row = $sth->fetchrow_hashref) {
	my ($user, $realm) = parseJID($row->{'collection-owner'}) or die "Could not parse jid: ".$row->{'collection-owner'};
	
	my $contact = $row->{'jid'};
	my $subscription = $subsName{$row->{'to'}.$row->{'from'}};
	
	$rosters->{$realm}->{$user}->{$contact} = {
		'subscription' => $subscription,
		'name'         => $row->{'name'},
		'groups'       => [ ]
	};
	$rosters->{$realm}->{$user}->{$contact}->{'groups'} = $roster_groups->{$realm}->{$user}->{$contact} if defined $roster_groups->{$realm}->{$user}->{$contact};
}
print STDERR "rosters:\n", Dumper($rosters) if $verbose > 1;
$sth->finish;

# 4.4 and 4.8: Offline Messages and Incoming Subscription Requests

my $offline_messages; # realm => user => xml
my $incoming_subs; # realm => user => xml
# queue: collection-owner, object-sequence, xml
$sth = $dbh->prepare("select * from queue order by `object-sequence`");
$sth->execute;
while (my $row = $sth->fetchrow_hashref) {
	my ($user, $realm) = parseJID($row->{'collection-owner'}) or die "Could not parse jid: ".$row->{'collection-owner'};
	$row->{'xml'} =~ /^(?:NAD)?(.+)$/; # remove the "NAD" prefix
	
	my $node = parseNADSingleNode($1);
	my $nodeName = $node->getName();
	
	if ($nodeName eq 'presence') {
		push(@{$incoming_subs->{$realm}->{$user}}, $node->toString);
	} elsif ($nodeName eq 'message') {
		push(@{$offline_messages->{$realm}->{$user}}, $node->toString);
	}
}
$sth->finish;
print STDERR "offline_messages:\n", Dumper($offline_messages) if $verbose > 1;
print STDERR "incoming_subs:\n", Dumper($incoming_subs) if $verbose > 1;


# 4.5 Private XML Storage
my $private; # realm => user => xml
$sth = $dbh->prepare("select * from private");
$sth->execute;
while (my $row = $sth->fetchrow_hashref) {
	my ($user, $realm) = parseJID($row->{'collection-owner'}) or die "Could not parse jid: ".$row->{'collection-owner'};
	$row->{'xml'} =~ /^(?:NAD)?(.+)$/; # remove the "NAD" prefix
	
	my $node = parseNADSingleNode($1);
	my $nodeName = $node->getName();
	
	$private->{$realm}->{$user}->{$row->{'ns'}} = $node->toString;
}
$sth->finish;
print STDERR "private:\n", Dumper($private) if $verbose > 1;

# 4.6 vCards

my $vcard; # realm => user => vcard
$sth = $dbh->prepare("select * from vcard");
$sth->execute;
while (my $row = $sth->fetchrow_hashref) {
	my ($user, $realm) = parseJID($row->{'collection-owner'}) or die "Could not parse jid: ".$row->{'collection-owner'};
	foreach my $field (keys %{$row}) {
		next if $field =~ /^(collection-owner|object-sequence)$/ or !defined $row->{$field};
		$vcard->{$realm}->{$user}->{$field} = $row->{$field};
	}
}
$sth->finish;
print STDERR "vcard:\n", Dumper($vcard) if $verbose > 1;


# 4.7 Privacy Lists

my $privacy; # realm => user => listname => [ { type => jid|group|subscription, value => <string> , action => allow|deny, order => <num>, block => { message => 0|1, presence-in => 0|1, presence-out => 0|1, iq => 0|1 } } ]
$sth = $dbh->prepare("select * from `privacy-items` order by `collection-owner`, list, `order`");
$sth->execute;

# this helps convert block bitwise column to hash
# bits:
# 0x0 = none
# 0x1 = message
# 0x2 = presence-in
# 0x4 = presence-out
# 0x8 = iq
my %blockBits = ( 0x1 => 'message', 0x2 => 'presence-in', 0x4 => 'presence-out', 0x8 => 'iq');
while (my $row = $sth->fetchrow_hashref) {
	my ($user, $realm) = parseJID($row->{'collection-owner'}) or die "Could not parse jid: ".$row->{'collection-owner'};
	
	my $item = {
		'value'  => $row->{'value'},
		'action' => ($row->{'deny'}?'deny':'allow'),
		'order'  => $row->{'order'},
		'block'  => {},
	};
	
	$item->{'type'} = $row->{'type'} if defined $row->{'type'}; # null type = fall-through in the spec (eg: all)
	
	foreach my $bit (keys %blockBits) {
		$item->{'block'}->{$blockBits{$bit}} = (defined($row->{'block'}) && ($row->{'block'} & $bit))?1:0;
	}
	
	push(@{$privacy->{$realm}->{$user}->{$row->{'list'}}}, $item);
}
$sth->finish;
print STDERR "privacy:\n", Dumper($privacy) if $verbose > 1;

my $privacy_default; # realm => user => default-listname
$sth = $dbh->prepare("select * from `privacy-default`");
$sth->execute;
while (my $row = $sth->fetchrow_hashref) {
	my ($user, $realm) = parseJID($row->{'collection-owner'}) or die "Could not parse jid: ".$row->{'collection-owner'};
	
	$privacy_default->{$realm}->{$user} = $row->{'default'};
}
$sth->finish;
print STDERR "privacy_default:\n", Dumper($privacy_default) if $verbose > 1;


# 4.8 Incoming Subscription Requests
# these are stored in queue if the user is offline at the time of a subscription request
# once the message is delivered (but still not answered), they aren't anywhere in the database
# nothing to do here?

$dbh->disconnect;


# Build XML

my $w = XML::Writer->new(OUTPUT => $output, NEWLINES => 0, DATA_MODE => 1, DATA_INDENT => 4, ENCODING => 'utf-8');

$w->xmlDecl();

# the specs say to use this:
# $w->startTag('server-data', 'xmlns' => 'urn:xmpp:pie:0');
# but xep227toprosody.lua needs this namespace:
$w->startTag('server-data', 'xmlns' => 'http://www.xmpp.org/extensions/xep-0227.html#ns');

REALM:
foreach my $realm (sort keys %{$users}) {
	if (defined $skipDomains->{$realm}) {
		print STDERR "Skipping domain: $realm\n";
		next REALM;
	}
	print STDERR "Exporting domain $realm\n" if $verbose > 0;
	$w->startTag('host', 'jid' => $realm);
	USER:
	foreach my $user (sort keys %{$users->{$realm}}) {
		if (defined $skipUsers->{"$user\@$realm"} || defined $skipUsers->{$user}) {
			print STDERR "Skipping user: $user\@$realm\n";
			next USER;
		}
		print STDERR "Exporting user $user\n" if $verbose > 0;
		
		my $password = $users->{$realm}->{$user}->{'password'};
		
		if (
			($opt_randomPass eq 'yes') ||
			(($opt_randomPass eq 'empty') && (!defined($password) || ($password eq '')))
		) {
			$users->{$realm}->{$user}->{'password'} = randomPassword();
		}
		
		$w->startTag('user', 'name' => $user, %{$users->{$realm}->{$user}});

		# 4.3 Rosters
		if (defined $rosters->{$realm}->{$user}) {
			$w->startTag('query', 'xmlns' => 'jabber:iq:roster');
			foreach my $contactJID (sort keys %{$rosters->{$realm}->{$user}}) {
				my $contact = $rosters->{$realm}->{$user}->{$contactJID};
				my %attrs = ('jid' => $contactJID, map { $_ => $contact->{$_} } grep { ! ($_ eq 'groups') && defined($contact->{$_}) } sort keys %{$contact} );
				
				$w->startTag('item', %attrs ) ; 

				# Groups for this contact
				foreach my $groupName (sort @{$contact->{'groups'}}) {
					$w->startTag('group');
					$w->characters($groupName);
					$w->endTag('group');
				}
				$w->endTag('item');
				
			}
			$w->endTag('query');
		}
		
		# 4.4 Offline Messages
		if (defined $offline_messages->{$realm}->{$user}) {
			$w->startTag('offline-messages');
			foreach my $msg (@{$offline_messages->{$realm}->{$user}}) {
				print "\n",' 'x(4*$w->getDataIndent());
				print $msg;
				print "\n";
			}
			$w->endTag('offline-messages');
		}
		
		# 4.5 Private XML Storage
		if (defined $private->{$realm}->{$user}) {
			$w->startTag('query', 'xmlns' => 'jabber:iq:private');
			foreach my $ns (sort keys %{$private->{$realm}->{$user}}) {
				$w->startTag('x', 'xmlns' => $ns);
				
				print "\n",' 'x(5*$w->getDataIndent());
				print $private->{$realm}->{$user}->{$ns};
				print "\n";
				
				$w->endTag('x');
			}
			$w->endTag('query');
		}
		
		# 4.6 vCard
		if (defined $vcard->{$realm}->{$user}) {
			$w->startTag('vCard', 'xmlns' => 'vcard-temp');
			foreach my $field (sort keys %{$vcard->{$realm}->{$user}}) {
				my $value = $vcard->{$realm}->{$user}->{$field};
				$w->startTag($field);
				if ($value =~ /[\r\n]/) {
					$w->cdata($vcard->{$realm}->{$user}->{$field});
				} else {
					$w->characters($vcard->{$realm}->{$user}->{$field});
				}
				$w->endTag($field);
			}
			$w->endTag('vCard');
		}
		
		# 4.7 Privacy Lists
		
		if (defined $privacy->{$realm}->{$user}) {
			$w->startTag('query', 'xmlns' => 'jabber:iq:privacy');
			
			# default list
			if (defined $privacy_default->{$realm}->{$user}) {
				$w->emptyTag('default', 'name' => $privacy_default->{$realm}->{$user});
			}
			
			foreach my $list (sort keys %{$privacy->{$realm}->{$user}}) {
				$w->startTag('list', 'name' => $list);
				foreach my $item (@{$privacy->{$realm}->{$user}->{$list}}) {
					if (scalar(keys %{$item->{'block'}}) == 0) {
						# no graunlar block control
						$w->emptyTag('item', map { $_ => $item->{$_} } grep { ! ($_ eq 'block')} sort keys %{$item} );
					} else {
						# granular block control
						$w->startTag('item', map { $_ => $item->{$_} } grep { ! ($_ eq 'block')} sort keys %{$item} );
						$w->endTag('item');
					}
				}
				$w->endTag('list');
			}
			$w->endTag('query');
		}
		
		# 4.8 Incoming Subscription Requests
		foreach my $presence (@{$incoming_subs->{$realm}->{$user}}) {
			print "\n",' 'x(3*$w->getDataIndent());
			print $presence;
			print "\n";
		}
		
		
		$w->endTag('user');
		
	}
	$w->endTag('host');
}

$w->endTag('server-data');
$w->end();

exit 0;

# parse user@domain, return arrayref: [user, domain]
sub parseJID {
	return ($1, $2) if $_[0] =~ /^(.+?)@([^@]+)$/;
	return undef;
}

# arg: NAD<route>xml</route>
# returns: XML Node (call toString to stringify it)
sub parseNADSingleNode {
	my ($s) = @_;

	my $xp = XML::XPath->new(xml => $s);
	my @nodes = $xp->findnodes("/*/*");
	die "Unexpected number of child nodes found in NAD string: ".scalar(@nodes)."\n$s" unless $#nodes == 0;
	die "Unexpected node type found in XML in NAD string:\n$s" unless $nodes[0]->getNodeType == ELEMENT_NODE;

	return $nodes[0];
}

sub randomPassword {
	my $ret = '';
	my @set = ('A'..'Z', 'a'..'z', '0'..'9', '#', '$', '%', '&','*', '+', ',', '-', '.', ':', ';','=', '_', '~');
	foreach (1 .. 8) {
		$ret .= $set[rand($#set)];
	}
	return $ret;
}

sub showUsage {
	my ($code, $msg) = @_;
	pod2usage(-msg => $msg, -exitval => $code, -verbose => $verbose, -output => $code?*STDERR:*STDOUT, -noperldoc => 1);
}
