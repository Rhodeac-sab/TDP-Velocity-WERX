#!/usr/bin/perl
use strict;
use warnings;
use Digest::SHA qw(sha256_hex);
use Digest::MD5 qw(md5_hex);
use DBI;
use File::Basename;

my $dbh = DBI->connect("DBI:mysql:database=file_tracking;host=mysql", "tdp_admin", "SSTS_2025!@", {'RaiseError' => 1});
opendir(DIR, "/staging") or die "Cannot open directory: $!";
while (my $file = readdir(DIR)) {
    next if ($file =~ /^\./); # Skip hidden files
    my $path = "/staging/$file";
    open my $fh, '<', $path or next;
    binmode($fh);
    my $sha256 = sha256_hex(<$fh>);
    my $md5 = md5_hex(<$fh>);
    close $fh;

    my $sth = $dbh->prepare("SELECT * FROM file_hashes WHERE sha256 = ?");
    $sth->execute($sha256);
    if ($sth->fetchrow_array) {
        print "Duplicate detected: $file\n";
        next;
    }

    $dbh->do("INSERT INTO file_hashes (filename, sha256, md5) VALUES (?, ?, ?)", undef, $file, $sha256, $md5);
    print "Processed: $file\n";
}
closedir(DIR);
$dbh->disconnect;
