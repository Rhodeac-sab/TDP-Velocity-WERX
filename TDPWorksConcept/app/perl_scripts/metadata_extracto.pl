#!/usr/bin/perl
use strict;
use warnings;
use File::stat;
use JSON;
use DBI;

my $dbh = DBI->connect("DBI:mysql:database=file_tracking;host=mysql", "tdp_admin", "SSTS_2025!@", {'RaiseError' => 1});
opendir(DIR, "/staging") or die "Cannot open directory: $!";
while (my $file = readdir(DIR)) {
    next if ($file =~ /^\./);
    my $path = "/staging/$file";
    my $stat = stat($path);
    my %metadata = (
        filename => $file,
        size => $stat->size,
        modified => scalar localtime $stat->mtime,
    );

    my $json_metadata = encode_json(\%metadata);
    $dbh->do("INSERT INTO file_metadata (filename, metadata) VALUES (?, ?)", undef, $file, $json_metadata);
    print "Metadata extracted: $file\n";
}
closedir(DIR);
$dbh->disconnect;
