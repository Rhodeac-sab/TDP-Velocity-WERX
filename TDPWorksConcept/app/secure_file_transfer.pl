#!/usr/bin/perl
use strict;
use warnings;
use Crypt::CBC;
use File::Copy;

my $key = "SuperSecretKey"; # Change for security
my $cipher = Crypt::CBC->new(-key => $key, -cipher => "Crypt::AES");

opendir(DIR, "/staging") or die "Cannot open directory: $!";
while (my $file = readdir(DIR)) {
    next if ($file =~ /^\./);
    my $input = "/staging/$file";
    my $output = "/data_warehouse1/$file.enc";
    
    open my $in, '<', $input or next;
    open my $out, '>', $output or next;
    print $out $cipher->encrypt(<$in>);
    close $in;
    close $out;
    
    unlink $input;
    print "Encrypted and moved: $file\n";
}
closedir(DIR);
