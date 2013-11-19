#!/usr/bin/perl

use warnings;
use strict;
use lib '.';
use Redis;
use Getopt::Long;
use Try::Tiny;
use Data::Dumper;

my $from;
my $to;
my $keyfile;
my $numberOfKeys;
my $maxHashKeysMove = 1000; # too many keys from hash at once killing nutcracker
my $informEveryPercent = 5; # How often print information about progress. Integer value <0,100> (not validated)

# Parameters from, to, and file with list of keys to copy
# are required. nok is optional. It's used to estimate
# how many keys have been processed.
GetOptions (
    "from=s" => \$from,
    "to=s" => \$to, 
    "keyfile=s" => \$keyfile,
    "nok=i" => \$numberOfKeys,
    );


sub CheckRequiredParameters {
    foreach  my $argument ( @_ ) {
        if ( ! defined( $argument ) ) {
            print<<END
Usage:  --from [ip:port|/path/to/socket] --to [ip:port|/path/to/socket] --keyfile /path/to/file/with/keys [--nok numberOfKeys]
END
;
            exit 1;
        }
    }
}


sub ConnectRedis {
# addres of redis as argument neded (ip:port or socket)
    my $socket = shift; 
    my $redis;
    if ( $from =~ /^[0-9\.:]+$/ ) {
        try {
            $redis = Redis -> new( server => $socket );
        } catch {
            die "Can't connect to $socket\n";
        }
    } else {
        try {
            $redis = Redis->new( sock => "$socket" );
        } catch {
            die "Can't connect to $socket\n";
        }
    }
    return $redis;
}

CheckRequiredParameters( $from, $to, $keyfile );
my $sredis = ConnectRedis( $from );
my $dredis = ConnectRedis( $to );

if ( ! -r $keyfile ) {
   print "Can not read $keyfile.";
   exit 1;
}

open my $keys, "<", $keyfile;
$numberOfKeys = defined $numberOfKeys ? $numberOfKeys : $sredis->dbsize;
my $processed = 0;
my $notifyEvery = $numberOfKeys * $informEveryPercent/100;
my $notifyCounter = -1;
while ( my $key = <$keys> ) {
    $notifyCounter++;
    $processed++;
    if ( $notifyCounter >= $notifyEvery ) {
        printf "   *** Processed %.0f%% (%d of %d keys) ***\n", $processed/$numberOfKeys*100, $processed, $numberOfKeys;
        $notifyCounter = -1;
    }
    chomp( $key );
    my $ktype = $sredis->type( "$key" ); # differents type - different treatment.
# STRING
    if ( $ktype eq "string" ) {
        $dredis->set( $key => $sredis->get( $key ) );
    }
# HASH
    elsif ( $ktype eq "hash" ) {
        if ( $sredis->hlen( $key ) <= $maxHashKeysMove ) {
            $dredis->hmset( $key => $sredis->hgetall( $key ) );
        } else {
            print "Hash \"$key\" has ".$sredis->hlen( "$key" )." elements. It's more than treshold: $maxHashKeysMove. Moving partially... ";
            my @hkeys = $sredis -> hkeys( $key );
            while ( my @hpart = splice @hkeys, 0, $maxHashKeysMove ) {
                my %hash;
                print ".";
                # concantenating two arrays into one hash with key from the first one
                @hash{@hpart} = $sredis->hmget( $key, @hpart );
                $dredis->hmset( $key,  %hash );
            }
            print "Done\n";
            print STDERR "[CRITICAL] Length of hash \"$key\" on source and destination not the same\n" if ( $dredis->hlen( "$key" ) != $sredis->hlen( "$key" ) );
        }
    }
# LIST
    elsif ( $ktype eq "list" ) {
        $dredis->del( $key ) if $dredis->exists( $key );
        $dredis->rpush( $key, $sredis->lrange( $key, 0, -1 ) );
        print STDERR "[CRITICAL] Length of list \"$key\" on source and destination not the same\n" if ( $dredis->llen( "$key" ) != $sredis->llen( "$key" ) );
    }
# SET
    elsif ( $ktype eq "set" ) {
        my @members = $sredis->smembers( $key );
        while ( my $member = shift @members ) {
            $dredis->sadd( $key, $member );
        }
        print "[CRITICAL] Length of set \"$key\" on source and destination not the same\n" if ( $dredis->scard( "$key" ) != $sredis->scard( "$key" ) );
    }
# ZSET
    elsif ( $ktype eq "zset" ) {
        my @zrangelist = $sredis->zrange( $key, 0, -1, "WITHSCORES" );
        while ( my ( $k, $v ) = splice @zrangelist, 0, 2 ) {
           $dredis->zadd( $key, $v, $k );
        }
    }
##### not processed :(
    else {
        if ( ! $sredis->exists( $key ) ) {
            print STDERR "[CRITICAL] Key \"$key\" was not copied because were not found on source redis.\n";
        } else {
            print STDERR "[CRITICAL] Key \"$key\" with type \"$ktype\" was not copied because is not supported yet.\n";
        }
    }
# TTL
    my $kttl = $sredis->ttl( $key );
    $dredis->expire( $key, $kttl ) if ( $kttl != -1 );
}

# vim:ts=4 et softtabstop=4 sw=4 hlsearch nu
