#!/usr/bin/perl

use warnings;
use strict;
use lib '.';
use Redis;
use Getopt::Long;
use Try::Tiny;
use Data::Dumper;
use Time::HiRes qw/ gettimeofday tv_interval /;
use Sys::Syslog;

my $from;
my $to;
my $keyfile;
my $numberOfKeys;
my $maxHashKeysMove = 10_000; # too many keys from hash at once killing nutcracker
my $informEveryPercent = 2; # How often print information about progress. Integer value <0,100> (not validated)
my $t;                      # Microtime
my $stats;

# Parameters from, to, and file with list of keys to copy
# are required. nok is optional. It's used to estimate
# how many keys have been processed.
GetOptions (
    "from=s" => \$from,
    "to=s" => \$to, 
    "keyfile=s" => \$keyfile,
    "nok=i" => \$numberOfKeys,
    "stats" => \$stats,
    );

my %benchmark;
openlog("redis-copy", "ndelay", "local0");
open LOGFILE, ">>", "redis-copy.log";

sub CheckRequiredParameters {
    foreach  my $argument ( @_ ) {
        if ( ! defined( $argument ) ) {
            print<<END
Usage:  --from [ip:port|/path/to/socket] --to [ip:port|/path/to/socket] --keyfile /path/to/file/with/keys [--nok numberOfKeys] [--stats]
END
;
            exit 1;
        }
    }
}

sub LOG {
    syslog( 'info', @_ );
    print LOGFILE "@_", "\n";
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
            LOG "Can't connect to $socket\n";
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
    next if $key =~ m/^admin./;
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
        $t = [gettimeofday] if ( $stats );
        try{ $dredis->set( $key => $sredis->get( $key ) ); } catch { LOG("[CRITICAL] key: \"$key\" not moved"); };
        $benchmark{'string'}{'count'}++ if ( $stats );
        $benchmark{'string'}{'time'}+= tv_interval($t) if ( $stats );
    }
# HASH
    elsif ( $ktype eq "hash" ) {
        $t = [gettimeofday] if ( $stats );
        if ( $sredis->hlen( $key ) <= $maxHashKeysMove ) {
            try { $dredis->hmset( $key => $sredis->hgetall( $key ) ); } catch { LOG("[CRITICAL] key: \"$key\" not moved"); };
        } else {
            print "Hash \"$key\" has ".$sredis->hlen( "$key" )." elements. It's more than treshold: $maxHashKeysMove. Moving partially";
            my @hkeys = $sredis -> hkeys( $key );
            while ( my @hpart = splice @hkeys, 0, $maxHashKeysMove ) {
                my %hash;
                print ".";
                # concantenating two arrays into one hash with key from the first one
                @hash{@hpart} = $sredis->hmget( $key, @hpart );
                try { $dredis->hmset( $key,  %hash ); } catch { LOG("[CRITICAL] key: \"$key\" not moved"); };
            }
            print " Done\n";
            LOG ("[CRITICAL] Length of hash \"$key\" on source and destination not the same\n" ) if ( $dredis->hlen( "$key" ) != $sredis->hlen( "$key" ) );
        }
        $benchmark{'hash'}{'count'}++ if ( $stats );
        $benchmark{'hash'}{'time'}+= tv_interval($t) if ( $stats );
    }
# LIST
    elsif ( $ktype eq "list" ) {
        $t = [gettimeofday] if ( $stats );
        $dredis->del( $key ) if $dredis->exists( $key );
        try { $dredis->rpush( $key, $sredis->lrange( $key, 0, -1 ) ); } catch { LOG("[CRITICAL] key: \"$key\" not moved"); };
        print STDERR "[CRITICAL] Length of list \"$key\" on source and destination not the same\n" if ( $dredis->llen( "$key" ) != $sredis->llen( "$key" ) );
        $benchmark{'list'}{'count'}++ if ( $stats );
        $benchmark{'list'}{'time'}+= tv_interval($t) if ( $stats );
    }
# SET
    elsif ( $ktype eq "set" ) {
        $t = [gettimeofday] if ( $stats );
        my @members = $sredis->smembers( $key );
        while ( my $member = shift @members ) {
            try { $dredis->sadd( $key, $member ); } catch { LOG("[CRITICAL] key: \"$key\" not moved"); };
        }
        LOG ( "[CRITICAL] Length of set \"$key\" on source and destination not the same\n" ) if ( $dredis->scard( "$key" ) != $sredis->scard( "$key" ) );
        $benchmark{'set'}{'count'}++ if ( $stats );
        $benchmark{'set'}{'time'}+= tv_interval($t) if ( $stats );
    }
# ZSET
    elsif ( $ktype eq "zset" ) {
        $t = [gettimeofday] if ( $stats );
        my @zrangelist = $sredis->zrange( $key, 0, -1, "WITHSCORES" );
        while ( my ( $k, $v ) = splice @zrangelist, 0, 2 ) {
           try { $dredis->zadd( $key, $v, $k ); } catch { LOG("[CRITICAL] key: \"$key\" not moved"); };
        }
        $benchmark{'zset'}{'count'}++ if ( $stats );
        $benchmark{'zset'}{'time'}+= tv_interval($t) if ( $stats );
    }
##### not processed :(
    else {
        if ( ! $sredis->exists( $key ) ) {
           LOG ( "[CRITICAL] Key \"$key\" was not copied because were not found on source redis." );
        } else {
           LOG ( "[CRITICAL] Key \"$key\" with type \"$ktype\" was not copied because is not supported yet." );
        }
    }
# TTL
    $t = [gettimeofday] if ( $stats );
    my $kttl = $sredis->ttl( $key );
    if ( $kttl != -1 ) { try {$dredis->expire( $key, $kttl ) } catch { LOG("[CRITICAL] key: \"$key\" not moved"); }; };
    $benchmark{'ttl'}{'count'}++ if ( $stats );
    $benchmark{'ttl'}{'time'}+= tv_interval($t) if ( $stats );
}

if ( $stats ) {
    for my $key ( keys %benchmark ) {
        print "$key\n";
        printf "  %14s: %d\n", "count", $benchmark{$key}{'count'};
        printf "  %14s: %.5fs\n", "time", $benchmark{$key}{'time'};
        printf "  %14s: %.5fs\n", "time per key", $benchmark{$key}{'time'}/$benchmark{$key}{'count'};
    }
}

# vim:ts=4 et softtabstop=4 sw=4 hlsearch nu
