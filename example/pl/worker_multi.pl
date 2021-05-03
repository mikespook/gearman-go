#!/usr/bin/perl

# Runs 20 children that expose "PerlToUpper" before returning the result.

use strict; use warnings;
use constant CHILDREN => 20;
use Time::HiRes qw(usleep);
use Gearman::Worker;

$|++;
my @child_pids;
for (1 .. CHILDREN) {
  if (my $pid = fork) {
    push @child_pids, $pid;
    next;
  }
  eval {
    my $w = Gearman::Worker->new(job_servers => '127.0.0.1:4730');
    $w->register_function(PerlToUpper => sub { print "."; uc $_[0]->arg });
    $w->work while 1;
  };
  warn $@ if $@;
  exit 0;
}

$SIG{INT} = $SIG{HUP} = sub {
  kill 9, @child_pids;
  print "\nChildren shut down, gracefully exiting\n";
  exit 0;
};

printf "Forked %d children, serving 'PerlToUpper' function to gearman\n", CHILDREN;
sleep;
