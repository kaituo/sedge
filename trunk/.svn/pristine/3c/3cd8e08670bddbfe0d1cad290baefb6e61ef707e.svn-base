#!/usr/bin/env perl 

use strict; 
use warnings;

if(scalar(@ARGV) < 3 )
{
    print STDERR "Usage: $0 <pigjar> <pigperfjar> <dir containing hadoopsitexml> <pig mix scripts dir> [numruns] [runmapreduce] \n";
    exit(-1);
}
my $pigjar=shift;
my $pigperfjar=shift;
my $confdir=shift;
my $scriptdir = shift;
my $runs = shift;
my $runmapreduce = shift;
if(!defined($runs)) {
    $runs = 3;
}
if(!defined($runmapreduce)) {
    $runmapreduce = 1;
}

my $java;
my $java_home = $ENV{'JAVA_HOME'};
my $classpath= $ENV{'HADOOP_CLASSPATH'};
if(!defined($java_home)) {
    $java = "/usr/bin/java";
} else {
    $java = $java_home."/bin/java";
}

my $pigmix_dir = $ENV{'PIGMIX_DIR'} || "/user/pig/tests/data/pigmix";
my $hadoop_opts = "-DPIGMIX_DIR=$pigmix_dir"; #"-Dmapred.job.queue.name=grideng -Dmapreduce.job.acl-view-job=*";



my $cmd;
my $total_pig_times = 0;
my $total_mr_times = 0;
for(my $i = 1; $i <= 17; $i++) {
    my $pig_times = 0;
    for(my $j = 0; $j < $runs; $j++) {
        $cmd = "$java $hadoop_opts -cp $classpath:$pigjar:$confdir org.apache.pig.Main -param PIGMIX_DIR=$pigmix_dir $scriptdir/L". $i.".pig" ;
        print STDERR "Running Pig Query L".$i."\n";
        print STDERR "L".$i.":";
        print STDERR "Going to run $cmd\n";
        my $s = time();
        print STDERR `$cmd 2>&1`;
        my $e = time();
        $pig_times += $e - $s;
        cleanup($i);
    }
    # find avg
    $pig_times = $pig_times/$runs;
    # round to next second
    $pig_times = int($pig_times + 0.5);
    $total_pig_times = $total_pig_times + $pig_times;

    if ($runmapreduce==0) {
        print "PigMix_$i pig run time: $pig_times\n";
    }
    else {
        my $mr_times = 0;
        for(my $j = 0; $j < $runs; $j++) {
            $cmd = "$java $hadoop_opts -cp $classpath:$pigjar:$confdir:pigperf.jar org.apache.pig.test.pigmix.mapreduce.L$i\n";
            print STDERR "Running Map-Reduce Query L".$i."\n";
            print STDERR "L".$i.":";
            print STDERR "Going to run $cmd";
            my $s = time();
            print STDERR `$cmd 2>&1`;
            my $e = time();
            $mr_times += $e - $s;
            cleanup($i);
        }
        # find avg
        $mr_times = $mr_times/$runs;
        # round to next second
        $mr_times = int($mr_times + 0.5);
        $total_mr_times = $total_mr_times + $mr_times;

        my $multiplier = $pig_times/$mr_times;
        print "PigMix_$i pig run time: $pig_times, java run time: $mr_times, multiplier: $multiplier\n";
    }
}

if ($runmapreduce==0) {
    print "Total pig run time: $total_pig_times\n";
}
else {
    my $total_multiplier = $total_pig_times / $total_mr_times;
    print "Total pig run time: $total_pig_times, total java time: $total_mr_times, multiplier: $total_multiplier\n";
}

sub cleanup {
    my $suffix = shift;
    my $cmd;
    $cmd = "$java -cp $classpath:$pigjar:$confdir org.apache.pig.Main -e rmf L".$suffix."out";
    print STDERR `$cmd 2>&1`;
    $cmd = "$java -cp $classpath:$pigjar:$confdir org.apache.pig.Main -e rmf highest_value_page_per_user";
    print STDERR `$cmd 2>&1`;
    $cmd = "$java -cp $classpath:$pigjar:$confdir org.apache.pig.Main -e rmf total_timespent_per_term";
    print STDERR `$cmd 2>&1`;
    $cmd = "$java -cp $classpath:$pigjar:$confdir org.apache.pig.Main -e rmf queries_per_action";
    print STDERR `$cmd 2>&1`;
    $cmd = "$java -cp $classpath:$pigjar:$confdir org.apache.pig.Main -e rmf tmp";
    print STDERR `$cmd 2>&1`;
}

