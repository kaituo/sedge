#!/bin/sh

java=${JAVA_HOME:='/usr'}/bin/java;
#hadoop_ops="-D mapred.job.queue.name=grideng -D mapreduce.job.acl-view-job=*"

if [ ! -e $java ] 
then
    echo "Cannot find java, set JAVA_HOME environment variable to directory above bin/java."
    exit
fi

PIG_HOME=${PIG_HOME:='.'} 
pigjar=$PIG_HOME/pig.jar
testjar=pigperf.jar
if [ ! -e $pigjar ]
then
    echo "Cannot find pig.jar, set PIG_HOME environment variable to directory pig.jar and build directory are in"
    exit
fi

if [ -z "$PIGMIX_DIR" ]
then 
    PIGMIX_DIR=/user/pig/tests/data/pigmix
fi
	
zipfjar=$PIG_HOME/lib/sdsuLibJKD12.jar

classpath=$pigjar:$zipfjar:$testjar

export HADOOP_CLASSPATH=$HADOOP_CLASSPATH:$classpath

# configure the cluster
conf_dir=$HADOOP_CONF_DIR

# configure the number of mappers, if 0, job is run locally
mappers=90

mainclass=org.apache.pig.test.utils.datagen.DataGenerator 

rows=625000000


user_field=s:20:1600000:z:7
action_field=i:1:2:u:0
os_field=i:1:20:z:0
query_term_field=s:10:1800000:z:20
ip_addr_field=l:1:1000000:z:0
timestamp_field=l:1:86400:z:0
estimated_revenue_field=d:1:100000:z:5
page_info_field=m:10:1:z:0
page_links_field=bm:10:1:z:20

pages=pages625m

echo "Generating $pages"

$HADOOP_HOME/bin/hadoop --config $conf_dir jar pigperf.jar $mainclass $hadoop_ops \
    -m $mappers -r $rows -f $pages $user_field \
    $action_field $os_field $query_term_field $ip_addr_field \
    $timestamp_field $estimated_revenue_field $page_info_field \
    $page_links_field


# Skim off 1 in 10 records for the user table
# Be careful the file is in HDFS if you run previous job as hadoop job, 
# you should either copy data into local disk to run following script
# or run hadoop job to trim the data

protousers=users
echo "Skimming users"
java -Xmx1024m -cp $HADOOP_CLASSPATH:$conf_dir:$PIG_HOME/pig.jar org.apache.pig.Main << EOF
register pigperf.jar;
fs -rmr users;
A = load '$pages' using org.apache.pig.test.udf.storefunc.PigPerformanceLoader()
    as (user, action, timespent, query_term, ip_addr, timestamp, estimated_revenue, page_info, page_links);
B = foreach A generate user;
C = distinct B parallel $mappers;
D = order C by \$0 parallel $mappers;
store D into '$protousers';
EOF


# Create users table, with now user field.
phone_field=s:10:1600000:z:20
address_field=s:20:1600000:z:20
city_field=s:10:1600000:z:20
state_field=s:2:1600:z:20
zip_field=i:2:1600:z:20

users=users100m

echo "Generating $users"

$HADOOP_HOME/bin/hadoop --config $conf_dir jar pigperf.jar $mainclass $hadoop_ops \
    -m $mappers -i $protousers -f $users $phone_field \
    $address_field $city_field $state_field $zip_field

# Find unique keys for fragment replicate join testing
# If file is in HDFS, extra steps are required
numuniquekeys=500
protopowerusers=power_users
echo "Skimming power users"

java -Xmx1024m -cp $HADOOP_CLASSPATH:$conf_dir:$PIG_HOME/pig.jar org.apache.pig.Main << EOF
register pigperf.jar;
fs -rmr $protopowerusers;
A = load '$pages' using org.apache.pig.test.udf.storefunc.PigPerformanceLoader()
    as (user, action, timespent, query_term, ip_addr, timestamp, estimated_revenue, page_info, page_links);
B = foreach A generate user;
C = distinct B parallel $mappers;
D = order C by \$0 parallel $mappers;
E = limit D 500;
store E into '$protopowerusers';
EOF

echo "Generating $powerusers"

powerusers=power_users100m

$HADOOP_HOME/bin/hadoop --config $conf_dir jar pigperf.jar $mainclass $hadoop_ops \
    -m $mappers -i $protopowerusers -f $powerusers $phone_field \
    $address_field $city_field $state_field $zip_field

echo "Generating widerow"

widerowcnt=10000000
widerows=widerow10m
user_field=s:20:10000:z:0
int_field=i:1:10000:u:0 

$HADOOP_HOME/bin/hadoop --config $conf_dir jar pigperf.jar $mainclass $hadoop_ops \
    -m $mappers -r $widerowcnt -f $widerows $user_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field

java -Xmx1024m -cp $HADOOP_CLASSPATH:$conf_dir:$PIG_HOME/pig.jar org.apache.pig.Main << EOF
register pigperf.jar;
fs -rmr ${pages}_sorted;
fs -rmr ${users}_sorted;
fs -rmr ${powerusers}_samples;
A = load '$pages' using org.apache.pig.test.udf.storefunc.PigPerformanceLoader()
    as (user, action, timespent, query_term, ip_addr, timestamp, estimated_revenue, page_info, page_links);
B = order A by user parallel $mappers;
store B into '${pages}_sorted' using PigStorage('\u0001');

alpha = load '$users' using PigStorage('\u0001') as (name, phone, address, city, state, zip);
a1 = order alpha by name parallel $mappers;
store a1 into '${users}_sorted' using PigStorage('\u0001');

a = load '$powerusers' using PigStorage('\u0001') as (name, phone, address, city, state, zip);
b = sample a 0.5;
store b into '${powerusers}_samples' using PigStorage('\u0001');

A = load '$pages' as (user, action, timespent, query_term, ip_addr, timestamp,
        estimated_revenue, page_info, page_links);
B = foreach A generate user, action, timespent, query_term, ip_addr, timestamp, estimated_revenue, page_info, page_links,
user as user1, action as action1, timespent as timespent1, query_term as query_term1, ip_addr as ip_addr1, timestamp as timestamp1, estimated_revenue as estimated_revenue1, page_info as page_info1, page_links as page_links1,
user as user2, action as action2, timespent as timespent2, query_term as query_term2, ip_addr as ip_addr2, timestamp as timestamp2, estimated_revenue as estimated_revenue2, page_info as page_info2, page_links as page_links2;
store B into 'widegroupbydata';
EOF

mkdir $powerusers
java -Xmx1024m -cp $HADOOP_CLASSPATH:$conf_dir:$PIG_HOME/pig.jar org.apache.pig.Main << EOF
fs -copyToLocal ${powerusers}/* $powerusers;
EOF

cat $powerusers/* > power_users

java -Xmx1024m -cp $HADOOP_CLASSPATH:$conf_dir:$PIG_HOME/pig.jar org.apache.pig.Main << EOF
fs -rmr $protousers;
fs -rmr $protopowerusers;
fs -rmr $powerusers;
fs -mkdir $PIGMIX_DIR;
fs -mv $pages $PIGMIX_DIR/page_views;
fs -mv ${pages}_sorted $PIGMIX_DIR/page_views_sorted;
fs -mv $users $PIGMIX_DIR/users;
fs -mv ${users}_sorted $PIGMIX_DIR/users_sorted;
fs -copyFromLocal power_users $PIGMIX_DIR/power_users;
fs -mv ${powerusers}_samples $PIGMIX_DIR/power_users_samples;
fs -mv widegroupbydata $PIGMIX_DIR/widegroupbydata;
fs -mv $widerows $PIGMIX_DIR/widerow;
EOF

rm -fr $powerusers
rm power_users
