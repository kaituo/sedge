register /home/kaituo/code/pigmix/pigperf.jar;
A = load '/home/kaituo/code/pig3/trunk/PigmixRandomData/100/page_views/pages625m9' using org.apache.pig.test.udf.storefunc.PigPerformanceLoader() as (user: chararray, action:int, timespent:int, query_term:chararray, ip_addr:long, timestamp:long,estimated_revenue:double, page_info:map[], page_links:bag{t:(p:map[])});
B = foreach A generate user, action, timespent, query_term, estimated_revenue;
store B into '/home/kaituo/code/pig3/trunk/PigmixRandomData/100/data12X/data12N9';
