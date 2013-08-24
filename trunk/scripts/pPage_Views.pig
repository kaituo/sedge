register pigperf.jar;
A = load '/home/kaituo/code/pig3/trunk/pigmixdata/page_views' using org.apache.pig.test.udf.storefunc.PigPerformanceLoader() as (user, action, timespent, query_term, ip_addr, timestamp, estimated_revenue, page_info, page_links);
B = limit A 10;
store B into '/home/kaituo/code/pig3/trunk/pigmixExtData/page_viewsData' using PigStorage('');
