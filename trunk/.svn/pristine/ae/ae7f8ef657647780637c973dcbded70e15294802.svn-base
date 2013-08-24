register /home/kaituo/code/pigmix/pigperf.jar;
A = load '/home/kaituo/code/pig3/trunk/PigmixRandomData/100/page_views/pages625m9' using org.apache.pig.test.udf.storefunc.PigPerformanceLoader() as (user, action, timespent, query_term, ip_addr, timestamp, estimated_revenue, page_info, page_links);
store A into '/home/kaituo/code/pig3/trunk/PigmixRandomData/100/page_viewsX/pages625m9' using PigStorage('');
