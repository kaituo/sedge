-- This script covers group all.
register pigperf.jar;
%default PIGMIX_DIR /user/pig/tests/data/pigmix
A = load '$PIGMIX_DIR/page_views' using org.apache.pig.test.udf.storefunc.PigPerformanceLoader()
    as (user, action, timespent, query_term, ip_addr, timestamp,
        estimated_revenue, page_info, page_links);
B = foreach A generate user, (int)timespent as timespent, (double)estimated_revenue as estimated_revenue;
C = group B all;
D = foreach C generate SUM(B.timespent), AVG(B.estimated_revenue);
store D into 'L8out';
