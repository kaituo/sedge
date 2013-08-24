-- This script covers the case where the group by key is a significant
-- percentage of the row.
register pigperf.jar;
%default PIGMIX_DIR /user/pig/tests/data/pigmix
A = load '$PIGMIX_DIR/page_views' using org.apache.pig.test.udf.storefunc.PigPerformanceLoader()
    as (user, action, timespent, query_term, ip_addr, timestamp,
        estimated_revenue, page_info, page_links);
B = foreach A generate user, action, (int)timespent as timespent, query_term, ip_addr, timestamp;
C = group B by (user, query_term, ip_addr, timestamp);
D = foreach C generate flatten(group), SUM(B.timespent);
store D into 'L6out';

