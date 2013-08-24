--This script covers order by of multiple values.
register pigperf.jar;
%default PIGMIX_DIR /user/pig/tests/data/pigmix
A = load '$PIGMIX_DIR/page_views' using org.apache.pig.test.udf.storefunc.PigPerformanceLoader()
    as (user, action, timespent:int, query_term, ip_addr, timestamp,
        estimated_revenue:double, page_info, page_links);
B = order A by query_term, estimated_revenue desc, timespent;
store B into 'L10out';
