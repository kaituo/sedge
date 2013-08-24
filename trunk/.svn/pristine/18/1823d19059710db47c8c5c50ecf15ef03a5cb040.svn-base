-- This script covers foreach/generate with a nested distinct.
register pigperf.jar;
%default PIGMIX_DIR /user/pig/tests/data/pigmix
A = load '$PIGMIX_DIR/page_views' using org.apache.pig.test.udf.storefunc.PigPerformanceLoader()
    as (user, action, timespent, query_term, ip_addr, timestamp,
        estimated_revenue, page_info, page_links);
B = foreach A generate user, action;
C = group B by user;
D = foreach C {
    aleph = B.action;
    beth = distinct aleph;
    generate group, COUNT(beth);
}
store D into 'L4out';
