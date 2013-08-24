register pigperf.jar;
%default PIGMIX_DIR /user/pig/tests/data/pigmix
A = load '$PIGMIX_DIR/page_views_sorted' using org.apache.pig.test.udf.storefunc.PigPerformanceLoader()
    as (user, action, timespent, query_term, ip_addr, timestamp, estimated_revenue, page_info, page_links);
B = foreach A generate user, estimated_revenue;
alpha = load '$PIGMIX_DIR/users_sorted' using PigStorage('\u0001') as (name, phone, address, city, state, zip);
beta = foreach alpha generate name;
C = join B by user, beta by name using "merge";
store C into 'L14out';

