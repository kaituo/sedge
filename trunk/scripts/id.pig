A = load '/home/kaituo/code/pig3/trunk/testdata/passwd' using PigStorage(':'); 
B = foreach A generate $0 as id;
dump B; 
store B into '/home/kaituo/code/pig3/trunk/testoutput/id.out';