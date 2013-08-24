register /home/kaituo/code/pigmix/pigperf.jar;
alpha = load '/home/kaituo/code/pig3/trunk/PigmixRandomData/100/usersX/users100m9' using PigStorage('') as (name: chararray, phone: chararray, address: chararray, city: chararray, state: chararray, zip: int);
a1 = order alpha by name;
store a1 into '/home/kaituo/code/pig3/trunk/PigmixRandomData/100/data14X/data14N9';
