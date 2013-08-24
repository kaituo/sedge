register /home/kaituo/code/pigmix/pigperf.jar;
A = load '/home/kaituo/code/pig3/trunk/PigmixRandomData/100/power_usersX/power_users100m9' using PigStorage('') as (name: chararray, phone: chararray, address: chararray, city: chararray, state: chararray, zip: int);
B = sample A 0.5;
store B into '/home/kaituo/code/pig3/trunk/PigmixRandomData/100/power_users_sampleX/power_users_sample9' using PigStorage('');
