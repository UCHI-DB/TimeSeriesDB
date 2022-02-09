cd /home/cc/TimeSeriesDB/database;
#dir=$1
#for comp in gorilla gorillabd splitdouble byteall bytedec bp bpsplit gzip snappy sprintz plain dict pqgzip pqsnappy;
TIME=$1
echo "" > new.out
for comp in buff buff-major fixed sprintz gorilla gorillabd gzip snappy;
#for comp in buff buff-major fixed;
do
  for ratio in 0.0001 0.001, 0.01 0.1 0.2 0.3 0.4 0.5 0.6 0.7 0.8 0.9 0.95;
		do
		  echo $i
#			for file in $(ls /mnt/hdd-2T-3/chunwei/timeseries_dataset/*/*/*);
      for i in $(seq 1 $TIME);
			    do
           cargo +nightly run --release --package time_series_start --bin query /home/cc/float_comp/signal/time_series_120rpm-c2-current.csv $comp 100000 37000000 $ratio >> new.out
           #cargo +nightly run --release --package time_series_start --bin query ../UCRArchive2018/Kernel/randomwalkdatasample1k-40k $comp 10000 40000000 $ratio >> new.out
			    done

		done
  for ratio in 0.0001 0.001, 0.01 0.1 0.2 0.3 0.4 0.5 0.6 0.7 0.8 0.9 0.95;
		do
		  echo $i
#			for file in $(ls /mnt/hdd-2T-3/chunwei/timeseries_dataset/*/*/*);
      for i in $(seq 1 $TIME);
			    do
            cargo +nightly run --release --package time_series_start --bin query /home/cc/float_comp/signal/time_series_120rpm-c8-supply-voltage.csv $comp 10000 36900000 $ratio >> new.out
			    done

		done

done
echo "outlier experiments done!"
python ./script/python/proj_logparser.py new.out proj_performance.csv $TIME
			
