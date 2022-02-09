cd /home/cc/TimeSeriesDB/database;
#dir=$1
#for comp in gorilla gorillabd splitdouble byteall bytedec bp bpsplit gzip snappy sprintz plain dict pqgzip pqsnappy;
TIME=$4
SCL=$2
PRED=$3


# start lossy compression profiling
# fft
for ratio in 0.01 0.05 0.1 0.2 0.3 0.4 0.5;
do
  for i in $(seq 1 $TIME);
		do
		  echo $i
#			for file in $(ls /mnt/hdd-2T-3/chunwei/timeseries_dataset/*/*/*);
      for file in $1;
			    do

            cargo +nightly run --release  --package time_series_start --bin comp_profiler $file fft $SCL $ratio

			    done

		done

done

# paa
for ws in 64 32 16 8 4;
do
  for i in $(seq 1 $TIME);
		do
		  echo $i
#			for file in $(ls /mnt/hdd-2T-3/chunwei/timeseries_dataset/*/*/*);
      for file in $1;
			    do

            cargo +nightly run --release  --package time_series_start --bin comp_profiler $file paa $SCL $ws

			    done

		done

done



# start lossless compression
#for comp in gorilla gorillabd splitdouble bytedec byteall sprintz bpsplit gzip snappy dict RAPG RAPG-major fixed;
for comp in buff buff-major fixed sprintz gorilla gorillabd gzip snappy;
do
  for i in $(seq 1 $TIME);
		do
		  echo $i
#			for file in $(ls /mnt/hdd-2T-3/chunwei/timeseries_dataset/*/*/*);
      for file in $1;
			    do

            cargo +nightly run --release  --package time_series_start --bin comp_profiler $file $comp $SCL $PRED

			    done

		done

done


echo "Float compression done!"
#python ./script/python/logparser.py new.out performance.csv $TIME
# borrow the simd parser for lossy logs
python ./script/python/logparser-simd.py new.out performance.csv $TIME
			
