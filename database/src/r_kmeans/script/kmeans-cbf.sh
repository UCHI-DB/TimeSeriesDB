cd /Users/chunwei/research/TimeSeriesDB/database/src/r_kmeans/;
method=$1
lossy=$2
TIME=$3
#for comp in ofsgorilla gorilla gorillabd splitbd split bp zlib paa fourier snappy deflate gzip deltabp;
file="CBF"
#for file in $(ls /Users/chunwei/research/TimeSeriesDB/UCRArchive2018);
#for file in $(ls /home/cc/TimeSeriesDB/UCRArchive2018);
#  do
    if [ "$lossy" == "grail" ]; then
      for prec in 1;
		  do
		    for i in $(seq 1 $TIME);
		      do
		        cargo +nightly run --release --package r_kmeans --bin r_kmeans $method ../../../UCRArchive2018/${file}/${file}_TEST $lossy $prec >> data/${method}_${lossy}_testlog.csv
		      done
	    done
    elif [ "$lossy" == "paa" ]; then
      for prec in 1 2 4 8 16 32 64;
		  do
		    for i in $(seq 1 $TIME);
		      do
		        cargo +nightly run --release --package r_kmeans --bin r_kmeans $method ../../../UCRArchive2018/${file}/${file}_TRAIN $lossy $prec >> data/${method}_${lossy}_testlog.csv
		      done
	    done
	  elif [ "$lossy" == "fft" ]; then
      for prec in 1.0 0.5 0.4 0.3 0.2 0.1 0.05 0.025;
		  do
		    for i in $(seq 1 $TIME);
		      do
		        cargo +nightly run --release --package r_kmeans --bin r_kmeans $method ../../../UCRArchive2018/${file}/${file}_TRAIN $lossy $prec >> data/${method}_${lossy}_testlog.csv
		      done
	    done
	  elif [ "$lossy" == "buff" ]; then
      for prec in -1 5 4 3 2 1 0;
		  do
		    for i in $(seq 1 $TIME);
		      do
		        cargo +nightly run --release --package r_kmeans --bin r_kmeans $method ../../../UCRArchive2018/${file}/${file}_TRAIN $lossy $prec >> data/${method}_${lossy}_testlog.csv
		      done
	    done
    fi
#    for prec in 1.0 0.8 0.6 0.4 0.2 0.1 0.05;
#    for prec in 1 2 4 8 16 32 64;
#    for prec in -1 5 4 3 2 1 0;

#	done
echo "all done to ${method}_${lossy}_testlog.csv"

python3 /Users/chunwei/research/TimeSeriesDB/database/src/r_kmeans/script/kmeans_logparser.py data/${method}_${lossy}_testlog.csv data/parsed-test-${method}_${lossy}.csv $TIME

			
