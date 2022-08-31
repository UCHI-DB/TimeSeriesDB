
method=$1
lossy=$2
TIME=$3
#for comp in ofsgorilla gorilla gorillabd splitbd split bp zlib paa fourier snappy deflate gzip deltabp;

for file in $(ls ../../../UCRArchive2018);
#for file in $(ls /home/cc/TimeSeriesDB/UCRArchive2018);
  do
    if [ "$lossy" == "grail" ]; then
      for prec in 1;
		  do
		    for i in $(seq 1 $TIME);
		      do
		        cargo +nightly run --release --package r_kmeans --bin r_kmeans $method ../../../UCRArchive2018/${file}/${file}_TRAIN $lossy $prec >> ./data/UCR-model-acc08/${method}_${lossy}_log.csv
		      done
	    done
    elif [ "$lossy" == "paa" ]; then
      for prec in 1 2 3 4 5 9 16 32;
		  do
		    for i in $(seq 1 $TIME);
		      do
		        cargo +nightly run --release --package lossyML --bin lossyML $method ../../../UCRArchive2018/${file}/${file}_TRAIN $lossy $prec >> ./data/UCR-model-acc08/${method}_${lossy}_log.csv
		      done
	    done
	  elif [ "$lossy" == "fft" ]; then
      for prec in 1.00	0.50	0.20	0.17	0.14	0.12	0.1	0.06;
		  do
		    for i in $(seq 1 $TIME);
		      do
		        cargo +nightly run --release --package lossyML --bin lossyML $method ../../../UCRArchive2018/${file}/${file}_TRAIN $lossy $prec >> ./data/UCR-model-acc08/${method}_${lossy}_log.csv
		      done
	    done
	  elif [ "$lossy" == "pla" ]; then
          for prec in 1.00	0.50	0.20	0.17	0.14	0.12	0.1	0.06;
    		  do
    		    for i in $(seq 1 $TIME);
    		      do
    		        cargo +nightly run --release --package lossyML --bin lossyML $method ../../../UCRArchive2018/${file}/${file}_TRAIN $lossy $prec >> ./data/UCR-model-acc08/${method}_${lossy}_log.csv
    		      done
    	    done
	  elif [ "$lossy" == "buff" ]; then
      for prec in -1 5 4 3 2 1 0;
		  do
		    for i in $(seq 1 $TIME);
		      do
		        cargo +nightly run --release --package lossyML --bin lossyML $method ../../../UCRArchive2018/${file}/${file}_TRAIN $lossy $prec >> ./data/UCR-model-acc08/${method}_${lossy}_log.csv
		      done
	    done
    fi
#    for prec in 1.0 0.8 0.6 0.4 0.2 0.1 0.05;
#    for prec in 1 2 4 8 16 32 64;
#    for prec in -1 5 4 3 2 1 0;

	done
echo "all done to ${method}_${lossy}_log.csv"

python3 ../../../database/src/lossyML/script/kmeans_logparser.py ./data/UCR-model-acc08/${method}_${lossy}_log.csv ./data/UCR-model-acc08/UCR-${method}_${lossy}.csv $TIME

			
