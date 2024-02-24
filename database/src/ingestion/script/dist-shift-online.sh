#for comp in gorilla gzip snappy sprintz buff
for comp in zlib_3 zlib_5 zlib_7
  do
    echo ${comp}
    cargo +nightly run --release --package ingestion --bin ingestion ../../test_configs/config-single-ingestion-cbf-gorilla.toml knn ${comp} paa 1 1 online 0.4 > ./data/data_shift_online/${comp}_on_cbf_gorilla_20s.txt
  done



#echo "mabonline"
#cargo +nightly run --release --package ingestion --bin ingestion ../../test_configs/config-single-ingestion-cbf-gorilla.toml knn gzip paa 1 1 mabonline 0.4 > ./data/data_shift_online/mab_on_cbf_gorilla_20s.txt
echo "all done"

