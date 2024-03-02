task=$1
budget=$2
tcr=0.1

#cargo +nightly run --release --package ingestion --bin ingestion ../../test_configs/config-single-ingestion.toml ${task} snappy fft 1 1 maboffline ${tcr}  > ./data/200k-${task}_${budget}_mab_mab.csv
#cargo +nightly run --release --package ingestion --bin ingestion ../../test_configs/config-single-ingestion.toml ${task} snappy fft 1 1 true > ./data/${task}_${budget}_mab_mab.csv
python3 /Users/chunwei/research/pygraph/adaedge/offline/acc_extractor.py ./data/200k-${task}_${budget}_mab_mab.csv
python3 /Users/chunwei/research/pygraph/adaedge/offline/space_acc_plot.py 200k-${task}_${budget}_mab_mab

for comp in gorilla gzip snappy sprintz buff
  do
    for rec in rrd bufflossy paa fft pla
      do
#        cargo +nightly run --release --package ingestion --bin ingestion ../../test_configs/config-single-ingestion.toml ${task} ${comp} ${rec} 1 1 offline ${tcr}  > ./data/200k-${task}_${budget}_${comp}_${rec}.csv
#        cargo +nightly run --release --package ingestion --bin ingestion ../../test_configs/config-single-ingestion.toml ${task} ${comp} ${rec} 1 1 offline ${tcr} >  ./data/${task}_${budget}_${comp}_${rec}.csv
        python3 /Users/chunwei/research/pygraph/adaedge/offline/acc_extractor.py ./data/200k-${task}_${budget}_${comp}_${rec}.csv
        python3 /Users/chunwei/research/pygraph/adaedge/offline/space_acc_plot.py 200k-${task}_${budget}_${comp}_${rec}
      done

  done


echo "all done"

