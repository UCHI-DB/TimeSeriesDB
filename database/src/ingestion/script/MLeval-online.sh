task=$1
budget=$2
comp=snappy


#for rec in bufflossy paa fft pla
#  do
#    for tcr in 0.999	0.98	0.9	0.8	0.7	0.65	0.6	0.5	0.499	0.4	0.333	0.332	0.31	0.3	0.29	0.25	0.249	0.2	0.199	0.15	0.143	0.142	0.125	0.124	0.1	0.05	0.01	0.0001
#      do
#        cargo +nightly run --release --package ingestion --bin ingestion ../../test_configs/config-single-ingestion.toml ${task} ${comp} ${rec} 1 1 online ${tcr} >  ./data/online/${task}/${task}_${budget}_${comp}_${rec}_${tcr}.csv
#        python3 /Users/chunwei/research/pygraph/adaedge/offline/acc_extractor.py ./data/online/${task}/${task}_${budget}_${comp}_${rec}_${tcr}.csv
#      done
#
#  done


for tcr in 0.999	0.98	0.9	0.8	0.7	0.65	0.6	0.5	0.499	0.4	0.333	0.332	0.31	0.3	0.29	0.25	0.249	0.2	0.199	0.15	0.143	0.142	0.125	0.124	0.1	0.05	0.01	0.0001
  do
    cargo +nightly run --release --package ingestion --bin ingestion ../../test_configs/config-single-ingestion.toml ${task} snappy fft 1 1 mabonline ${tcr} >  ./data/online/${task}/${task}_${budget}_mab_mab_${tcr}.csv
    python3 /Users/chunwei/research/pygraph/adaedge/offline/acc_extractor.py ./data/online/${task}/${task}_${budget}_mab_mab_${tcr}.csv

  done


#for rec in bufflossy paa fft pla
#  do
#    echo ${rec}
#    for tcr in 0.999	0.98	0.9	0.8	0.7	0.65	0.6	0.5	0.499	0.4	0.333	0.332	0.31	0.3	0.29	0.25	0.249	0.2	0.199	0.15	0.143	0.142	0.125	0.124	0.1	0.05	0.01	0.0001
#      do
#        tail -2 ./data/online/${task}/${task}_${budget}_${comp}_${rec}_${tcr}_acc.txt | head -1
#      done
#  done

echo "mabonline"
for tcr in 0.999	0.98	0.9	0.8	0.7	0.65	0.6	0.5	0.499	0.4	0.333	0.332	0.31	0.3	0.29	0.25	0.249	0.2	0.199	0.15	0.143	0.142	0.125	0.124	0.1	0.05	0.01	0.0001
      do
        tail -2 ./data/online/${task}/${task}_${budget}_mab_mab_${tcr}_acc.txt | head -1
      done


echo "all done"

