task=$1
budget=$2
comp=snappy


for tcr in 0.999	0.98	0.9	0.8	0.7	0.65	0.6	0.5	0.499	0.4	0.333	0.332	0.31	0.3	0.29	0.25	0.249	0.2	0.199	0.15	0.143	0.142	0.125	0.124	0.1	0.05	0.01	0.0001
  do
    for rec in fft pla
      do
        cargo +nightly run --release --package ingestion --bin ingestion ../../test_configs/config-single-ingestion.toml ${task} ${comp} ${rec} 1 1 online ${tcr} >  ./data/online/${task}_${budget}_${comp}_${rec}_${tcr}.csv
        python3 /Users/chunwei/research/pygraph/adaedge/offline/acc_extractor.py ./data/online/${task}_${budget}_${comp}_${rec}_${tcr}.csv
      done

  done

for rec in fft pla
  do
    for tcr in 0.999	0.98	0.9	0.8	0.7	0.65	0.6	0.5	0.499	0.4	0.333	0.332	0.31	0.3	0.29	0.25	0.249	0.2	0.199	0.15	0.143	0.142	0.125	0.124	0.1	0.05	0.01	0.0001
      do
        tail -1 ./data/online/${task}_${budget}_${comp}_${rec}_${tcr}_acc.txt
      done
  done


echo "all done"

