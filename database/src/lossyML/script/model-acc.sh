nohup bash script/kmeans-UCR.sh dtree pla 3 > nohup1.out &
nohup bash script/kmeans-UCR.sh dtree buff 3 > nohup1.out &
nohup bash script/kmeans-UCR.sh dtree paa 3 > nohup2.out &
nohup bash script/kmeans-UCR.sh dtree fft 3 > nohup3.out

nohup bash script/kmeans-UCR.sh knn pla 3 > nohup1.out &
nohup bash script/kmeans-UCR.sh knn buff 3 > nohup1.out &
nohup bash script/kmeans-UCR.sh knn paa 3 > nohup2.out &
nohup bash script/kmeans-UCR.sh knn fft 3 > nohup3.out

nohup bash script/kmeans-UCR.sh rforest pla 3 > nohup1.out &
nohup bash script/kmeans-UCR.sh rforest buff 3 > nohup1.out &
nohup bash script/kmeans-UCR.sh rforest paa 3 > nohup2.out &
nohup bash script/kmeans-UCR.sh rforest fft 3 > nohup3.out

nohup bash script/kmeans-UCR.sh kmeans pla 3 > nohup1.out &
nohup bash script/kmeans-UCR.sh kmeans buff 3 > nohup1.out &
nohup bash script/kmeans-UCR.sh kmeans paa 3 > nohup2.out &
nohup bash script/kmeans-UCR.sh kmeans fft 3 > nohup3.out

#nohup bash script/kmeans-UCR.sh nb pla 3 > nohup1.out &
#nohup bash script/kmeans-UCR.sh nb buff 3 > nohup1.out &
#nohup bash script/kmeans-UCR.sh nb paa 3 > nohup2.out &
#nohup bash script/kmeans-UCR.sh nb fft 3 > nohup3.out
#
#nohup bash script/kmeans-UCR.sh dbscan pla 3 > nohup1.out &
#nohup bash script/kmeans-UCR.sh dbscan buff 3 > nohup1.out &
#nohup bash script/kmeans-UCR.sh dbscan paa 3 > nohup2.out &
#nohup bash script/kmeans-UCR.sh dbscan fft 3 > nohup3.out

