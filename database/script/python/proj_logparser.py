import re
import sys
import csv
onheap = []
skip = []
a = 0
b = 0
infile = sys.argv[1]
outfile = sys.argv[2]
time = int(sys.argv[3])

def isfloat(value):
    try:
        float(value)
        return True
    except ValueError:
        return False

# infile = "input.txt"
# outfile = "output.txt"
# time =3
cur = ""
i = 0
t_proj = 0.0
sample = 0.0
with open(outfile, 'w+') as the_file:
    with open(infile) as f:
        content = f.readlines()
        for line in content:
            if line.rstrip().split(":")[0] == "res":
                i+=1
                cur= line.rstrip().split(":")[1]
                arr = cur.rstrip().split(",")
                # print cur
                t_proj += float(arr[5])
                sample += float(arr[4])
                # the_file.write("%s\n" % cur)
                if i==time:
                    paramters = arr[:4]
                    paramters.append(str(int(sample/time)))
                    paramters.append(str(t_proj/time))
                    overview =','.join([str(x) for x in paramters])
                    the_file.write("%s\n" % overview)
                    i = 0
                    t_proj = 0.0
                    sample=0.0
                cur=""
