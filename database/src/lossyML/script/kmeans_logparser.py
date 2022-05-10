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
dist = 0.0
acc = 0.0
acc1 = 0.0
homo = 0.0
comp = 0.0
vm =0.0

with open(outfile, 'w+') as the_file:
    with open(infile) as f:
        content = f.readlines()
        for line in content:
            if "UCI121" in line.rstrip():
                cur= line.rstrip()
            elif "UCRArchive2018" in line.rstrip():
                cur= line.rstrip()
            elif 'ns:' in line.rstrip().split(",")[0] :
                cur+= line.rstrip().split(",")[0].split(":")[1]
                cur+=","
                cur+= line.rstrip().split(",")[1].split(":")[1]
                cur+=","
            elif line.rstrip().split(":")[0] == "K":
                cur+= line.rstrip().split(":")[1]
                cur+=","
            elif line.rstrip().split(":")[0] == "distortion":
                dist += float(line.rstrip().split(":")[1])
            elif line.rstrip().split(":")[0] == "Acc":
                acc += float(line.rstrip().split(":")[1].split(",")[0])
                acc1 += float(line.rstrip().split(":")[1].split(",")[1])
            elif line.rstrip().split(":")[0] == "Homogeneity":
                homo += float(line.rstrip().split(":")[1])
            elif line.rstrip().split(":")[0] == "Completeness":
                comp += float(line.rstrip().split(":")[1])
            elif line.rstrip().split(":")[0] == "V Measure":
                i+=1
                vm += float(line.rstrip().split(":")[1])

                # the_file.write("%s\n" % cur)
                if i==time:
                    cur+=str(dist/time)
                    cur+=","
                    cur+=str(acc/time)
                    cur+=","
                    cur+=str(acc1/time)
                    cur+=","
                    cur+=str(homo/time)
                    cur+=","
                    cur+=str(comp/time)
                    cur+=","
                    cur+=str(vm/time)
                    the_file.write("%s\n" % cur)
                    i = 0
                    dist = 0.0
                    acc = 0.0
                    acc1 = 0.0
                    homo = 0.0
                    comp = 0.0
                    vm = 0.0
                cur=""
