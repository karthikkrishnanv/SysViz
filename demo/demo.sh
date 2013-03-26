#!/bin/sh

echo "Uncomment any of the lines in demo.sh below to try.."

# collect local system data for 45 seconds at 1 second interval
#../sysviz.sh  -i 1 -c 45 

# collect system data on remote nodes dnode{0..4}  for 30 seconds
# need to be able to ssh to these nodes without password
#../sysviz.sh  -i 1 -c 30 -r dnode0:dnode1:dnode2:dnode3:dnode4


# collect system data on remote nodes dnode{0..4}  for 30 seconds
# do not build vizualizer pdf. Just collect data


# collect system data on remote nodes dnode{0..4}  for 30 seconds
# specify nnode to be the namenode => mapreduce jobs will also be vizualized
# for mapreduce, we plot concurrent map/reduce/shuffle tasks, HDFS,Local I/O information etc
# Individual task level details are included in the csv as well
#../sysviz.sh  -i 1 -c 30 -r dnode0:dnode1:dnode2:dnode3:dnode4 -m nnode
