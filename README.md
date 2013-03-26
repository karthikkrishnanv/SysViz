SysViz collects system statistics (cpu, memory, disk, network etc) of a cluster of nodes and vizualizes using "R".
Uses vanilla UNIX tools (sar, python, sed, bash etc) for collecting system data. The vizualizer uses "R". 

Includes more details for hadoop mapreduce jobs such as concurrency level of map/reduce/shuffle tasks, HDFS/Local IO etc 
(Built on top of another opensourced parsing code)
