#!/bin/bash 

PATH=/bin:/sbin:/usr/bin:/usr/sbin:$PATH:.

###########################################################################################
# parse log files for all jobs between [jobid_start] and [jobid_end] 
# print detailed job related counters for all jobs
###########################################################################################


mapred_parse_usage() {
  echo "Usage: `basename $1` [OPTIONS] [jobid_start] [jobid_end]"
  echo "                              [-hv]            # help"
  echo "                              [-n namenode]    # parse log files from namenode"
  echo "                              [-l log]         # optional hadoop log dir"
  echo "                              [-t tempdir]     # optional temp dir for intermediate files"
  echo "                              [-d]             # disable header print?"
  echo "                              [-s]             # print job summary vs detailed"
  echo "                              [jobid_start]    # mapreduce start jobid"
  echo "                              [jobid_end]      # mapreduce end jobid"
}

function cleanup_temp {
  tmpfile=$1
  [ -e $tmpfile ] && rm --force $tmpfile
}


###########################################################################################
# Need to be able to do scp/ssh to namenode
###########################################################################################

script_dir="$( cd "$( dirname "$0" )" && pwd )"

namenode=
workdir=$(mktemp -d).$RANDOM
start_jobid=
end_jobid=
disable_header=0
print_summary=0
hadooplog=
# Parse command line options.
while getopts hv:n:t:l:d:s: OPT; do
    case "$OPT" in
        h)  mapred_parse_usage $0 && exit 0 ;;
        v)  echo "`basename $0` version 0.1" && exit 0 ;;
        n)  namenode=$OPTARG;;
        t)  workdir=$OPTARG;;
        l)  hadooplog=$OPTARG;;
        s)  print_summary=$OPTARG;;
        d)  disable_header=$OPTARG;;
        \?) mapred_parse_usage $0 >&2 && exit 1;;
    esac
done


# Remove the options we parsed above.
shift `expr $OPTIND - 1`

# We want at least one non-option argument. 
# Remove this block if you don't need it.
if [ $# -ne 2 ]; then
    mapred_parse_usage $0 >&2
    exit 1
fi

start_jobid=$1
end_jobid=$2

if [ -z $namenode ]
then
  echo "namenode not specified!..Exiting!"
  mapred_parse_usage $0 >&2
        exit 1
fi

ssh -q $namenode exit
if [ $? != "0" ]
then
  echo "Unable to do ssh to $namenode..exiting"
  mapred_parse_usage $0 >&2
        exit 1
fi

ssh $namenode mkdir -p $workdir
scp -qr $script_dir $USER@$namenode:$workdir

#if [ $disable_header -eq 1 ]; then
  ssh $namenode $workdir/mapred/print_job_details.sh -d ${disable_header} -s ${print_summary} $start_jobid $end_jobid $hadooplog

#else
 # ssh $namenode $workdir/mapred/print_job_details.sh $start_jobid $end_jobid $hadooplog
#fi




