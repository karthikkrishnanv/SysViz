#!/bin/bash 

###########################################################################################
# Program to visualize cluster (system and architecture) as well as mapreduce job counters
# Author:  Karthik Krishnan (karthik.krishnan.v@gmail.com)
# Needs passwordless ssh/scp to remote hosts, if specified
# Expects R (http://www.r-project.org/) to have been installed (only on the local client)
###########################################################################################

PATH=/bin:/sbin:/usr/bin:/usr/sbin:$PATH:.

printhelp() {
  echo "Usage: `basename $1` [OPTIONS] "
  echo "                 [-hv]            # help"
  echo "                 [-i interval(s)] # sar data collection interval (s)"
  echo "                 [-c count]       # sar data collection count"
  echo "                 [-t tempdir]     # optional temp workdir for intermediate files"
  echo "                 [-r n0:n1:...]   # optional collect sar data on remote hosts n0,n1 etc"
  echo "                 [-o outputdir]   # optional full path to final output dir"
  echo "                 [-x cmd.sh]      # optional execute file cmd.sh in background"
  echo "                 [-m namenode]    # optional plot completed mapred counters from namenode"
  echo "                 [-l log]         # optional mapreduce log dir on namenode"
  echo "                 [-a t0:t1]       # optional t0:rampup, t1:steadystate (time in seconds)"
  echo "                 [-d]             # optional disable vizualizer (data collect only)"
  echo "                 [-e]             # optional collect emon (scripts in emon dir)"
}

script_dir="$( cd "$( dirname "$0" )" && pwd )"
process_mapred_script_fullpath=$script_dir/mapred/process_mapred_jobs.sh
mapredjobs_watchdog_script_fullpath=$script_dir/mapred/print_completed_mapredjobs.sh
remotenodes=$('hostname')
localhost=$('hostname')
useremote=0
namenode=
count=0
interval=0
outputdir=.
workdir=$(mktemp -d).$random
command_exec=
hadooplog="/var/log/hadoop/history/done"
disable_viz=0
collect_emon=0
apply_filter=0
filter_windows=
sar_log_splitter=$script_dir/parse/parse_sar_data.sh


###########################################################################################
# Parse command line options
###########################################################################################

while getopts hvei:c:t:r:o:x:m:l:a:d opt; do
  case "$opt" in
    h)  printhelp $0 && exit 0;;
    v)  echo "`basename $0` version 0.1";;
    i)  interval=$OPTARG;;
    c)  count=$OPTARG;;
    t)  workdir=$OPTARG;;
    r)  remotenodes=$OPTARG && useremote=1;;
    a)  filter_windows=$OPTARG && apply_filter=1;;
    o)  outputdir=$OPTARG;;
    x)  command_exec=$OPTARG;;
    m)  namenode=$OPTARG;;
    l)  hadooplog=$OPTARG;;
    d)  disable_viz=1;;
    e)  collect_emon=1;;
   \?)  printhelp $0 >&2 && exit 0;;
    esac
done

# remove the options we parsed above.
shift `expr $OPTIND - 1`

# exit if unused args still remain
if [ $# -gt 0 ]; then
    printhelp $0 >&2
    exit 1
fi

# Atleast interval and count should be specified
[ $count == 0 ] && printhelp $0 >&2 && exit 1
[ $interval == 0 ] && printhelp $0 >&2 && exit 1


# if namenode is specified, check if passwordless ssh is possible
if [ -n "$namenode" ]; then
  ssh -q $USER@$namenode exit
  if [ $? != "0" ]
  then
     echo "hadoop namenode:$namenode specified. ssh to $namenode failed! exiting.."
     printhelp $0 >&2
     exit 1
  fi
fi


timestamp=$('date' | sed "s/[ \t][ \t]*//g")
nodes=(${remotenodes//:/ })
filter_windows=(${filter_windows//:/ }) #filter_window = [ rampup steadystate ]


if [ ${apply_filter} == 1 ]
then
  if [ ${#filter_windows[@]} != "2" ]
  then
    echo "Specify rampup and steadystate times!"
    printhelp $0 >&2
    exit 1
  fi
  tmp_time=$((${filter_windows[0]} + ${filter_windows[1]} ))
  sleeptime=$(($count*$interval))
  if [ $sleeptime -lt $tmp_time ];then
    echo "Total sample interval less than rampup+steadystate time!"
    printhelp $0 >&2
    exit 1
  fi
fi

sar_cmd_args="-bBdqrRSuvwWy -I SUM -I XALL -n ALL -u ALL -P ALL"

# start sar on remote hosts
if [[ $useremote == 0 ]]; then
  mkdir -p $workdir/$timestamp
  sar -o $workdir/$timestamp/log.bin $sar_cmd_args $interval $count >/dev/null 2>&1 &
else
  for cpu in ${nodes[@]}; do
    ssh $USER@$cpu mkdir -p $workdir/$cpu/$timestamp
    ssh $USER@$cpu "sar -o $workdir/$cpu/$timestamp/log.bin $sar_cmd_args $interval $count >/dev/null 2>&1 &"
  done
fi

# copy scripts and start emon on remote hosts
if [ $collect_emon == 1 ]; then
  echo "Initiating emon data collector..."
  if [[ $useremote == 0 ]]; then
    mkdir -p $workdir/$timestamp/emon
    scp -qr $script_dir/emon $workdir/$timestamp
    ($workdir/$timestamp/emon/start_emon >/dev/null 2>&1 &)
  else
    for cpu in ${nodes[@]}; do
      ssh $USER@$cpu mkdir -p $workdir/$cpu/$timestamp/emon
      scp -qr $script_dir/emon $USER@$cpu:$workdir/$cpu/$timestamp
      ssh $USER@$cpu "$workdir/$cpu/$timestamp/emon/start_emon > /dev/null 2>&1 &"
    done
  fi
fi

sleeptime=$(($count*$interval))
buffer=5
sleeptime=$(($sleeptime + $buffer ))


###########################################################################################
# hosts have started sar data collection
# start background script if specified 
###########################################################################################

if [ -n "$command_exec" ]; then
  echo "starting $command_exec in background.."
  mkdir -p $outputdir/$timestamp/log
  ($command_exec > $outputdir/$timestamp/job.log 2>&1 &)
fi

###########################################################################################
# if namenode specified, poll and gather completed jobs in duration
# else sleep until time elapse
###########################################################################################


if [ -n "$namenode" ]; then
  echo "mapreduce jobs watchdog initiated for $sleeptime seconds.."
  mapred_jobs=$(${mapredjobs_watchdog_script_fullpath}  $sleeptime)
else
  echo "waiting for $sleeptime seconds to elapse.."
  sleep $sleeptime
fi



###########################################################################################
# copy log.bin from remote hosts locally
# textify log.bin, parse individual system data and store
###########################################################################################

sar_log_data=$outputdir/$timestamp/log/sar
node_list_for_r=


if [[ $useremote == 0 ]]; then
  mkdir -p $outputdir/$timestamp/log/sar/$localhost
  cp $workdir/$timestamp/log.bin $sar_log_data/$localhost
  sar -f $sar_log_data/$localhost/log.bin $sar_cmd_args  > $sar_log_data/$localhost/log.txt
  echo "Parsing sar data on $localhost..."
  $sar_log_splitter $sar_log_data/$localhost/log.txt $sar_log_data/$localhost/
  node_list_for_r=\"$localhost\"
else
  for cpu in ${nodes[@]}; do
    mkdir -p $outputdir/$timestamp/log/sar/$cpu
    scp -r $USER@$cpu:$workdir/$cpu/$timestamp/log.bin $sar_log_data/$cpu
    sar -f $sar_log_data/$cpu/log.bin $sar_cmd_args  > $sar_log_data/$cpu/log.txt
    echo "Parsing sar data on $cpu..."
    $sar_log_splitter $sar_log_data/$cpu/log.txt $sar_log_data/$cpu/
    if [ -z $node_list_for_r ]; then
      node_list_for_r=\"$cpu\"
    else
      node_list_for_r=$node_list_for_r,\"$cpu\"
    fi
  done
fi



###########################################################################################
# copy emon data from remote hosts
###########################################################################################
emon_output=$outputdir/$timestamp/log/emon
if [ $collect_emon == 1 ]; then
  echo "Stopping and collecting emon data..."
  if [[ $useremote == 0 ]]; then
    mkdir -p $emon_output/$localhost
    ($workdir/$timestamp/emon/kill_emon >/dev/null 2>&1)
    cp -r $workdir/$timestamp/emon/*.dat $emon_output/$localhost
  else
    for cpu in ${nodes[@]}; do
      mkdir -p $emon_output/$cpu
      ssh $USER@$cpu "$workdir/$cpu/$timestamp/emon/kill_emon >/dev/null 2>&1"
      scp -qr $USER@$cpu:$workdir/$cpu/$timestamp/emon/*.dat $emon_output/$cpu 
    done
  fi
fi

###########################################################################################
# hack really. Find unique disk dev id's (typically dev-8,dev-16 etc)
###########################################################################################

unique_dev_list=$(find ${sar_log_data} -name disk.csv | xargs cat \
                  | awk -F, '{print $2}' | sort | uniq | grep -v DEV)
unique_dev_list_for_r=
for udev in $unique_dev_list; do
  if [ -z $unique_dev_list_for_r ]; then
    unique_dev_list_for_r=\"$udev\"
  else
    unique_dev_list_for_r=$unique_dev_list_for_r,\"$udev\"
  fi
done


###########################################################################################
# build filter options. Visualize only steadystate (if specified -a)
###########################################################################################
if [ ${apply_filter} == 1 ]
then
  rampup_time=$((${filter_windows[0]}))
  rampdown_start=$((${filter_windows[0]} + ${filter_windows[1]} ))
  filter_start=$(find ${sar_log_data} -name cpu.csv | xargs cat | awk -F, '{print $1}' \
                 | sort | uniq | head -${rampup_time} | tail -1)
  filter_end=$(find ${sar_log_data} -name cpu.csv | xargs cat | awk -F, '{print $1}' \
                 | sort | uniq | head -${rampdown_start} | tail -1)
  filter_choice="TRUE"
else
  filter_choice="FALSE"
  filter_start="012:00:00 AM"
  filter_end="011:00:00 PM"
fi


###########################################################################################
# build options.r file with the sar data generated
# build final pdf with all the charts by calling R CMD BATCH
###########################################################################################

mkdir -p $outputdir/$timestamp/pdf

jobs_detail_file="job_${timestamp}_detailed.csv"
jobs_summary_file="job_${timestamp}_summary.csv"
echo "log.dir=\"log/sar\"
sar.pdf=\"pdf/sar_summary_$timestamp.pdf\"
mapred.pdf=\"pdf/mapred_$timestamp.pdf\"
jobsummary.csv=\"log/mapred/${jobs_summary_file}\"
jobdetail.csv=\"log/mapred/${jobs_detail_file}\"
node.name=c(${node_list_for_r})
disk.dev=c(${unique_dev_list_for_r})
nnodes <- length(node.name)
breaks.log=TRUE
filter.choice=${filter_choice}
filter.start=\"${filter_start}\"
filter.end=\"${filter_end}\"
max.decimals=2
" > $outputdir/$timestamp/options.r

cp $script_dir/*.r $outputdir/$timestamp/
cp $script_dir/buildpdf.sh $outputdir/$timestamp/
if [ $disable_viz == 0 ]; then
  echo "Building sar visualizer as pdf..."
  (cd $outputdir/$timestamp/ && R CMD BATCH visualizeR.r)
else
  echo "Note: sar visualizer is disabled"
fi


###########################################################################################
# if hadoop cluster, parse completed mapredjobs during duration specified
###########################################################################################

mkdir -p $outputdir/$timestamp/log/mapred/
mapred_details_file=$outputdir/$timestamp/log/mapred/$jobs_detail_file
[ -e $mapred_details_file ] && rm -rf ${mapred_details_file}
mapred_summary_file=$outputdir/$timestamp/log/mapred/$jobs_summary_file
[ -e $mapred_summary_file ] && rm -rf ${mapred_summary_file}

# d: disable header?
# s: print summary or details?

if [ -n "$namenode" ]; then
  if [ -n "$mapred_jobs" ]; then
    for jobid in $mapred_jobs; do
      if [ -e $mapred_details_file ]; then
        (${process_mapred_script_fullpath} -d 1 -s 0 -n $namenode -l $hadooplog  $jobid $jobid >> ${mapred_details_file})
        (${process_mapred_script_fullpath} -d 1 -s 1 -n $namenode -l $hadooplog  $jobid $jobid >> ${mapred_summary_file})
      else
        (${process_mapred_script_fullpath} -d 0 -s 0 -n $namenode -l $hadooplog  $jobid $jobid > ${mapred_details_file})
        (${process_mapred_script_fullpath} -d 0 -s 1 -n $namenode -l $hadooplog  $jobid $jobid > ${mapred_summary_file})
      fi
    done
  fi
fi


if [ -n "$namenode" ]; then
  if [ $disable_viz == 0 ]; then
    echo "Building mapreduce visualizer as pdf..."
    (cd $outputdir/$timestamp/ && R CMD BATCH ${script_dir}/mapred/mapred.r)
  else
#   time filter does not apply for mapreduce
    echo "Building mapreduce visualizer as pdf..."
    (cd $outputdir/$timestamp/ && R CMD BATCH ${script_dir}/mapred/mapred.r)
#    echo "Note: mapreduce vizualiser is disabled"
  fi
fi

# TODO: need to cleanup workdir in remote nodes


