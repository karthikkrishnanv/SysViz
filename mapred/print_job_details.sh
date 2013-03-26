#!/bin/bash 

###########################################################################################
# print detailed job counter data of mapreduce jobs
# script needs to be run locally on namenode
# change hadoop history dir (default:/var/log/hadoop/history/done) if needed
###########################################################################################

print_jobdetails_usage() {
	  echo "Usage: `basename $1` [-d] [jobid_start] [jobid_end] [hadoop_log]"
		echo "                            [-d]          # disable printing header in csv?"
		echo "                            [-s]          # print summary vs detailed?"
		echo "                            [jobid_start] # mapreduce jobid start"
		echo "                            [jobid_end]   # mapreduce jobid end"
		echo "                            [log]         # hadoop log dir"
}

while getopts hv:d:s: opt; do
  case "$opt" in
    h)  print_jobdetails_usage $0 && exit 0;;
    v)  echo "`basename $0` version 0.1";;
    d)  disable_header=$OPTARG;;
    s)  print_summary=$OPTARG;;
   \?)  print_jobdetails_usage $0 >&2 && exit 0;;
    esac
done

if [ ${print_summary} -eq 1 ]; then
  print_details=0
else
  print_details=1
fi

# remove the options we parsed above.
shift `expr $OPTIND - 1`

# exit if unused args still remain
if [ $# -ne 3 ]; then
    print_jobdetails_usage $0 >&2
    exit 1
fi

basedir=$(dirname $0)
hadoop_history_dir="/var/log/hadoop/history/done"
start_job=$1
end_job=$2
hadoop_history_dir=$3


function check_job_complete() {
	id=$1
	status=$(hadoop job -status $id | grep completion | grep 1.0 | wc -l)
	if [ $status != "2" ]; then
	  lost_job=$(hadoop job -status $id | grep "Could not find job" | wc -l)
### Not sure what to do when previous completed jobs are not available!
  	echo "job $id not complete"
	  echo $(hadoop job -status $id | grep completion)
		echo "Exiting.."
		exit 1
	fi
}

##########################################################################################
# sanity check if atleast the start and end jobs are complete
##########################################################################################

check_job_complete $start_job
check_job_complete $end_job


##########################################################################################
# find corresponding conf file for input jobid
##########################################################################################

function find_conf_file() {
	dir=$hadoop_history_dir
	pattern=$1
	conf_job_files=$(/bin/ls $dir/$pattern)
	if [ $? != "0" ]; then
		# did not print error message to not tamper w/ csv with other jobids
		exit 1
	fi
	echo $(/bin/find $dir/$pattern -name "*.xml")
}

function find_job_file() {
	dir=$hadoop_history_dir
	pattern=$1
	conf_job_files=$(/bin/ls $dir/$pattern)
	if [ $? != "0" ]; then
		# did not print error message to not tamper w/ csv with other jobids
		exit 1
	fi
	echo $(/bin/find $dir/$pattern -not -name "*.xml")
}


dir=$hadoop_history_dir
pattern="*"$start_job"*"
start_files=$(/bin/ls $dir/$pattern)
pattern="*"$end_job"*"
end_files=$(/bin/ls $dir/$pattern)


start_conf_file=$(find_conf_file "*"$start_job"*")
start_job_file=$(find_job_file "*"$start_job"*")
end_conf_file=$(find_conf_file "*"$end_job"*")
end_job_file=$(find_job_file "*"$end_job"*")

all_conf_files=$(/bin/find $dir/*.xml -cnewer $start_conf_file -and ! -cnewer $end_conf_file)
all_job_files=$(/bin/find $dir -not -name "*.xml*" -not -name "*.crc*" \
						    -cnewer $start_job_file -and ! -cnewer $end_job_file )

declare -a final_conf_files
declare -a final_job_files

index=0
final_job_files[$index]=$start_job_file
for v in $all_job_files;do
	index=$[$index+1]
	final_job_files[$index]=$v
done

index=0
final_conf_files[$index]=$start_conf_file
for v in $all_conf_files;do
	index=$[$index+1]
	final_conf_files[$index]=$v
done

control_c()
# run if user hits control-c
{
  echo -en "\n*** mapred details print interrupted. Exiting..!***\n"
  cleanup
  exit $?
}
 
# trap keyboard interrupt (control-c)
trap control_c SIGINT

###########################################################################################
# x: print header
# d: print detailed job counters also
# s: print only summary data
###########################################################################################

if [ $disable_header -eq 1 ]; then
	for i in "${!final_job_files[@]}"; do
		if [ -f ${final_job_files[$i]} ]; then
			$basedir/summary.py  -v  -j ${final_job_files[$i]} -c ${final_conf_files[$i]} \
												   -x 0 -d ${print_details} -s ${print_summary}
		fi
	done
else
	for i in "${!final_job_files[@]}"; do
  		if [ ${i} -eq 0 ]; then
				if [ -f ${final_job_files[$i]} ]; then
					$basedir/summary.py -v  -j ${final_job_files[$i]} -c ${final_conf_files[$i]} \
															-x 1 -d ${print_details} -s ${print_summary}
			fi 
		else
			if [ -f ${final_job_files[$i]} ]; then
				$basedir/summary.py  -v  -j ${final_job_files[$i]} -c ${final_conf_files[$i]} \
															-x 0 -d ${print_details} -s ${print_summary}
			fi
		fi
	done
fi




