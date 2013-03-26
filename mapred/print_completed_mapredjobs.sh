#!/bin/bash 

# This program is mainly a mapreduce job watchdog
# Wait for a given duration and prints the list of all completed job during the time

PATH=/bin:/sbin:/usr/bin:/usr/sbin:$PATH:.

expected_args=1
if [ $# -ne $expected_args ]
then
  echo "Usage: `basename $0` [duration]"
  exit $E_BADARGS
fi

duration=$1

###########################################################################################
# check status of job and search for map/reduce completion to be 1.0
# hack really and need to modify if hadoop status query change
###########################################################################################

function check_job_complete() {
  jobid=$1
  map_complete=$(hadoop job -status $1 | grep map | 
                 grep completion | grep 1.0 | wc -l)
  reduce_complete=$(hadoop job -status $1 | 
                    grep reduce | grep completion | grep 1.0 | wc -l)
  if [[ ${map_complete} == 1 && ${reduce_complete} == 1 ]]; then
    echo "1"
  else
    echo "0"
  fi

}

###########################################################################################
# find list of all completed mapred jobs within the duration specified
# keep polling for running mapred jobs until time elapse and filter completed jobs
###########################################################################################


function get_completed_mapred_jobs()
{
  total=$1
  alljobs=""
  interval=5

  for (( i = 0; i < $total; i += $interval )); do
    currjob=$(hadoop job -list | grep job_ | awk -F' ' '{print $1}' | tr '\n' ' ')
    alljobs=$(echo  ${alljobs} && echo ${currjob})
    alljobs=$(echo $alljobs | xargs -n1 | sort -u)
    sleep $interval
  done
  uniqjobs=$(echo $alljobs | xargs -n1 | sort -u)
  for job in $uniqjobs; do 
    status=$(check_job_complete $job)
    if [[ ${status} == "1" ]]; then
      echo $job
    fi
  done
}

completed_jobs=$(get_completed_mapred_jobs $duration)
echo $completed_jobs


