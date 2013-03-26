#!/bin/bash 

PATH=/bin:/sbin:/usr/bin:/usr/sbin:$PATH:.
irq_num=( 98 99 100 101 )


###########################################################################################
# split sar log.txt file and generate individual memory,cpu,disk data files
###########################################################################################

expected_args=2
if [ $# -ne $expected_args ]
then
  echo "Usage: `basename $0` [sar_log_txt] [output_dir]"
  exit $E_BADARGS
fi

sar_log_txt=$1
output_dir=$2

if [ ! -f $sar_log_txt ];
then
    echo "$sar_log_txt: File not found!"
fi

specialtag=0xa43cd  # random tag to ensure we don't delete anything else!

###########################################################################################
# format sar txt file to csv
###########################################################################################

function saferemove() {
  filename=$1
  echo $filename | grep -q ${specialtag} && rm -rf $filename
}

function format2csv() {
  randomtmp=_tmp007_${RANDOM}_${specialtag}
  cat $1 | awk '{ for (i=1; i<=NF; i++) printf "%s,", $i; printf "\n"; }'  | 
         sed 's/.$//g' | sed 's/,AM,/ AM,/g' | 
         sed 's/,PM,/ PM,/g' | sed '1 s/.*PM/Time/g' | 
         sed '1 s/.*AM/Time/g' > $randomtmp
  cp -f $randomtmp $1
  saferemove $randomtmp
  sync
}


function format2csvall() {
  all_files=$1
  for f in $all_files;do
    format2csv $f
  done
}

###########################################################################################
# system stats are separated by empty lines
# separate different stats on a per file basis in output _sysviz_split.{0,1..n}
###########################################################################################

sarviz_split_name=_sysviz_split_${specialtag}

function split_sar_records() {
  log_txt=$1
  temp_tag=_sysviz.${RANDOM}_${specialtag}
# rm -rf ${temp_tag}*
  csplit -q -z -f $temp_tag $1  '/^$/' {*}
  split_count=0
  for tmp_file in $temp_tag*;do
    split_count=$(($split_count + 1))
    sed '/^$/d' $tmp_file > ${sarviz_split_name}.$split_count
    saferemove $tmp_file
    format2csv ${sarviz_split_name}.$split_count
  done
  sync
}




###########################################################################################
# find log file associated with system data specified by tag
###########################################################################################

function pickfiles() {
  tag=$1
  all_files=$2
  for f in $sar_individual_files;do
    if_exists=$(cat $f | grep $tag | wc -l)
    if [ ${if_exists} == "0" ];then
        continue
    else
       echo $f " "
     fi
  done
}



###########################################################################################
# split log.txt into individual files
###########################################################################################
split_sar_records $sar_log_txt


sar_individual_files=$(ls -bt ${sarviz_split_name}*)

# not super clean. works for now. Toast if sar tag/format changes!

SYSTEM_TAG="idle cswch INTR bread kbmemfree kbswpfree await rxkB"

for t in $SYSTEM_TAG; do
  echo "Parsing for " $t
  files=$(pickfiles $t)
  declare -a filtered=($files)
  header=$(head -1 ${filtered[0]})
  if [ ${t} == "idle" ]; then
    echo $header > $output_dir/cpu.csv
    tac $files | grep -v Average | grep all | tac >> $output_dir/cpu.csv
  fi
  if [ ${t} == "cswch" ]; then
    tac $files | grep -v Average | tac > $output_dir/cswch.csv
  fi
  if [ ${t} == "INTR" ]; then
    echo $header > $output_dir/INTR.csv
    tac $files | grep sum | grep -v Average |tac   >>  $output_dir/INTR.csv
    intr_file=$output_dir/IRQ_intr.csv
    echo $header > $intr_file
    for irq in "${irq_num[@]}"; do
      irq_pattern=",$irq,"
      tac $files | grep  $irq_pattern | grep -v Average |tac >>  $intr_file
    done
  fi
  if [ ${t} == "bread" ]; then
    tac $files | grep -v Average |tac  >>  $output_dir/disk_total.csv
  fi
  if [ ${t} == "kbmemfree" ]; then
    tac $files | grep -v Average |tac  >  $output_dir/memory.csv
  fi
  if [ ${t} == "kbswpfree" ]; then
    tac $files | grep -v Average |tac  >  $output_dir/swap.csv
  fi
  if [ ${t} == "await" ]; then
    tac $files | grep -v Average  |tac  >  $output_dir/disk.csv
  fi
  if [ ${t} == "rxkB" ]; then
    tac $files | grep -v Average |tac  >>  $output_dir/eth.csv
  fi
done

for ftmp in $sar_individual_files; do
  [ -e $ftmp ] && saferemove ${ftmp}
done
sync
