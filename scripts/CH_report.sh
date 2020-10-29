#!/bin/bash
#get date of today
year=`date +%Y`
month=`date +%m`
day=`date +%d`

date_pattern="+%Y-%m-%d"

if [[ $# -lt 1 ]]; then
    echo "please provide a period number( e.g 7 for 7 days)"
    exit 1
fi 

#provided 2 args. start=$1, end=$2
if [[ $# -gt 1 ]]; then
    end_days_ago=`echo $2 | bc`
    end_date=`date --date="$((end_days_ago-1)) days ago" $date_pattern`
else
    end_date=`date $date_pattern --date tomorrow`
fi

start_days_ago=`echo $1 | bc`
start_date=`date --date="$start_days_ago days ago" $date_pattern`

echo $start_date - $end_date
sreport -M maui -t Hours cluster AccountUtilizationByUser -T billing Accounts=comm00213 start=$start_date end=$end_date
sreport -M mahuika -t Hours cluster AccountUtilizationByUser -T billing Accounts=comm00213 start=$start_date end=$end_date
sreport -M maui -t Hours cluster AccountUtilizationByUser -T billing Accounts=nesi00213 start=$start_date end=$end_date
sreport -M mahuika -t Hours cluster AccountUtilizationByUser -T billing Accounts=nesi00213 start=$start_date end=$end_date
