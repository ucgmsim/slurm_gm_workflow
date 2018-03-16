#!/bin/bash
#get date of today
year=`date +%Y`
month=`date +%m`
day=`date +%d`

period=`echo 7 | bc`


#check for underflow of day (when executed at start of the month)
list_31days='03 05 07 08 10 12'
list_30days='4 6 9 11'

#day=1
#month=3
#start_day=`echo 1-7 | bc`
start_month=$month
start_year=$year
#get date for 7days ago
start_day=`echo $day-$period | bc`


#special case for Feb
if (( $start_day <  1)); then
    #check the maximum day of the previous month
    if (( $month-1 == 2));then
       #check if its Feb last month 
        echo "Feb"
        start_day=`echo 28+$day-$period | bc`
        start_month=`echo $month-1 | bc`
    elif (( $month == 1));then
        start_day=`echo 31+$day-$period | bc`
        start_month=12
        #years need to modified, since its last year
        start_year=`echo $year-1 | bc`
    else
        #not Feb nor Dec, get max day by checking list
        [[ $list_31days =~ (^|[[:space:]])$month($|[[:space:]]) ]] && bigmonth=1 || bigmonth=0
        if (( $bigmonth == 1));then
            echo big
            start_month=`echo $month-1| bc`
            start_day=`echo 31+$day-$period | bc`
        else
            echo small
            start_month=`echo $month-1| bc`
            start_day=`echo 30+$day-$period | bc`
        fi
    fi
fi
start_date=$start_year-$start_month-$start_day
echo $start_date
sreport cluster AccountUtilizationByUser start=$start_date | grep nesi00213
