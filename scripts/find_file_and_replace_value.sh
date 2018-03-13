#!/bin/bash

#takes 4 args, 
#1. the path to find, 
#2. the file to find.
#3. the phrase to look for
#4. the phrase to replaced as

if [[ $# -lt 4 ]]; then
    echo "usage: ./find_and_replace.sh /path/to/search/ file_name phrase_to_search replacing_phrase"
    exit 1
fi

search_folder=$1
file_name=$2
old_txt=$3
new_txt=$4

for file in `find $search_folder -name $file_name`
do
    #echo $file
    sed -i -e "s?$old_txt?$new_txt?g" $file
done
