#!/usr/bin/env bash

if [ $# -lt 1 ]; then
  echo "Please provide a ROOT installation directory, and the version code"
  exit 1
fi
ROOT=$1
script_dir=`dirname "$0"`

function print_message {
    echo "***************"
    echo "$1"
    echo "***************"
}

if [[ ! -d ${ROOT}/share ]];then
    mkdir -p ${ROOT}/share
fi

SHARED_RC=${ROOT}/share/bashrc.uceq
if [[ -f $SHARED_RC ]];then
# preparing the file bashrc.uceq
    mv $SHARED_RC $SHARED_RC\_`date +%Y%m%d_%H%M%S`
fi
touch $SHARED_RC

echo "export project='$ROOT'" >> $SHARED_RC
echo "export nobackup='/nesi/nobackup/nesi00213'" >> $SHARED_RC

cat $script_dir/extra_bashrc_functions.sh >> $SHARED_RC
cat $script_dir/python_load_functions.sh >> $SHARED_RC
cat $script_dir/load_default_modules.sh >> $SHARED_RC

print_message "Add source $ROOT/share/bashrc.uceq to your .bashrc"
