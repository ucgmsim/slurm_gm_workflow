#!/usr/bin/env bash

if [ $# -lt 2 ]; then
  echo "Please provide a ROOT installation directory, and the version code"
  exit 1
fi
ROOT=$1
version=$2
script_dir=`dirname "$0"`
function test_dir {
    [ -d $1 ] && echo "$1 is available" || echo "Please get a version of the $1 library later on $2"
}
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
touch ${ROOT}/share/bashrc.uceq

echo "export gmsim='$ROOT'" >> ${ROOT}/share/bashrc.uceq
echo "export nobackup='/nesi/nobackup/nesi00213'" >> ${ROOT}/share/bashrc.uceq
echo 'export PATH=$PATH:'${ROOT}/workflow/scripts >> ${ROOT}/share/bashrc.uceq
echo "export PYTHONPATH=$ROOT/workflow:"'$PYTHONPATH' >> ${ROOT}/share/bashrc.uceq

cat $script_dir/extra_bashrc_functions.sh >> ${ROOT}/share/bashrc.uceq
cat $script_dir/python_load_functions.sh >> ${ROOT}/share/bashrc.uceq
cat $script_dir/load_default_modules.sh >> ${ROOT}/share/bashrc.uceq

print_message "Add source $ROOT/share/bashrc.uceq to your .bashrc"
