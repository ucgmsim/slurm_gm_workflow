#!/usr/bin/env bash

if [ $# -eq 0 ]; then
  echo "Please provide a ROOT installation directory"
  exit 1
fi
ROOT=$1

function test_dir {
    [ -d $1 ] && echo "$1 is available" || echo "Please get a version of the $1 library later on $2"
}
function print_message {
    echo "***************"
    echo "$1"
    echo "***************"
}

# check for directories that are needed on $ROOT
test_dir ${ROOT}/EMOD3D ${ROOT}
test_dir ${ROOT}/qcore ${ROOT}

echo "Preparing the necessary directories to run GM"
mkdir -p -v ${ROOT}/{VelocityModel,VelocityModels,StationInfo,workflow,RunFolder,RupModel,share}
chmod g+w ${ROOT}/{VelocityModel,VelocityModels,StationInfo,workflow,RunFolder,RupModel,share}

# preparing the file bashrc.uceq
rm -f ${ROOT}/share/bashrc.uceq
touch ${ROOT}/share/bashrc.uceq
echo "export gmsim='$ROOT'" >> ${ROOT}/share/bashrc.uceq
echo 'export PATH=$PATH:'${ROOT}/workflow >> ${ROOT}/share/bashrc.uceq
echo "export PYTHONPATH=$ROOT/qcore:$ROOT/workflow:"'$PYTHONPATH' >> ${ROOT}/share/bashrc.uceq

print_message "Add source $ROOT/share/bashrc.uceq to your .bashrc"

# copying the files to the workflow
cp -r ../scripts ${ROOT}/workflow/
cp -r ../templates ${ROOT}/workflow/
cp -r ../shared_workflow ${ROOT}/workflow/

touch ../scripts ${ROOT}/workflow/{scripts,templates,shared_workflow}/__init__.py

# Adding legacy install.sh
echo '#!/usr/bin/env bash

python '${ROOT}/workflow/scripts'/install.py
' > ${ROOT}/RunFolder/install.sh
chmod +x ${ROOT}/RunFolder/install.sh

# Create a JSON config file for python
python create_config_file.py ${ROOT}


