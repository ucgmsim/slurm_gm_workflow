#script to change the current shell group to nesi00213 in case folder permission is not properly setup.
group=`id -g -n`
user_name=`whoami`
if [[ $group = $user_name ]]; then
    echo "changing group to nesi00213"
    newgrp 'nesi00213'
    exit
fi
