group=`id -g -n`
user_name=`whoami`
if [[ $group = $user_name ]]; then
    echo "changing group to nesi00213"
    newgrp 'nesi00213'
fi
