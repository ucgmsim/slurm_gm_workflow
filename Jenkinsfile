pipeline {
    agent any 
    stages {
	stage('Install dependencies and download data') {
	    steps {
		echo "Dependencies"
		sh """
	        source /var/lib/jenkins/py3env/bin/activate
	
		docker pull sungeunbae/qcore-ubuntu-tiny
		make -p {env.WORKSPACE}/build	
		cd ${env.WORKSPACE}/build 
		wget https://qc-s3-autotest.s3-ap-southeast-2.amazonaws.com/testing/slurm_gm_workflow/SGMW_bins.zip
		wget https://qc-s3-autotest.s3-ap-southeast-2.amazonaws.com/testing/slurm_gm_workflow/SGMW_usr_lib.zip		      unzip -q SGMW_bins.zip
		unzip -q SGMW_usr_lib.zip
		
		cd ${env.WORKSPACE}
		pip install -r requirements.txt

                echo ${currentBuild}
                mkdir -p /tmp/${currentBuild} 
                cd /tmp/${currentBuild}
                rm -rf qcore
                git clone https://github.com/ucgmsim/qcore.git

		mkdir -p ${env.WORKSPACE}/sample0
		cd ${env.WORKSPACE}/sample0
		wget https://qc-s3-autotest.s3-ap-southeast-2.amazonaws.com/testing/slurm_gm_workflow/PangopangoF29_HYP01-10_S1244.zip
		unzip -q PangopangoF29_HYP01-10_S1244.zip
		"""
	    }
	}
        stage('Run regression tests') {
            steps {
                echo 'Run pytest through docker' 
		sh """
		docker run  -v /tmp/${currentBuild}:/home/root/git -v ${env.WORKSPACE}:/home/root/git/slurm_gm_workflow -v ${env.WORKSPACE}/build/bins:/home/root/bins -v ${env.WORKSPACE}/build/usr_lib/python3.6:/usr/local/lib/python3.6 sungeunbae/qcore-ubuntu-tiny bash -c "
		cp -r /home/root/bins/* /;
		cp -r /home/root/git/qcore /home/root/qcore;
		cd /home/root/qcore;
		python setup.py install;
		cp -r /home/root/git/slurm_gm_workflow /home/root/slurm_gm_workflow;
		cd /home/root/slurm_gm_workflow;
		export PYTHONPATH=$PYTHONPATH:/home/root/slurm_gm_workflow;
		pytest -vs --ignore testing/test_manual_install &&
		pytest --block --ignore=testing;"
		"""
            }
        }
        stage('Teardown') {
            steps {
                echo 'Tear down the environments'
		sh """
		rm -rf /tmp/${currentBuild}/*
		"""
            }
        }
    }
}
