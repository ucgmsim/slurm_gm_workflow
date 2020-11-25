pipeline {
    agent any 
    stages {
	stage('Install dependencies and download data') {
	    steps {
		echo "Dependencies"
		sh """
	        source /var/lib/jenkins/py3env/bin/activate
		env

		cd ${env.WORKSPACE}
		pip install -r requirements.txt

                echo ${env.JOB_NAME}
                mkdir -p /tmp/${env.JOB_NAME} 
                cd /tmp/${env.JOB_NAME}
                rm -rf qcore
                git clone https://github.com/ucgmsim/qcore.git

		mkdir -p /tmp/${env.JOB_NAME}/build	
		cd /tmp/${env.JOB_NAME}/build 
		wget -q https://qc-s3-autotest.s3-ap-southeast-2.amazonaws.com/testing/slurm_gm_workflow/SGMW_bins.zip
		wget -q https://qc-s3-autotest.s3-ap-southeast-2.amazonaws.com/testing/slurm_gm_workflow/SGMW_usr_lib.zip

		unzip -q SGMW_bins.zip
		unzip -q SGMW_usr_lib.zip
		rm *.zip
	
		mkdir -p ${env.WORKSPACE}/sample0
		cd ${env.WORKSPACE}/sample0
		wget -q https://qc-s3-autotest.s3-ap-southeast-2.amazonaws.com/testing/slurm_gm_workflow/PangopangoF29_HYP01-10_S1244.zip
		unzip -q PangopangoF29_HYP01-10_S1244.zip
		rm *.zip
		
		"""
	    }
	}
        stage('Run regression tests') {
            steps {
                echo "Run pytest through docker: To avoid root writing temp files in workspace, copy files into docker's filesystem first" 
		sh """
		docker run  -v /tmp/${env.JOB_NAME}/qcore:/home/root/git/qcore -v ${env.WORKSPACE}:/home/root/git/slurm_gm_workflow -v /tmp/${env.JOB_NAME}/build/bins:/home/root/bins -v /tmp/${env.JOB_NAME}/build/usr_lib:/home/root/libs sungeunbae/qcore-ubuntu-tiny bash -c "
		cp -rf /home/root/bins/* /;
		cd /home/root/libs;
		mkdir -p /usr/local/lib/python3.6;
		cp -r python3.6/* /usr/local/lib/python3.6;
		mkdir -p /home/root/test/qcore;
		cp -rf /home/root/git/qcore/* /home/root/test/qcore;
		cd /home/root/test/qcore;
		python setup.py install;
		mkdir -p /home/root/test/slurm_gm_workflow;
		cp -rf /home/root/git/slurm_gm_workflow/* /home/root/test/slurm_gm_workflow;
		cd /home/root/test/slurm_gm_workflow;
		export PYTHONPATH=/home/root/test/slurm_gm_workflow;
		pytest -vs --ignore testing/test_manual_install &&
		pytest --black --ignore=testing;"
		"""
            }
        }
    }
    post {
	always {
                echo 'Tear down the environments'
		sh """
		rm -rf /tmp/${env.JOB_NAME}/*
		docker container prune -f
		"""
            }
    }

}
