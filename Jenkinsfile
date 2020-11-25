pipeline {
    agent any 
    stages {
	stage('Install dependencies and download data') {
	    steps {
		echo "Dependencies"
		sh """
	        source /var/lib/jenkins/py3env/bin/activate
	
		cd ${env.WORKSPACE}
		pip install -r requirements.txt

                echo ${env.ghprbActualCommit}
                mkdir -p /tmp/${env.ghprbActualCommit} 
                cd /tmp/${env.ghprbActualCommit}
                rm -rf qcore
                git clone https://github.com/ucgmsim/qcore.git

		mkdir -p /tmp/${env.ghprbActualCommit}/build	
		cd /tmp/${env.ghprbActualCommit}/build 
		wget https://qc-s3-autotest.s3-ap-southeast-2.amazonaws.com/testing/slurm_gm_workflow/SGMW_bins.zip
		wget https://qc-s3-autotest.s3-ap-southeast-2.amazonaws.com/testing/slurm_gm_workflow/SGMW_usr_lib.zip

		unzip -q SGMW_bins.zip
		unzip -q SGMW_usr_lib.zip
		rm *.zip
	
		mkdir -p ${env.WORKSPACE}/sample0
		cd ${env.WORKSPACE}/sample0
		wget https://qc-s3-autotest.s3-ap-southeast-2.amazonaws.com/testing/slurm_gm_workflow/PangopangoF29_HYP01-10_S1244.zip
		unzip -q PangopangoF29_HYP01-10_S1244.zip
		rm *.zip
		"""
	    }
	}
        stage('Run regression tests') {
            steps {
                echo "Run pytest through docker: To avoid root writing temp files in workspace, copy files into docker's filesystem first" 
		sh """
		docker run  -v /tmp/${env.ghprbActualCommit}/qcore:/home/root/git/qcore -v ${env.WORKSPACE}:/home/root/git/slurm_gm_workflow -v /tmp/${env.ghprbActualCommit}/build/bins:/home/root/bins -v /tmp/${env.ghprbActualCommit}/build/usr_lib/python3.6:/usr/local/lib/python3.6 sungeunbae/qcore-ubuntu-tiny bash -c "
		cp -r /home/root/bins/* /;
		mkdir -p /home/root/test/qcore
		cp -r /home/root/git/qcore/* /home/root/test/qcore;
		cd /home/root/test/qcore;
		python setup.py install;
		mkdir -p /home/root/test/slurm_gm_workflow;
		cp -r /home/root/git/slurm_gm_workflow/* /home/root/test/slurm_gm_workflow;
		cd /home/root/test/slurm_gm_workflow;
		export PYTHONPATH=/home/root/test/slurm_gm_workflow;
		pytest -vs --ignore testing/test_manual_install &&
		pytest --black --ignore=testing;"
		"""
            }
        }
        stage('Teardown') {
            steps {
                echo 'Tear down the environments'
		sh """
		rm -rf /tmp/${env.ghprbActualCommit}/*
		"""
            }
        }
    }
}
