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
		rm -rf /tmp/${env.JOB_NAME}
                mkdir -p /tmp/${env.JOB_NAME}/sample0
                cd /tmp/${env.JOB_NAME}
                git clone https://github.com/ucgmsim/qcore.git
		cd qcore
		git checkout rmtree_ignore_error

		ln -s $HOME/data/testing/slurm_gm_workflow/SGMW /tmp/${env.JOB_NAME}/build
		cd /tmp/${env.JOB_NAME}/sample0
		cp -r $HOME/data/testing/slurm_gm_workflow/PangopangoF29_HYP01-10_S1244/* . 
		
		"""
	    }
	}
        stage('Run regression tests') {
            steps {
                echo "Run pytest through docker: To avoid root writing temp files in workspace, copy files into docker's filesystem first" 
		sh """
		docker run --rm  -v /tmp/${env.JOB_NAME}/qcore:/home/jenkins/qcore -v ${env.WORKSPACE}:/home/jenkins/slurm_gm_workflow -v /tmp/${env.JOB_NAME}/build/bins:/home/jenkins/bins -v /tmp/${env.JOB_NAME}/build/usr_lib:/home/jenkins/lib -v /tmp/${env.JOB_NAME}/sample0:/home/jenkins/slurm_gm_workflow/sample0 --user `id -u`:`id -g` sungeunbae/qcore-ubuntu-tiny bash -c "
		export PATH=/home/jenkins/bins/usr/bin:/home/jenkins/bins/usr/local/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin;
		export PYTHONPATH=/home/jenkins/lib/python3.6/dist-packages:/home/jenkins/slurm_gm_workflow;
		cd /home/jenkins/qcore;
		python setup.py install --user;
		cd  /home/jenkins/slurm_gm_workflow;
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
