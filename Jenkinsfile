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
	PYTHONPATH=/tmp/${env.JOB_NAME}/qcore:$PYTHONPATH

        ln -s $HOME/data/testing/slurm_gm_workflow/SGMW /tmp/${env.JOB_NAME}/build
        cd /tmp/${env.JOB_NAME}/sample0
        cp -r $HOME/data/testing/slurm_gm_workflow/PangopangoF29_HYP01-10_S1244/* .

		"""
	    }
	}
        stage('Run regression tests') {
            steps {
                echo "Run pytest"
		sh """
		source /var/lib/jenkins/py3env/bin/activate
		ln -s /tmp/${env.JOB_NAME}/sample0 ${env.WORKSPACE};
        export PYTHONPATH=/tmp/${env.JOB_NAME}/qcore:${env.WORKSPACE};
        pytest -vs --ignore=testing/test_manual_install --ignore-glob="scripts/*" && pytest --black --ignore=testing;
		"""
            }
        }
    }
    post {
	always {
                echo 'Tear down the environments'
		sh """
		rm -rf /tmp/${env.JOB_NAME}/*
		"""
            }
    }

}
