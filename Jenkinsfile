pipeline {
    agent any
    
    environment {
        TEMP_DIR="/tmp/${env.JOB_NAME}/${env.ghprbActualCommit}",
        VIRT_ENV="$TEMP_DIR/venv"
    }
    stages {

        stage('Settin up env') {
            steps {
                echo "[[ Start virtual environment ]]"
                sh """
                    echo "[ Current directory ] : " `pwd`
                    echo "[ Environment Variables ] "
                    env
# Each stage needs custom setting done again. By default /bin/python is used.
                    source /var/lib/jenkins/py3env/bin/activate
                    mkdir -p $TEMP_DIR
                    python -m venv $VIRT_ENV
# activate new virtual env
                    source /tmp/${env.JOB_NAME}/${env.ghprbActualCommit}/venv/bin/activate
                    echo "[ Python used ] : " `which python`
                    cd ${env.WORKSPACE}
                    echo "[ Install dependencies ]"
                    pip install -r requirements.txt
                    echo "[ Install qcore ]"
                    cd $TEMP_DIR
                    rm -rf qcore
                    git clone https://github.com/ucgmsim/qcore.git
                    cd qcore
                    python setup.py install --no-data
                """
            }
        }

        stage('Run regression tests') {
            steps {
                echo '[[ Run pytest ]]'
                sh """
# activate virtual environment again
                    source $VIRT_ENV/bin/activate
                    echo "[ Python used ] : " `which python`
                    cd ${env.WORKSPACE}
                    echo "[ Installing ${env.JOB_NAME} ]"
# full installation is not possible as it takes more than 3.0Gb for building and kills the server
#                   python setup.py install
                    echo "[ Linking bins and libs ]"
                    rm -rf build
                    ln -s $HOME/data/testing/slurm_gm_workflow/SGMW build
                    echo "[ Linking test data ]"
                    rm -rf sample0
                    mkdir sample0
                    cp -r $HOME/data/testing/${env.JOB_NAME}/PangopangoF29_HYP01-10_S1244/* sample0/
                    echo "[ Run test now ]"
                    pytest -vs --ignore=testing/test_manual_install --ignore-glob="scripts/*" && pytest --black --ignore=testing; 
                """
            }
        }
    }

    post {
        always {
                echo 'Tear down the environments'
                sh """
                    rm -rf $TEMP_DIR
                """
            }
    }
}
