pipeline {
    agent any
    
    environment {
        TEMP_DIR="/tmp/${env.JOB_NAME}/${env.ghprbActualCommit}"
    }
    stages {

        stage('Setting up env') {
            steps {
                echo "[[ Start virtual environment ]]"
                sh """
                    echo "[ Current directory ] : " `pwd`
                    echo "[ Environment Variables ] "
                    env
# Each stage needs custom setting done again. By default /bin/python is used.
                    source /home/qcadmin/py310/bin/activate
                    mkdir -p $TEMP_DIR
                    python -m venv $TEMP_DIR/venv
# activate new virtual env
                    source $TEMP_DIR/venv/bin/activate
                    echo "[ Python used ] : " `which python`
                    cd ${env.WORKSPACE}
                    echo "[ Install dependencies ]"
                    pip install -r requirements.txt
                    echo "[ Install qcore ]"
                    cd $TEMP_DIR
                    rm -rf qcore
                    git clone https://github.com/ucgmsim/qcore.git
                    cd qcore
                    pip install -r requirements.txt
                    python setup.py install --no-data
                    echo "[ Installing ${env.JOB_NAME} ]"
                    cd ${env.WORKSPACE}
                    cd ..
                    pip install -e ${env.JOB_NAME}
                    cd -
                    echo "[ Linking bins and libs ]"
                    rm -rf build
                    ln -s /home/qcadmin/data/testing/slurm_gm_workflow/SGMW build
                    echo "[ Linking test data ]"
                    rm -rf sample0
                    mkdir sample0
                    cp -r /home/qcadmin/data/testing/${env.JOB_NAME}/PangopangoF29_HYP01-10_S1244/* sample0/
                """
            }
        }

        stage('Run regression tests') {
            steps {
                echo '[[ Run pytest ]]'
                sh """
# activate virtual environment again
                    source $TEMP_DIR/venv/bin/activate
                    echo "[ Python used ] : " `which python`
                    cd ${env.WORKSPACE}

                    echo "[ Run test now ]"
                    pytest -vs '--ignore-glob=verification/*' && pytest --black --ignore=workflow/automation/tests;
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
