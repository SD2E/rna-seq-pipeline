#!groovy

pipeline {
    agent any
    environment {
        AGAVE_GET_DATA    = 1
        AGAVE_JOB_TIMEOUT = 1800
        AGAVE_JOB_GET_DIR = "job_output"
        AGAVE_DATA_URI    = "agave://data-tacc-work-cylu/sd2e-data/rnaseq-application/test_data"
        CONTAINER_REPO    = "rnaseq"
        CONTAINER_TAG     = "test"
        AGAVE_CACHE_DIR   = "${HOME}/credentials_cache/${JOB_BASE_NAME}"
        AGAVE_JSON_PARSER = "jq"
        AGAVE_TENANTID    = "sd2e"
        AGAVE_APISERVER   = "https://api.sd2e.org"
        AGAVE_USERNAME    = "sd2etest"
        AGAVE_PASSWORD    = credentials('sd2etest-tacc-password')
        REGISTRY_USERNAME = "sd2etest"
        REGISTRY_PASSWORD = credentials('sd2etest-dockerhub-password')
        REGISTRY_ORG      = credentials('sd2etest-dockerhub-org')
        PATH = "${HOME}/bin:${HOME}/sd2e-cloud-cli/bin:${env.PATH}"
        }
    stages {

        stage('Create Oauth client') { 
            steps {
                sh "make-session-client ${JOB_BASE_NAME} ${JOB_BASE_NAME}-${BUILD_ID}"
            }
        }
        stage('Build app container') { 
            steps {
                sh "apps-build-container -O ${REGISTRY_USERNAME} --image ${CONTAINER_REPO} --tag ${CONTAINER_TAG}"
            }
        }
        stage('Deploy to TACC.cloud') { 
            steps {
                sh "apps-deploy -T -O ${REGISTRY_USERNAME} --image ${CONTAINER_REPO} --tag ${CONTAINER_TAG}"
                sh "cat deploy-*"
            }
        }
        stage('Run a test job') { 
            steps {
                // Run job
                sh "run-test-job deploy-${AGAVE_USERNAME}-job.json ${AGAVE_JOB_TIMEOUT}"
                // Get outputs
                sh "get-test-job-outputs deploy-${AGAVE_USERNAME}-job.json.jobid"
                // Evaluate results
            }
        }
        stage('Validate results') {
            steps {
                sh "python -m pytest tests/validate_job --job-directory ${AGAVE_JOB_GET_DIR}"
            }
        }

    }
    post {
        always {
            sh "delete-session-client ${JOB_BASE_NAME} ${JOB_BASE_NAME}-${BUILD_ID}"
        }
        success {
            deleteDir()
        }
    }
}
