pipeline {
     agent any
     stages {
         stage('Build') {
             steps {
                 'echo "Hello World"'
                 '''
                     echo "Multiline shell steps works too"
                     ls -lah
                 '''
             }
         }
         stage('Upload to AWS') {
              steps {
                  withAWS(region:'us-east-1',credentials:'Jenkins_AWS') {
                  'echo "Uploading content with AWS creds"'
                      s3Upload(pathStyleAccessEnabled: true, payloadSigningEnabled: true, file:'outlier_week.py', bucket:'big-data-oct-2022-lf')
                  }
              }
         }
     }
}