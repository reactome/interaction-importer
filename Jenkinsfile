import groovy.json.JsonSlurper
// This Jenkinsfile is used by Jenkins to run the graph-importer step of Reactome's release.
// It requires that OrthoInferenaceStableIDHistory has run successfully
def currentRelease
def previousRelease
pipeline
{
	agent any
	stages
	{
		// This stage checks that upstream projects OrthoinferenceStableIdentifierHistory were run successfully for their last build.
		stage('Check OrthoinferenceStableIdentifierHistory builds succeeded')
		{
			steps
			{
				script
				{
					currentRelease = (pwd() =~ /Releases\/(\d+)\//)[0][1];
					previousRelease = (pwd() =~ /Releases\/(\d+)\//)[0][1].toInteger() - 1;
					// This queries the Jenkins API to confirm that the most recent builds of AddLinks-Download and Orthoinference were successful.
					checkUpstreamBuildsSucceeded("OrthoinferenceStableIdentifierHistory", "$currentRelease")
				}
			}
		}

		// This stage builds the jar file using maven.
		stage('Setup: Build jar file')
		{
			steps
			{
				script
				{
					sh "mvn clean package -DskipTests"
				}
			}
		}
		stage('Main: Run interaction-importer')
		{
			steps
			{
				script
				{
					withCredentials([usernamePassword(credentialsId: 'graphdbCredentials', passwordVariable: 'pass', usernameVariable: 'user')])
					{
						sh """java -jar target/InteractionImporter-exec.jar -h localhost -d reactome -u $user -p $pass """
					}
				}
			}
		}
		// TODO: Maybe use the actual "post" directive?
		stage('Post: Archive logs, run qa')
		{
			steps
			{
				script
				{
					sh "mkdir -p archive/${currentRelease}/logs"
					sh "mv logs/* archive/${currentRelease}/logs"
				}
			}
			stages
			{
				stage('graph-qa')
				{
					steps
					{
						checkout changelog: false, poll: false, scm: [$class: 'GitSCM', branches: [[name: '*/master']], doGenerateSubmoduleConfigurations: false, extensions: [], submoduleCfg: [], userRemoteConfigs: [[credentialsId: 'github credentials', url: 'https://github.com/reactome/graph-qa.git']]]
						dir('./interaction-importer')
						{
							script
							{
								sh "mvn clean package -DskipTests"
							}
							withCredentials([usernamePassword(credentialsId: 'graphdbCredentials', passwordVariable: 'graphdbPassword', usernameVariable: 'graphdbUser')])
							{
								script
								{
									def qaSummary = sh(returnStdout: true, script: "java -jar target/graph-qa-exec.jar -h localhost -u $graphdbUser -p $graphdbPassword -o ./$currentRelease -v").trim()
									sh "tar -czf ./$currentRelease.tgz ./$currentRelease"
									emailext attachmentsPattern: "$currentRelease.tgz", body: '''Graph-qa has finished. Summary is:

$qaSummary

Detailed reports are attached.''', subject: "graph-qa results", to: "reactome-developer@reactome.org"
								}
							}
						}
					}
				}
			}
		}
	}
}

// Utility function that checks upstream builds of this project were successfully built.
def checkUpstreamBuildsSucceeded(String stepName, String currentRelease)
{
	def statusUrl = httpRequest authentication: 'jenkinsKey', validResponseCodes: "${env.VALID_RESPONSE_CODES}", url: "${env.JENKINS_JOB_URL}/job/$currentRelease/job/$stepName/lastBuild/api/json"
	if (statusUrl.getStatus() == 404)
	{
		error("$stepName has not yet been run. Please complete a successful build.")
	}
	else
	{
		def statusJson = new JsonSlurper().parseText(statusUrl.getContent())
		if(statusJson['result'] != "SUCCESS")
		{
			error("Most recent $stepName build status: " + statusJson['result'] + ". Please complete a successful build.")
		}
	}
}
