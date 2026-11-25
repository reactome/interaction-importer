import groovy.json.JsonSlurper
// This Jenkinsfile is used by Jenkins to run the interaction-importer step of Reactome's release:
// IntAct's interactions added to the graph graph-importer built and installed, as graph-importer's
// own -i did before. It requires that the graph-importer step has run successfully.
def currentRelease
def previousRelease
pipeline
{
	agent any
	stages
	{
		// This stage checks that the graph-importer step, which builds and installs the graph this
		// adds to, was run successfully for its last build.
		stage('Check GraphImporter build succeeded')
		{
			steps
			{
				script
				{
					currentRelease = (pwd() =~ /Releases\/(\d+)\//)[0][1];
					previousRelease = (pwd() =~ /Releases\/(\d+)\//)[0][1].toInteger() - 1;
					// This queries the Jenkins API to confirm that the most recent builds of AddLinks-Download and Orthoinference were successful.
					checkUpstreamBuildsSucceeded("GraphImporter", "$currentRelease")
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
						// No -f: IntAct's current file is downloaded, as graph-importer's -i did. -d is
						// the installed graph's database (graph-importer's Post stage). A graph that
						// already has interactions is refused, so a rerun cannot add them twice.
						// -m: fewer interactions than this and nothing is committed, so a file IntAct's
						// parser stopped part way through fails the step (release 98 had 78,872).
						sh """java -jar target/InteractionImporter-exec.jar -h localhost -d graph.db -m 70000 -u $user -p $pass """
					}
				}
			}
		}
		stage('Post: Archive logs')
		{
			steps
			{
				script
				{
					sh "mkdir -p archive/${currentRelease}/logs"
					sh "mv logs/* archive/${currentRelease}/logs"
				}
			}
		}
		// The graph checked once its interactions are in. A stage of its own: a declarative stage
		// has either steps or stages, not both.
		stage('Post: graph-qa')
		{
			steps
			{
				// In a folder of its own: checked out into the workspace, graph-qa's files would
				// overwrite this project's (its pom.xml).
				dir('./graph-qa')
				{
					checkout changelog: false, poll: false, scm: [$class: 'GitSCM', branches: [[name: '*/master']], doGenerateSubmoduleConfigurations: false, extensions: [], submoduleCfg: [], userRemoteConfigs: [[credentialsId: 'github credentials', url: 'https://github.com/reactome/graph-qa.git']]]
					script
					{
						sh "mvn clean package -DskipTests"
					}
					withCredentials([usernamePassword(credentialsId: 'graphdbCredentials', passwordVariable: 'graphdbPassword', usernameVariable: 'graphdbUser')])
					{
						script
						{
							def qaSummary = sh(returnStdout: true, script: "java -jar target/graph-qa-exec.jar -h localhost -u $graphdbUser -p $graphdbPassword -o ./$currentRelease -v").trim()
							sh "tar -czf ./${currentRelease}.tgz ./$currentRelease"
							// The attachment is found from the workspace, where graph-qa's folder is; the
							// body is a GString, so the summary is in it rather than its name.
							emailext attachmentsPattern: "graph-qa/${currentRelease}.tgz", body: """Graph-qa has finished. Summary is:

${qaSummary}

Detailed reports are attached.""", subject: "graph-qa results", to: "reactome-developer@reactome.org"
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
