== CloudFront LogAnalyzer
                                                                                                             
CloudFront LogAnalyzer is an analysis package for Amazon CloudFront Access
Logs. The application is built to run on Amazon Elastic MapReduce. It uses
Cascading(http://www.cascading.org) to generate the reports.  The application
reads in the location of your CloudFront logs and the date range for
consideration. It runs map reduce jobs to process these logs to produce the
following usage reports

- Overall Volume Report
- Client IP Report
- Object Popularity Report
- Edge Location Report


== Getting Setup

=== Building CloudFront LogAnalyzer

Building the tool is pretty straight forward. All the dependent jars
needed are present in the libs directory. After unpacking the tgz, run
ant jar to build create a jar file

  ant jar

The jar file logprocessor.jar gets generated in the build
directory.


=== Upload jar to your S3 bucket

Upload the generated jar to an S3 bucket you own and note down the jar
location (e.g s3n://<yourbucket>/<prefix>/logprocessor.jar).
You will be needing this location to run this using Amazon Elastic
MapReduce


=== Running the application using the Amazon Elastic MapReduce Web Console

Sign up for Amazon Elastic MapReduce (http://aws.amazon.com/elasticmapreduce/)
if you have not already done so. Login to the webconsole and follow the steps to
create a new JobFlow. Choose the CloudFront LogAnalyzer sample application in the 
JobFlow Wizard. This will pre fill the jar location and parameters with default
values that uses the jar and date in the public sample bucket. You can either go
with the defaults or  modify these values to use the jar file that you have 
uploaded and/or provide your cloudfront log files.

=== Running the application from the Ruby Client

Download the Amazon Elastic MapReduce Ruby Client from Resources -> Sample Code
and Librarues in http://aws.amazon.com/elasticmapreduce. Follow the instructions
provided to get it setup. The following command runs the CloudFront LogAnalyzer 
application wuth default parameters

  ./elastic-mapreduce --create --jar s3n://elasticmapreduce/samples/cloudfront/logprocessor.jar
  --args "-input,s3n://elasticmapreduce/samples/cloudfront/input,
  -output,s3n://<yourbucket>/<your-non-existant-path-to-output>" 

=== Jar Arguments

The application takes the following arguments. Use these arguments either from the console or from the 

  -input <path>  
      the s3 location of the cloudfront log files e.g s3n://mycloudfrontlogbucket/cloudfrontlogs

  -output 
     s3 location to which the reports get written to (e.g s3n://myoutputbucket/reports Note: It 
     is important that the subdirectory doesnt already exist)


  -start <start datetime in GMT formatted as yyyy-MM-dd-HH, Specify "any" to not constrain the start date> 
	(e.g 2009-02-01-01 => 1st February 2009, 01:00)(Defaults to "any")
  -end <end datetime in GMT formatted as yyyy-MM-dd-HH, Specify "any" to not constrain the end date> 
	(e.g 2009-03-31-23 => 31st March 2009, 23:00)(Defaults to "any")
  -timeBucket <time in seconds to group records by> 
        (Defaults to 300 seconds)
  -allReports 
        (If specified generates all the reports. Default behaviour is to generate all the reports)
  -clientIPReport 
        (If specified generates the Client IP Report)
  -objectPopularityReport 
        (If specified generates the Object Popularity Report)
  -overallVolumeReport 
        (If specified generates the Overall Volume Report)
  -edgeLocationReport 
        (If specified generates the Edege Location Report)

=== Accessing the generated reports

The generated reports can be found by downloading all the files in the
specified output path. The outputs are all tab delimted txt files. You
can safely ignore or delete the _runlogs_ directory in your output
path.
                                                           

 
