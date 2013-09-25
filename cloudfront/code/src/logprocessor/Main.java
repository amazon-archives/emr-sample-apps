package logprocessor;

import java.io.IOException;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;

import logprocessor.enums.Columns;
import logprocessor.reporters.FieldAggregatedReporterAssembly;
import logprocessor.reporters.TimeBucketedReporterAssembly;
import logprocessor.s3copy.CopyFromS3;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.tools.DistCp;

import cascading.cascade.Cascade;
import cascading.cascade.CascadeConnector;
import cascading.cascade.Cascades;
import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.operation.Filter;
import cascading.operation.regex.RegexFilter;
import cascading.operation.regex.RegexParser;
import cascading.operation.text.DateParser;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.scheme.SequenceFile;
import cascading.scheme.TextLine;
import cascading.tap.Hfs;
import cascading.tap.Tap;
import cascading.tuple.Fields;

/**
 * This is the main point of entry for the program.
 * The S3 copy job and the Cascading flows are set up and executed here.
 */
public class Main {
    
  @SuppressWarnings("unchecked")
  private static Pipe createHeadPipe() {
    Fields cloudFrontFields = new Fields(Columns.DateTime, Columns.EdgeLocation, Columns.Bytes, Columns.IPAddress, Columns.Operation, Columns.Domain, Columns.Object, Columns.HTTPResponse, Columns.UserAgent);
    String w = "[\\s]+"; // whitespace regex
    String cfRegex = "([\\S]+[\\s]+[\\S]+)"  // DateTime
               + w + "([\\S]+)"              // EdgeLocation
               + w + "([\\S]+)"              // Bytes
               + w + "([\\S]+)"              // IPAddress
               + w + "([\\S]+)"              // Operation
               + w + "([\\S]+)"              // Domain
               + w + "([\\S]+)"              // Object
               + w + "([\\S]+)"              // HttpResponse
               + w + "[\\S]+"                //   (ignore junk)
               + w + "(.+)";                 // UserAgent
    int groups[] = {1,2,3,4,5,6,7,8,9};
    RegexParser regexParser = new RegexParser(cloudFrontFields, cfRegex, groups);
    Pipe pipe = new Pipe("head");

    // Filter all lines that begin with #
    Filter hashFilter = new RegexFilter( "^#.*", true);
    pipe = new Each(pipe, new Fields(Columns.Line), hashFilter);

    // Add the field parser
    pipe = new Each(pipe, new Fields(Columns.Line), regexParser, Fields.RESULTS);
    
    // Parse the date
    DateParser dateParser = new DateParser("yyyy-MM-dd\tHH:mm:ss");
    pipe = new Each(pipe, new Fields(Columns.DateTime), dateParser, Fields.ALL);
    
    return pipe;
  }

  /**
   * Main method that sets up S3 copy job and Cascading flows.
   */
  public static void main(String[] args) throws IOException, java.text.ParseException {
    Options options = createOptions();
    CommandLine cmd = null;
    try {
      cmd = new PosixParser().parse(options, args);
    } catch (ParseException ex ) {
      System.out.println("Error parsing the options");
      printHelpText(options);
      return;
    }

    if (cmd.hasOption("help")) {
      printHelpText(options);
      System.exit(0);
    }

    if (!cmd.hasOption("input") || !cmd.hasOption("output")) {
      printHelpText(options);
      return;
    }

    String inputPath = cmd.getOptionValue("input");
    inputPath = appendTrailingSlash(inputPath);

    String outputPath = cmd.getOptionValue("output");
    outputPath = appendTrailingSlash(outputPath);

    String startDate = "1970-01-01-00";   // Set something to span all old records
    if (cmd.hasOption("start") && !cmd.getOptionValue("start").equalsIgnoreCase("any")) {
      startDate = cmd.getOptionValue("start");
    }

    String endDate = getFormattedDate();  // Set the latest date in GMT
    if (cmd.hasOption("end") && !cmd.getOptionValue("end").equalsIgnoreCase("any")) {
      endDate = cmd.getOptionValue("end");
    }

    String tempPath = "hdfs:///home/hadoop/temp-output/";
    if (cmd.hasOption("tempOutput")) {
      tempPath = cmd.getOptionValue("tempOutput");
      tempPath = appendTrailingSlash(tempPath);
    }

    Integer delta = 300; // 5 minute grouping by default
    if (cmd.hasOption("timeBucket")) {
      delta = Integer.parseInt(cmd.getOptionValue("timeBucket"));
    }

    String tempIndex = tempPath.substring(0, tempPath.length()-1) + "-tempIndex.txt";
    String tempS3Copy = tempPath.substring(0, tempPath.length()-1) + "-s3copy/";   
    String intermediateOut = tempPath.substring(0, tempPath.length()-1) + "-cf-reports/";

    List<SubAssembly> reportsToGenerate = new ArrayList<SubAssembly>();
    boolean reportFlag = false;
    if (!cmd.hasOption("allReports")) {
      if (cmd.hasOption("overallVolumeReport")) {
        reportsToGenerate.add(new TimeBucketedReporterAssembly("overall-volume", delta));
        reportFlag = true;
      }
      if (cmd.hasOption("objectPopularityReport")) {
        reportsToGenerate.add(new FieldAggregatedReporterAssembly("object-popularity", Columns.Object));
        reportFlag = true;
      }
      if (cmd.hasOption("clientIPReport")) {
        reportsToGenerate.add(new FieldAggregatedReporterAssembly("client-ip", Columns.IPAddress));
        reportFlag = true;
      }
      if (cmd.hasOption("edgeLocationReport")) {
        reportsToGenerate.add(new FieldAggregatedReporterAssembly("edge-location", Columns.EdgeLocation));
        reportFlag = true;
      }
    }
    if (!reportFlag) {
      // Choosing all reports if none specified
      reportsToGenerate.add(new FieldAggregatedReporterAssembly("client-ip", Columns.IPAddress));
      reportsToGenerate.add(new FieldAggregatedReporterAssembly("object-popularity", Columns.Object));
      reportsToGenerate.add(new TimeBucketedReporterAssembly("overall-volume", delta));
      reportsToGenerate.add(new FieldAggregatedReporterAssembly("edge-location", Columns.EdgeLocation));
    }


    // Set the current job jar
    Properties properties = new Properties();
    FlowConnector.setApplicationJarClass(properties, Main.class);
    
    // Construct and run the S3 copy job.
    CopyFromS3 copyFromS3 = 
        new CopyFromS3(inputPath, tempIndex, tempS3Copy, startDate, endDate);
    JobConf copyFromS3Conf = copyFromS3.getJobConf();
    
    JobClient.runJob(copyFromS3Conf);
    
    // Parse the S3 files to create a Cascading sequence file with the appropriate fields.
    FlowConnector connector = new FlowConnector();
    Tap source = new Hfs(new TextLine(), tempS3Copy);
    Tap sink = 
        new Hfs(new SequenceFile(new Fields(Columns.DateTime, Columns.EdgeLocation, Columns.Bytes, 
                                            Columns.IPAddress, Columns.Object, Columns.HTTPResponse, Columns.TimeStamp)), 
                tempPath);
    Flow headFlow = connector.connect(source, sink, createHeadPipe());
    headFlow.start();
    headFlow.complete();
    
    //Check if the data exists in HDFS
    Configuration conf  = new JobConf();
    FileSystem fs = null;
    try {
      fs = FileSystem.get(URI.create(tempPath), conf);
      if (!fs.exists(new Path(tempPath))) {
        throw new RuntimeException("No logfiles found in S3 in the given time range " + startDate + " to " + endDate);
      }
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    } finally {
      try {
        if(fs != null) {
          fs.close();
        }
      } catch (IOException e) {
        //Ignore it
      }
    }
    
    // Run reports from HDFS
    List<Flow> flowList = new ArrayList<Flow>();
    for (SubAssembly report : reportsToGenerate) {
      String[] tailNames = report.getTailNames();
      List<Tap> sinks = new ArrayList<Tap>();
      for (String tail : tailNames) {
        Tap s = new Hfs(new TextLine(1), intermediateOut + tail);
        sinks.add(s);
      }
      Map<String,Tap> tapMap = Cascades.tapsMap(tailNames, sinks.toArray(new Tap[0]));
      flowList.add(connector.connect(sink, tapMap, report));
    }
    Cascade cascade = new CascadeConnector().connect(flowList.toArray(new Flow[0]));
    cascade.complete();

    // Copy over the output into s3
    try {
      fs = FileSystem.get(URI.create(tempPath), conf);
      for (SubAssembly report:reportsToGenerate) {
        for (String name:report.getTailNames()) {
          fs.rename(new Path(intermediateOut + name + "/part-00000"), new Path(intermediateOut + name + "/" + name + "_" + startDate + "_" + endDate + ".txt"));
        }
      }
      fs.close();
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    } finally {
      try {
        fs.close();
      } catch (IOException e) {
        //Ignore it
      }
    }
    DistCp distCp = new DistCp(conf);
    String[] dcmd = new String[4];
    dcmd[0] = "-log";
    dcmd[1] = outputPath + "_runlogs_";
    dcmd[2] = intermediateOut;
    dcmd[3]= outputPath;
    try {
      distCp.run(dcmd);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static String appendTrailingSlash(String inputPath) {
    if (!inputPath.endsWith("/")) {
      inputPath += "/";
    }
    return inputPath;
  }

  private static String getFormattedDate() {
    Calendar c = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd-HH");
    return sdf.format(c.getTime());
  }

  private static void printHelpText(Options options) {
    (new HelpFormatter()).printHelp("hadoop jar logprocessor.jar " +
                    "-input <path to input> " +
                    "-output <path to output> " +
                    "-start <start date> " +
                    "-end <end date> " +
                    "<report options>", options);
  }

  @SuppressWarnings("static-access")
  private static Options createOptions() {
    Options options = new Options();
    options.addOption(new Option("help", "print this message"));
    options.addOption (OptionBuilder.withArgName("input")
                    .hasArg()
                    .withDescription("Path to the logfiles that you wish to analyse e.g s3n://<mybucket>/cflogs/")
                    .create("input")
                      );
    options.addOption (OptionBuilder.withArgName("tempOutput")
        .hasArg()
        .withDescription("[Optional:Do not use unless you know what you are doing] Path to the end of first stage processing")
        .create("tempOutput")
          );
    options.addOption (OptionBuilder.withArgName("output")
                     .hasArg()
                     .withDescription("Path to which reports need to get written e.g s3n://<mybucket>/output/ ")
                     .create("output")
                     );
    options.addOption (OptionBuilder.withArgName("start")
                     .hasArg()
                     .withDescription("[Optional default:any] Start date to the hour in GMT formatted as yyyy-MM-dd-HH where HH is in 24 hour format e.g 2008-02-18-14. Specify \"any\" to not constrain the start date")
                     .create("start")
                     );
    options.addOption (OptionBuilder.withArgName("end")
                    .hasArg()
                    .withDescription("[Optional default:any] End date to the hour in GMT formatted as yyyy-MM-dd-HH where HH is in 24 hour format e.g 2008-02-18-14. Specify \"any\" to not constrain the end date")
                    .create("end")
                    );
    options.addOption (OptionBuilder.withArgName("timeBucket")
          .hasArg()
          .withDescription("[Optional default:300] Time interval in seconds to aggregate reports by")
          .create("timeBucket")
          );
    options.addOption(new Option("allReports","generate all the reports (Overall Volume, Object Popularity, Client IP"));
    options.addOption(new Option("overallVolumeReport","generate the Overall Volume report"));
    options.addOption(new Option("objectPopularityReport","generate the Object Popularity report"));
    options.addOption(new Option("clientIPReport","generate the Client IP report"));
    options.addOption(new Option("edgeLocationReport","generate the Edge Location report"));
    return options;
  }

}
