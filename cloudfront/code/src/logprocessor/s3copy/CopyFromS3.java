package logprocessor.s3copy;

import java.io.IOException;
import java.net.URI;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextOutputFormat;

/**
 * This class constructs a map reduce job to copy files from S3 to HDFS. Each
 * map task is assigned a subset of the files in S3 to copy. The number of
 * reducers is set to 0, thus the output of the map tasks is the final output of
 * this job. If the total data in S3 is small and the number of map tasks high,
 * then having 0 reducers will be sub-optimal since we will create many small
 * files in HDFS. This logic can be made smarter to detect this case and
 * configure reducers. Currently, we don't do that, mostly because we don't
 * expect this to be the mainline case.
 * 
 * A few optimizations used:
 * 
 * 1. No more than the maximum number of concurrent mappers configured in the
 * cluster are started. This is to avoid spending time in the job queue waiting
 * for map slots to become available. An obvious downside is that in the case of
 * a failure, we will have to repeat a lot more work. However, since the cluster
 * is started and stopped with every job, we consider this an acceptable
 * tradeoff since the probability of task failure should be very low. 2. This
 * class will first do an ls on the files in S3 to get the full file names and
 * the size of each file. This information is used to try and distribute the
 * files among the mappers so that each map task is downloading roughly the same
 * amount of data from S3.
 */
public class CopyFromS3 {

  private String inputPathPrefix;
  private String tempFile;
  private String outputPath;
  private String startDate;
  private String endDate;

  /**
   * Constructs a new object of this class with the given information.
   * 
   * @param inputPathPrefix
   *          The prefix of the path in S3 from where to download the files.
   * @param tempFile
   *          The location of the temp index file created by this job.
   * @param outputPath
   *          The location where the data from S3 should be output to.
   * @param startDate
   *          The date from which to start downloading log files.
   * @param endDate
   *          The end date of the log files to download.
   */
  public CopyFromS3(final String inputPathPrefix, final String tempFile, final String outputPath,
      final String startDate, final String endDate) {
    this.inputPathPrefix = inputPathPrefix;
    this.tempFile = tempFile;
    this.outputPath = outputPath;
    this.startDate = startDate;
    this.endDate = endDate;
  }

  /**
   * This method constructs the JobConf to be used to run the map reduce job to
   * download the files from S3. This is a potentially expensive method since it
   * makes multiple calls to S3 to get a listing of all the input data. Clients
   * are encouraged to cache the returned JobConf reference and not call this
   * method multiple times unless necessary.
   * 
   * @return the JobConf to be used to run the map reduce job to download the
   *         files from S3.
   */
  public JobConf getJobConf() throws IOException, ParseException {
    JobConf conf = new JobConf(CopyFromS3.class);
    conf.setJobName("CopyFromS3");
    conf.setOutputKeyClass(NullWritable.class);
    conf.setOutputValueClass(Text.class);
    conf.setMapperClass(S3CopyMapper.class);
    // We configure a reducer, even though we don't use it right now.
    // The idea is that, in the future we may. 
    conf.setReducerClass(HDFSWriterReducer.class);
    conf.setNumReduceTasks(0);

    FileInputFormat.setInputPaths(conf, new Path(tempFile));
    FileOutputFormat.setOutputPath(conf, new Path(outputPath));
    conf.setOutputFormat(TextOutputFormat.class);
    conf.setCompressMapOutput(true);

    JobClient jobClient = new JobClient(conf);

    FileSystem inputFS = FileSystem.get(URI.create(inputPathPrefix), conf);
    DatePathFilter datePathFilter = new DatePathFilter(startDate, endDate);
    List<Path> filePaths = getFilePaths(inputFS, new Path(inputPathPrefix), datePathFilter, jobClient.getDefaultMaps());

    // Write the file names to a temporary index file to be used
    // as input to the map tasks.
    FileSystem outputFS = FileSystem.get(URI.create(tempFile), conf);
    FSDataOutputStream outputStream = outputFS.create(new Path(tempFile), true);
    try {
      for (Path path : filePaths) {
        outputStream.writeBytes(path.toString() + "\n");
      }
    }
    finally {
      outputStream.close();
    }

    conf.setNumMapTasks(Math.min(filePaths.size(), jobClient.getDefaultMaps()));

    return conf;
  }

  private static List<Path> getFilePaths(FileSystem fs, final Path dir, DatePathFilter dateFilter, int maxNumMappers)
      throws IOException {
    List<PathsPerMapper> pathsPerMappers = getPathsPerMappers(fs, dir, dateFilter, maxNumMappers);
    List<Path> paths = new ArrayList<Path>();
    for (PathsPerMapper pathsPerMapper : pathsPerMappers) {
      paths.addAll(pathsPerMapper.paths);
    }

    return paths;
  }

  static class PathsPerMapper {
    public double totalSize = 0.0;
    public List<Path> paths = new ArrayList<Path>();
  }

  private static List<PathsPerMapper> getPathsPerMappers(FileSystem fs, final Path dir, DatePathFilter dateFilter,
      int maxNumMappers) throws IOException {
    List<PathsPerMapper> pathsPerMappers = new ArrayList<PathsPerMapper>(maxNumMappers);
    for (int i = 0; i < maxNumMappers; ++i) {
      pathsPerMappers.add(new PathsPerMapper());
    }

    FileStatus[] fileStatuses = fs.listStatus(dir);
    for (FileStatus fileStatus : fileStatuses) {
      if (fileStatus.isDir()) {
        pathsPerMappers.addAll(getPathsPerMappers(fs, fileStatus.getPath(), dateFilter, maxNumMappers));
      } else {
        // Check if this path passes the date filter check
        if (!dateFilter.accept(fileStatus.getPath())) {
          // Skip and continue to the next file.
          continue;
        }

        // Find the mapper with the smallest workload so far.
        // Doing a linear scan since expect the number of mappers to be small (less than a 1000 or so). 
        PathsPerMapper mapperWithSmallestSize = pathsPerMappers.get(0);

        for (PathsPerMapper pathsPerMapper : pathsPerMappers) {
          if (pathsPerMapper.totalSize < mapperWithSmallestSize.totalSize) {
            mapperWithSmallestSize = pathsPerMapper;
          }
        }

        mapperWithSmallestSize.paths.add(fileStatus.getPath());
        mapperWithSmallestSize.totalSize += fileStatus.getLen();
      }
    }

    return pathsPerMappers;
  }

}
