package logprocessor.s3copy;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.zip.GZIPInputStream;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;


public class S3CopyMapper extends MapReduceBase 
	implements Mapper<LongWritable, Text, NullWritable, Text> 
{   
  public void map(LongWritable key, Text value,
				OutputCollector<NullWritable, Text> output, Reporter reporter)
		throws IOException 
	{
    String fileName = value.toString();
		reporter.setStatus("Fetching file: " + fileName);
		
		NullWritable outputKey = NullWritable.get();
		FileSystem fs = FileSystem.get(URI.create(fileName), new JobConf());
		InputStream in = null;
		try {
	    in = fs.open(new Path(fileName));
	    if(fileName.endsWith(".gz")) {
        in = new GZIPInputStream(in);
	    }
	    BufferedReader reader = new BufferedReader(new InputStreamReader(in));
	    try {
		    for (String line = reader.readLine(); line != null; line = reader.readLine()) {
	        // relying on short circuiting here to not get a null pointer
	        if(line == null || line.startsWith("#")) {
	            continue;
	        }
	        output.collect(outputKey, new Text(line));
		    }
	    } finally {
	      reader.close();
	    }
		} finally {
		  if (in != null) {
		    in.close();
		  }
	    fs.close();
	    reporter.setStatus("Finished downloading file: " + fileName);
		}
	}
}
