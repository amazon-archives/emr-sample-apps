package logprocessor.s3copy;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class HDFSWriterReducer extends MapReduceBase implements Reducer<NullWritable, Text, NullWritable, Text> {

  public void reduce(NullWritable key, Iterator<Text> values, OutputCollector<NullWritable, Text> output,
      Reporter reporter) throws IOException {
    while (values.hasNext()) {
      output.collect(key, values.next());
    }
  }
}
