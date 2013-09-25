package cloudBurst;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;


public class CountKmers {
		
		public static class MerMapClass extends MapReduceBase implements
				Mapper<IntWritable, BytesWritable, BytesWritable, IntWritable> 
		{
			private FastaRecord record = new FastaRecord();
			private BytesWritable mer = new BytesWritable();
			private IntWritable pos = new IntWritable(1);
			private byte [] dnabuffer = null;
			private int KMER_LEN;

			public void configure(JobConf conf) 
			{
				KMER_LEN = Integer.parseInt(conf.get("KMER_LEN"));
				dnabuffer = new byte[DNAString.arrToDNALen(KMER_LEN)];
			}

			public void map(IntWritable id, BytesWritable rawRecord,
					OutputCollector<BytesWritable, IntWritable> output, Reporter reporter) throws IOException 
			{
				record.fromBytes(rawRecord);
				
				byte [] seq         = record.m_sequence;
				int realoffsetstart = record.m_offset;
				int seqlen = seq.length;
				
				int startoffset = 0;
				
				// If I'm not the first chunk, shift over so there is room for the left flank
				if (realoffsetstart != 0)
				{
					int shift = CloudBurst.CHUNK_OVERLAP + 1 - KMER_LEN;
					startoffset = shift;
					realoffsetstart += shift;
				}
				
				// stop so the last mer will just fit
				int end = seqlen - KMER_LEN + 1;
				
				for (int start = startoffset, realoffset = realoffsetstart; start < end; start++, realoffset++)
				{						
					if (DNAString.arrHasN(seq, start, KMER_LEN)) { continue; }
					DNAString.arrToDNAStr(seq, start, KMER_LEN, dnabuffer, 0);
					mer.set(dnabuffer, 0, dnabuffer.length);
					pos.set(realoffset);
					output.collect(mer, pos);
				}
			}
		}
		
		
		public static class MerReduceClass extends MapReduceBase implements
				Reducer<BytesWritable, IntWritable, Text, Text> 
		{
			private static Text mertext = new Text();
			private static Text locations = new Text();
			private static StringBuilder builder = new StringBuilder();
			private boolean SHOW_POS;
			
			public void configure(JobConf conf) 
			{
				SHOW_POS = (Integer.parseInt(conf.get("SHOW_POS")) == 0) ? false : true;
			}
					
			
			public synchronized void reduce(BytesWritable mer, Iterator<IntWritable> values,
					OutputCollector<Text, Text> output, Reporter reporter)
					throws IOException 
			{
				int cnt = 0;
				builder.setLength(0);
				
				while (values.hasNext()) 
				{
					cnt++;
					if (SHOW_POS)
					{
						builder.append('\t');
						builder.append(values.next().get());
					}
				}
				
				String val = DNAString.bytesToString(DNAString.bytesWritableDNAToArr(mer));
				mertext.set(val);
				
				if (SHOW_POS)
				{
					builder.insert(0, cnt);
					String locs = builder.toString();
					locations.set(locs);
				}
				else
				{
					locations.set(Integer.toString(cnt));
				}
				
				output.collect(mertext, locations);
			}
		}
	

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException 
	{
		String inpath = null;
		String outpath = null;
		int kmerlen = 0;
		int numMappers = 1;
		int numReducers = 1;
		int showpos = 0;
		
		int data = 1;
		
		if (data == 0)
		{	
			if (args.length != 6)
			{
				System.err.println("Usage: CountKmers filename outpath kmerlen showpos numMappers numReducers");
				return;
			}
			
			inpath      =                  args[0];
			outpath     =                  args[1];
			kmerlen     = Integer.parseInt(args[2]);
			showpos     = Integer.parseInt(args[3]);
			numMappers  = Integer.parseInt(args[4]);
			numReducers = Integer.parseInt(args[5]);
		}
		else if (data == 1)
		{
			inpath = "/user/guest/cloudburst/s_suis.br";
			outpath = "/user/mschatz/kmers";
			kmerlen = 12;
			showpos = 0;
			numMappers = 1;
			numReducers = 1;
		}
		
		System.out.println("inpath: " + inpath);
		System.out.println("outpath: " + outpath);
		System.out.println("kmerlen: " + kmerlen);
		System.out.println("showpos: " + showpos);
		System.out.println("nummappers: " + numMappers);
		System.out.println("numreducers: " + numReducers);
		
		JobConf conf = new JobConf(MerReduce.class);
		conf.setNumMapTasks(numMappers);
		conf.setNumReduceTasks(numReducers);
			
		conf.addInputPath(new Path(inpath));;
		conf.set("KMER_LEN", Integer.toString(kmerlen));
		conf.set("SHOW_POS", Integer.toString(showpos));
		
		conf.setInputFormat(SequenceFileInputFormat.class);
			
		conf.setMapOutputKeyClass(BytesWritable.class);
		conf.setMapOutputValueClass(IntWritable.class);
		//conf.setCompressMapOutput(true);
				
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		conf.setOutputFormat(TextOutputFormat.class);
		
		conf.setMapperClass(MerMapClass.class);
		conf.setReducerClass(MerReduceClass.class);

		Path oPath = new Path(outpath);
		conf.setOutputPath(oPath);
		System.err.println("  Removing old results");
		FileSystem.get(conf).delete(oPath);
				
		conf.setJobName("CountMers");

		Timer t = new Timer();
		RunningJob rj = JobClient.runJob(conf);
		System.err.println("CountMers Finished");
		
		System.err.println("Total Running time was " + t.get());
		
		Counters counters = rj.getCounters( );
		Counters.Group task = counters.getGroup("org.apache.hadoop.mapred.Task$Counter");		
		long numDistinctMers = task.getCounter("REDUCE_INPUT_GROUPS");
		System.err.println("Num Distinct Mers: " + numDistinctMers);
	}
}
