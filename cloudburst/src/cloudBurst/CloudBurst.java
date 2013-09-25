package cloudBurst;


import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;

import cloudBurst.MerReduce.MapClass;
import cloudBurst.MerReduce.ReduceClass;

import cloudBurst.FilterAlignments.FilterCombinerClass;
import cloudBurst.FilterAlignments.FilterMapClass;
import cloudBurst.FilterAlignments.FilterReduceClass;

public class CloudBurst {	
	
	// Make sure this number is longer than the longest read
	public static final int CHUNK_OVERLAP = 1024;
	
	
	//------------------------- alignall --------------------------
	// Setup and run the hadoop job for running the alignment

	public static RunningJob alignall(String refpath, 
			                          String qrypath,
			                          String outpath,
			                          int MIN_READ_LEN,
			                          int MAX_READ_LEN,
			                          int K,
			                          int ALLOW_DIFFERENCES,
			                          boolean FILTER_ALIGNMENTS,
			                          int NUM_MAP_TASKS,
			                          int NUM_REDUCE_TASKS,
			                          int BLOCK_SIZE,
			                          int REDUNDANCY) throws IOException, Exception
	{
		int SEED_LEN   = MIN_READ_LEN / (K+1);
		int FLANK_LEN  = MAX_READ_LEN-SEED_LEN+K;
		
		System.out.println("refath: "            + refpath);
		System.out.println("qrypath: "           + qrypath);
		System.out.println("outpath: "           + outpath);
		System.out.println("MIN_READ_LEN: "      + MIN_READ_LEN);
		System.out.println("MAX_READ_LEN: "      + MAX_READ_LEN);
		System.out.println("K: "                 + K);
		System.out.println("SEED_LEN: "          + SEED_LEN);
		System.out.println("FLANK_LEN: "         + FLANK_LEN);
		System.out.println("ALLOW_DIFFERENCES: " + ALLOW_DIFFERENCES);
		System.out.println("FILTER_ALIGNMENTS: " + FILTER_ALIGNMENTS);
		System.out.println("NUM_MAP_TASKS: "     + NUM_MAP_TASKS);
		System.out.println("NUM_REDUCE_TASKS: "  + NUM_REDUCE_TASKS);
		System.out.println("BLOCK_SIZE: "        + BLOCK_SIZE);
		System.out.println("REDUNDANCY: "        + REDUNDANCY);
		
		JobConf conf = new JobConf(MerReduce.class);
		conf.setJobName("CloudBurst");
		conf.setNumMapTasks(NUM_MAP_TASKS);
		conf.setNumReduceTasks(NUM_REDUCE_TASKS);
		
		FileInputFormat.addInputPath(conf, new Path(refpath));
		FileInputFormat.addInputPath(conf, new Path(qrypath));

		conf.set("refpath",           refpath);
		conf.set("qrypath",           qrypath);
		conf.set("MIN_READ_LEN",      Integer.toString(MIN_READ_LEN));
		conf.set("MAX_READ_LEN",      Integer.toString(MAX_READ_LEN));
		conf.set("K",                 Integer.toString(K));
		conf.set("SEED_LEN",          Integer.toString(SEED_LEN));
		conf.set("FLANK_LEN",         Integer.toString(FLANK_LEN));
		conf.set("ALLOW_DIFFERENCES", Integer.toString(ALLOW_DIFFERENCES));
		conf.set("BLOCK_SIZE",        Integer.toString(BLOCK_SIZE));
		conf.set("REDUNDANCY",        Integer.toString(REDUNDANCY));
		conf.set("FILTER_ALIGNMENTS", (FILTER_ALIGNMENTS ? "1" : "0"));
		
		conf.setMapperClass(MapClass.class);
		
		conf.setInputFormat(SequenceFileInputFormat.class);			
		conf.setMapOutputKeyClass(BytesWritable.class);
		conf.setMapOutputValueClass(BytesWritable.class);
		
		conf.setReducerClass(ReduceClass.class);		
		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(BytesWritable.class);
		conf.setOutputFormat(SequenceFileOutputFormat.class);

		Path oPath = new Path(outpath);
		FileOutputFormat.setOutputPath(conf, oPath);
		System.err.println("  Removing old results");
		FileSystem.get(conf).delete(oPath);
		
		RunningJob rj = JobClient.runJob(conf);
		System.err.println("CloudBurst Finished");
		return rj;
	}
	
	
	//------------------------- filter --------------------------
	// Setup and run the hadoop job for filtering the alignments to just report unambiguous bests
	
	public static void filter(String alignpath, 
			                  String outpath,
                              int nummappers,
                              int numreducers) throws IOException, Exception
    {
		System.out.println("NUM_FMAP_TASKS: "     + nummappers);
		System.out.println("NUM_FREDUCE_TASKS: "  + numreducers);
		
		JobConf conf = new JobConf(FilterAlignments.class);
		conf.setJobName("FilterAlignments");
		conf.setNumMapTasks(nummappers);
		conf.setNumReduceTasks(numreducers);
		
		FileInputFormat.addInputPath(conf, new Path(alignpath));
		
		conf.setMapperClass(FilterMapClass.class);
		
		conf.setInputFormat(SequenceFileInputFormat.class);			
		conf.setMapOutputKeyClass(IntWritable.class);
		conf.setMapOutputValueClass(BytesWritable.class);
		
		conf.setCombinerClass(FilterCombinerClass.class);
		
		conf.setReducerClass(FilterReduceClass.class);		
		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(BytesWritable.class);
		conf.setOutputFormat(SequenceFileOutputFormat.class);

		Path oPath = new Path(outpath);
		FileOutputFormat.setOutputPath(conf, oPath);
		System.err.println("  Removing old results");
		FileSystem.get(conf).delete(oPath);
		
		JobClient.runJob(conf);
		
		System.err.println("FilterAlignments Finished");		
    }

	
	//------------------------- main --------------------------
	// Parse the command line options, run alignment and filtering
	
	public static void main(String[] args) throws Exception 
	{	
		String refpath = null;
		String qrypath = null;
		String outpath = null;
		
		int K                = 0;
		int readlen          = 0;
		int allowdifferences = 0;
		
		int nummappers   = 1;
		int numreducers  = 1;
		int numfmappers  = 1;
		int numfreducers = 1;
		int blocksize    = 128;
		int redundancy   = 1;
		
		boolean filteralignments = false;
		
		int local = 0; // set to zero to use command line arguments
		
		if (local == 1)
		{
			refpath = "/user/guest/cloudburst/s_suis.br";
			qrypath = "/user/guest/cloudburst/100k.br";
			outpath = "/user/guest/br-results";
			readlen = 36;
			
			K = 3;
			allowdifferences = 0;
			filteralignments = true;
			redundancy       = 2;
		}
		else if (args.length < 13)
		{
			System.err.println("Usage: CloudBurst refpath qrypath outpath readlen k allowdifferences filteralignments #mappers #reduces #fmappers #freducers blocksize redundancy");
			return;
		}
		else
		{
			refpath          = args[0];
			qrypath          = args[1];
			outpath          = args[2];
			readlen          = Integer.parseInt(args[3]);
			K                = Integer.parseInt(args[4]);
			allowdifferences = Integer.parseInt(args[5]);
			filteralignments = Integer.parseInt(args[6]) == 1;
			nummappers       = Integer.parseInt(args[7]);
			numreducers      = Integer.parseInt(args[8]);
			numfmappers      = Integer.parseInt(args[9]);
			numfreducers     = Integer.parseInt(args[10]);
			blocksize        = Integer.parseInt(args[11]);
			redundancy       = Integer.parseInt(args[12]);
		}
		
		if (redundancy < 1) { System.err.println("minimum redundancy is 1"); return; }
		
		if (readlen > CHUNK_OVERLAP)
		{
			System.err.println("Increase CHUNK_OVERLAP for " + readlen + " length reads, and reconvert fasta file");
			return;
		}
			
		// start the timer
		Timer all = new Timer();
		
		String alignpath = outpath;
		if (filteralignments) { alignpath += "-alignments"; }
		
		
		// run the alignments
		Timer talign = new Timer();
		alignall(refpath,  qrypath, alignpath, readlen, readlen, K, allowdifferences, filteralignments, 
				 nummappers, numreducers, blocksize, redundancy);
		System.err.println("Alignment time: " + talign.get());
		
		
		// filter to report best alignments
		if (filteralignments)
		{
			Timer tfilter = new Timer();
			filter(alignpath, outpath, numfmappers, numfreducers);
		
			System.err.println("Filtering time: " + tfilter.get());
		}
		
		System.err.println("Total Running time:  " + all.get());
	};
}
