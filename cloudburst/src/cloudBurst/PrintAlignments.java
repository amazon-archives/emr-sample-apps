package cloudBurst;

import java.io.IOException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapred.JobConf;

public class PrintAlignments {
	
	private static JobConf conf = null;
	private static AlignmentRecord ar = new AlignmentRecord();
	
	public static void printFile(Path thePath) throws IOException
	{
		SequenceFile.Reader theReader = new SequenceFile.Reader(FileSystem.get(conf), thePath, conf);
	       
	    IntWritable key = new IntWritable();
	    BytesWritable value = new BytesWritable();
	       
	    while(theReader.next(key,value))
	    {
	    	ar.fromBytes(value);
	    	System.out.println(ar.toAlignment(key.get()));
	    }
	}

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException 
	{
		String filename = null;
		//filename = "/user/guest/br-results/";
		
		if (filename == null)
		{
			if (args.length != 1) 
			{
				System.err.println("Usage: PrintAlignments seqfile");
				System.exit(-1);
			}
			
			filename = args[0];
		}
	
		
		System.err.println("Printing " + filename);
		
		Path thePath = new Path(filename);
	    conf = new JobConf(AlignmentStats.class);
	       
	    FileSystem fs = FileSystem.get(conf);
	       
	    if (!fs.exists(thePath))
	    {
	    	throw new IOException(thePath + " not found");   
	    }

	    FileStatus status = fs.getFileStatus(thePath);

	    if (status.isDir())
	    {    	   
	    	FileStatus [] files = fs.listStatus(thePath);
	    	for(FileStatus file : files)
	    	{
	    		String str = file.getPath().getName();

	    		if (str.startsWith("."))
	    		{
	    			// skip
	    		}			   
	    		else if (!file.isDir())
	    		{
	    			printFile(file.getPath());
	    		}
	    	}
	    }
	    else
	    {
	    	printFile(thePath);	   
	    }
	}
}
