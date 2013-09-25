package cloudBurst;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;


public class AlignmentStats {
	
	private static int numalignments = 0;
    private static int rcalignments = 0;
    private static int numfiles = 0;
    private static Set<Integer> reads = new HashSet<Integer>();
    private static AlignmentRecord ar = new AlignmentRecord();
    private static JobConf conf = null;
	
   public static void fileStats(Path thePath) throws Exception
   {
       SequenceFile.Reader theReader = new SequenceFile.Reader(FileSystem.get(conf), thePath, conf);
       IntWritable key = new IntWritable();
       Text value = new Text();
       numfiles++;
       
       while(theReader.next(key,value))
       {
    	   	numalignments++;
    	   
       		int thisread = key.get();
    	
       		if (ar.fromText(value).m_isRC)
       		{
       			rcalignments++;
       		}
    	
       		reads.add(thisread);
       }
   }   
	
   public static void stats(Path thePath) throws Exception
   {
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
				   fileStats(file.getPath());
			   }
		   }
       }
       else
       {
    	   fileStats(thePath);	   
       }

       
       int numreads = reads.size();
       
		System.out.println(numfiles + " files processed");
        System.out.println(numalignments + " Total Alignments");
		System.out.println(rcalignments + " RC Alignments");
		System.out.println(numreads + " Reads Aligned");
   }
	
   public static void main(String[] args) throws Exception
   {
	   String path = "/user/guest/br-results/"; //args[0];   
	   stats(new Path(path));
   }
}
