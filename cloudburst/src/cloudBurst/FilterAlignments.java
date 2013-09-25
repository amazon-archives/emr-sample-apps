package cloudBurst;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class FilterAlignments 
{
	// Identity mapper
	public static class FilterMapClass extends MapReduceBase implements
				        Mapper<IntWritable, BytesWritable, IntWritable, BytesWritable> 
	{
		public void map(IntWritable readid, BytesWritable rawAlignment,
					OutputCollector<IntWritable, BytesWritable> output, Reporter reporter) throws IOException 
		{
			output.collect(readid, rawAlignment);
		}
	}

	
	// The combiner scans the partial list of alignments for each read, and only outputs the top 2 alignments
	// Should have good speedup, since an arbitrarily long list is reduced to just 2 items
	// Can't just record the top 1, because then it might be lost that number 1 is a tie
	// Must output at least the top 1, or a second best alignment might be recorded instead
	public static class FilterCombinerClass extends MapReduceBase implements
						Reducer<IntWritable, BytesWritable, IntWritable, BytesWritable> 
	{
		private static AlignmentRecord bestAlignment = new AlignmentRecord();
		private static AlignmentRecord curAlignment  = new AlignmentRecord();
		private static AlignmentRecord secondBest    = new AlignmentRecord();

		public synchronized void reduce(IntWritable readid, Iterator<BytesWritable> values,
										OutputCollector<IntWritable, BytesWritable> output, Reporter reporter) throws IOException 
		{
			boolean recordSecond = false;
			bestAlignment.fromBytes(values.next());

			while (values.hasNext()) 
			{
				curAlignment.fromBytes(values.next());

				if (curAlignment.m_differences < bestAlignment.m_differences)
				{
					bestAlignment.set(curAlignment);
					recordSecond = false;
				}
				else if (curAlignment.m_differences == bestAlignment.m_differences)
				{
					recordSecond = true;
					secondBest.set(curAlignment);
				}
				else
				{
//					curAlignment is worse than best alignment, nothing to do
				}
			}
			
			output.collect(readid, bestAlignment.toBytes());
			
			if (recordSecond)
			{
				output.collect(readid, secondBest.toBytes());
			}
		}
	}	

	
	// if there is a unique best alignment, record that alignment
	public static class FilterReduceClass extends MapReduceBase implements
		         		Reducer<IntWritable, BytesWritable, IntWritable, BytesWritable> 
	{
		private static AlignmentRecord bestAlignment = new AlignmentRecord();
		private static AlignmentRecord curAlignment = new AlignmentRecord();
				
		public synchronized void reduce(IntWritable readid, Iterator<BytesWritable> values,
		  			             OutputCollector<IntWritable, BytesWritable> output, Reporter reporter) throws IOException 
		{
			boolean recordBest = true;
			bestAlignment.fromBytes(values.next());
						
			while (values.hasNext()) 
			{
				curAlignment.fromBytes(values.next());
				
				if (curAlignment.m_differences < bestAlignment.m_differences)
				{
					bestAlignment.set(curAlignment);
					recordBest = true;
				}
				else if (curAlignment.m_differences == bestAlignment.m_differences)
				{
					recordBest = false;
				}
				else
				{
					// curAlignment is worse than best alignment, nothing to do
					
				}
			}
			
			if (recordBest)
			{
				output.collect(readid, bestAlignment.toBytes());
			}
		}
	}	
}
