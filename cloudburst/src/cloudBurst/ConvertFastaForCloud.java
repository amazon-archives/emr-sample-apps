package cloudBurst;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.mapred.JobConf;


public class ConvertFastaForCloud {

	private static final FastaRecord record = new FastaRecord();
	
	public static int min_seq_len = Integer.MAX_VALUE;
	public static int max_seq_len = 0;
		
	public static int min(int a, int b)
	{
		if (a < b) return a;
		return b;
	}
	
	public static int max(int a, int b)
	{
		if (a > b) return a;
		return b;
	}
		
	private static IntWritable iw = new IntWritable();
	
	public static void saveSequence(int id, StringBuilder sequence, Writer writer) throws IOException
	{	
		int fulllength = sequence.length();
		int maxchunk = 65535;
		
		if (fulllength < min_seq_len) { min_seq_len = fulllength; }
		if (fulllength > max_seq_len) { max_seq_len = fulllength; }
		
		if (fulllength > 100)
		{
			System.out.println("In " + id + "... "  + fulllength + "bp");
		}
		
		int offset = 0;
		int numchunks = 0;
		
		while(offset < fulllength)
		{
			numchunks++;
			int end = min(offset + maxchunk, fulllength);
			
			boolean lastChunk = (end == fulllength);

			record.m_sequence = DNAString.stringToBytes(sequence.substring(offset, end));
			record.m_offset = offset;
			record.m_lastChunk = lastChunk;
			
			iw.set(id);
			writer.append(iw, record.toBytes());
			
			if (end == fulllength) 
			{ 
				offset = fulllength; 
			}
			else
			{
				offset = end - cloudBurst.CloudBurst.CHUNK_OVERLAP;
			}
		}
		
		if (numchunks > 1)
		{
			System.out.println("  " + numchunks + " chunks");
		}
	}
	
	public static void convertFile(String infile, SequenceFile.Writer writer) throws IOException
	{
		String header = "";
		StringBuilder sequence = null;
		
		int count = 0;
		
		try 
		{
			BufferedReader data = new BufferedReader(new InputStreamReader(new FileInputStream(infile)));
			
			String mapfile = infile;
			mapfile += ".map";
			FileWriter fstream = new FileWriter(mapfile);
		    BufferedWriter out = new BufferedWriter(fstream);
		    
			String line;
			while ((line = data.readLine()) != null) 
			{
				line.trim();
				
				if (line.charAt(0) == '>')
				{
					if (count > 0)
					{
					  saveSequence(count, sequence, writer);
					}
					
					sequence = new StringBuilder();
					header = line.substring(1); // skip the >
					count++;
					
					out.write(count + " " + header + "\n");
				}
				else
				{
					sequence.append(line.toUpperCase());
				}
			}
			
			saveSequence(count, sequence, writer);
			
		    out.close();
		} 
		catch (FileNotFoundException e) 
		{
			System.err.println("Can't open " + infile);
			e.printStackTrace();
			System.exit(1);
		}
		
		System.err.println("Processed " + count + " sequences");
	}
	
	
	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		if (args.length != 2) {
			System.err.println("Usage: ConvertFastaForCloud file.fa outfile.br");
			System.exit(-1);
		}
		
		String infile = args[0];
		String outfile = args[1];
		
		System.err.println("Converting " + infile + " into " + outfile);
		
		JobConf config = new JobConf();
		
		SequenceFile.Writer writer = SequenceFile.createWriter(FileSystem.get(config), config,
				new Path(outfile), IntWritable.class, BytesWritable.class);
		
		convertFile(infile, writer);
		
		writer.close();
		
		System.err.println("min_seq_len: " + min_seq_len);
		System.err.println("max_seq_len: " + max_seq_len);
		System.err.println("Using DNAString version: " + DNAString.VERSION);
	}
};
