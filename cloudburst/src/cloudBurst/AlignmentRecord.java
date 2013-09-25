package cloudBurst;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import java.io.IOException;

public class AlignmentRecord 
{	
	public int     m_refID;
	public int     m_refStart = 0;
	public int     m_refEnd = 0;
	public int     m_differences = 0;
	public boolean m_isRC = false;
	
	private static final StringBuilder builder = new StringBuilder();
	private static final BytesWritable bytes = new BytesWritable();
	private static final byte [] sbuffer = new byte[17];
	
	AlignmentRecord()
	{
		
	}
	
	AlignmentRecord(int refid, int refstart, int refend, int differences, boolean rc)
	{
		m_refID = refid;
		m_refStart = refstart;
		m_refEnd = refend;
		m_differences = differences;
		m_isRC = rc;
	}
	
	AlignmentRecord(AlignmentRecord other)
	{
		set(other);
	}
	
	AlignmentRecord(Text t) throws IOException
	{
		fromText(t);
	}
	
	AlignmentRecord(BytesWritable b) throws IOException
	{
		fromBytes(b);
	}
	
	public void set(AlignmentRecord other)
	{
		m_refID       = other.m_refID;
		m_refStart    = other.m_refStart;
		m_refEnd      = other.m_refEnd;
		m_differences = other.m_differences;
		m_isRC        = other.m_isRC;
	}
	
	public Text toText()
	{
		return new Text(toString());
	}
	
	public String toString()
	{
		builder.setLength(0);
	
		builder.append(m_refID);              builder.append('\t');
		builder.append(m_isRC ? 1 : 0);       builder.append('\t');
		builder.append(m_refStart);           builder.append('\t');
		builder.append(m_refEnd);             builder.append('\t');
		builder.append(m_differences); 
		
		return builder.toString();
	}
	
	public String toAlignment(int readid)
	{
		builder.setLength(0);
		
		builder.append(m_refID);              builder.append('\t');
		builder.append(m_refStart);           builder.append('\t');
		builder.append(m_refEnd);             builder.append('\t');
		builder.append(readid);               builder.append('\t');
		builder.append(m_differences);        builder.append('\t');
		builder.append(m_isRC ? "-" : "+");       
		
		return builder.toString();		
	}
	
	public AlignmentRecord fromText(Text t)
	{
		String [] vals = t.toString().split("\t", 5);
		
		m_refID         = Integer.parseInt(vals[0]);
		m_isRC          = Integer.parseInt(vals[1]) == 1;
		m_refStart      = Integer.parseInt(vals[2]);
		m_refEnd        = Integer.parseInt(vals[3]);
		m_differences   = Integer.parseInt(vals[4]);
		
		return this;
	}
	
	public BytesWritable toBytes() //throws IOException
	{
		sbuffer[0] = (byte) (m_isRC ? 1 : 0);
		
		sbuffer[1] = (byte) ((m_refID & 0xFF000000) >> 24);
		sbuffer[2] = (byte) ((m_refID & 0x00FF0000) >> 16);
		sbuffer[3] = (byte) ((m_refID & 0x0000FF00) >> 8);
		sbuffer[4] = (byte) ((m_refID & 0x000000FF));
		
		sbuffer[5] = (byte) ((m_refStart & 0xFF000000) >> 24);
		sbuffer[6] = (byte) ((m_refStart & 0x00FF0000) >> 16);
		sbuffer[7] = (byte) ((m_refStart & 0x0000FF00) >> 8);
		sbuffer[8] = (byte) ((m_refStart & 0x000000FF));
		
		sbuffer[9]  = (byte) ((m_refEnd & 0xFF000000) >> 24);
		sbuffer[10] = (byte) ((m_refEnd & 0x00FF0000) >> 16);
		sbuffer[11] = (byte) ((m_refEnd & 0x0000FF00) >> 8);
		sbuffer[12] = (byte) ((m_refEnd & 0x000000FF));
		
		sbuffer[13] = (byte) ((m_differences & 0xFF000000) >> 24);
		sbuffer[14] = (byte) ((m_differences & 0x00FF0000) >> 16);
		sbuffer[15] = (byte) ((m_differences & 0x0000FF00) >> 8);
		sbuffer[16] = (byte) ((m_differences & 0x000000FF));
		
		bytes.set(sbuffer, 0, 17);
		return bytes;
	}
	
	
	public void fromBytes(BytesWritable t)
	{
		byte [] raw = t.get();
		
		m_isRC   = raw[0] == 1;
		
		m_refID = (raw[1] & 0xFF) << 24 
		        | (raw[2] & 0xFF) << 16
		        | (raw[3] & 0xFF) << 8
		        | (raw[4] & 0xFF);
		
		m_refStart = (raw[5] & 0xFF) << 24 
		           | (raw[6] & 0xFF) << 16
		           | (raw[7] & 0xFF) << 8
		           | (raw[8] & 0xFF);
		
		m_refEnd = (raw[9] & 0xFF) << 24 
		         | (raw[10] & 0xFF) << 16
		         | (raw[11] & 0xFF) << 8
		         | (raw[12] & 0xFF);
		
		m_differences = (raw[13] & 0xFF) << 24 
		              | (raw[14] & 0xFF) << 16
		              | (raw[15] & 0xFF) << 8
		              | (raw[16] & 0xFF);
	}
}