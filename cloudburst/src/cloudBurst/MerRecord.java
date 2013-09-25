package cloudBurst;

import org.apache.hadoop.io.BytesWritable;
import java.io.IOException;

public class MerRecord 
{	
	public boolean isReference = false;
	public boolean isRC = false;
	public int     offset = 0;
	public int     id;
	
	public byte[]  leftFlank;  // only set in the reduce phase
	public byte[]  rightFlank; // only set in the reduce phase
	
	private static StringBuilder builder = new StringBuilder();
	private static BytesWritable bytes   = new BytesWritable();
	
	private static byte[] sbuffer = new byte [1024];
	
	//------------------------- Constructor --------------------------
	MerRecord()
	{
	
	}
	
	//------------------------- Constructor --------------------------
	MerRecord(BytesWritable t) throws IOException
	{
		fromBytes(t);
	}
	
	//------------------------- toBytes --------------------------
	// Pack the MerRecord information into a BytesWritable
	// extract the flanking sequence on-the-fly do avoid copying as much as possible
	
	public BytesWritable toBytes(byte [] seq, int leftstart, int leftlen, int rightstart, int rightlen) //throws IOException
	{
		int len = 1 + // isReference, isRC
		          4 + // offset
		          4 + // id
		          1;  // hardstop between left and right flank
		
		if (leftlen > 0)  
		{ 
			len += DNAString.arrToDNALen(leftlen); 
		}
		
		if (rightlen > 0) 
		{ 
			len += DNAString.arrToDNALen(rightlen); 
		}
		
		if (len > sbuffer.length)
		{
			sbuffer = new byte[len*2];
		}
		
		sbuffer[0] = (byte) ((isReference ? 0x01 : 0x00) | (isRC ? 0x10 : 0x00));
		
		sbuffer[1] = (byte) ((offset & 0xFF000000) >> 24);
		sbuffer[2] = (byte) ((offset & 0x00FF0000) >> 16);
		sbuffer[3] = (byte) ((offset & 0x0000FF00) >> 8);
		sbuffer[4] = (byte) ((offset & 0x000000FF));
		
		sbuffer[5] = (byte) ((id & 0xFF000000) >> 24);
		sbuffer[6] = (byte) ((id & 0x00FF0000) >> 16);
		sbuffer[7] = (byte) ((id & 0x0000FF00) >> 8);
		sbuffer[8] = (byte) ((id & 0x000000FF));
		
		int pos = 9;
		
		if (leftlen > 0)
		{
			pos += DNAString.arrToDNAStrRev(seq, leftstart, leftlen, sbuffer, pos);
		}
		
		sbuffer[pos] = DNAString.hardstop; pos++;
		
		if (rightlen > 0)
		{
			pos += DNAString.arrToDNAStr(seq, rightstart, rightlen, sbuffer, pos);
		}
		
		/*
		if (pos != len)
		{
			throw new IOException("pos(" + pos + ") != len(" + len + ")");
		}
		*/
		
		bytes.set(sbuffer, 0, len);
		return bytes;
	}

	
	//------------------------- fromBytes --------------------------
	// Unpack the raw bytes and set the MerRecord fields

	public void fromBytes(BytesWritable t)
	{
		byte [] raw = t.get();
		int rawlen = t.getSize();
		
		//sbuffer[0] = (byte) ((isReference ? 0x01 : 0x00) | (isRC ? 0x10 : 0x00));
		
		isReference = (raw[0] & 0x01) == 0x01;
		isRC        = (raw[0] & 0x10) == 0x10;
		
		offset = (raw[1] & 0xFF) << 24 
		       | (raw[2] & 0xFF) << 16
		       | (raw[3] & 0xFF) << 8
		       | (raw[4] & 0xFF);
		
		id = (raw[5] & 0xFF) << 24 
           | (raw[6] & 0xFF) << 16
           | (raw[7] & 0xFF) << 8
           | (raw[8] & 0xFF);

		int fieldstart = 9;
		
		for (int i = fieldstart; i < rawlen; i++)
		{
			if (raw[i] == DNAString.hardstop)
			{
				//leftFlank = DNAString.dnaToArr(raw, fieldstart, i-fieldstart);
				leftFlank = new byte[i-fieldstart];
				System.arraycopy(raw, fieldstart, leftFlank, 0, i-fieldstart);
				
				fieldstart = i+1; // skip the hardstop
				break;
			}
		}
		
		rightFlank = new byte[rawlen - fieldstart];
		System.arraycopy(raw, fieldstart, rightFlank, 0, rawlen-fieldstart);
		//rightFlank = DNAString.dnaToArr(raw, fieldstart, rawlen-fieldstart);
	}
	
	
	//------------------------- toString --------------------------
	// Serialize the fields to a string for debugging
	
	public String toString()
	{
		builder.setLength(0);
		
		builder.append(isReference?'1':'0');                 builder.append(';');
		builder.append(isRC?'1':'0');                        builder.append(';');
		builder.append(offset);                              builder.append(';');
		builder.append(id);                                  builder.append(';');
		builder.append(DNAString.bytesToString(DNAString.dnaToArr(leftFlank)));  builder.append(';');
		builder.append(DNAString.bytesToString(DNAString.dnaToArr(rightFlank)));
		
		return builder.toString();
	}
	
	
	//------------------------- main --------------------------
	// Make sure the serialization is correct and fast
	
	public static void main(String[] args) throws IOException 
	{
		byte[] seq = DNAString.stringToBytes("ACGTACGTACGTACGTACGT");
		
		MerRecord mr = new MerRecord();
		mr.id = 12345;
		mr.isRC = true;
		mr.isReference = false;
		
		mr.offset = 1234567;
			
		Timer t = new Timer();
		int num = 10000000;
		for (int i = 0; i < num; i++)
		{
			//System.out.println("Org: " + mr.toString());
			
			BytesWritable bw = mr.toBytes(seq, 0, 5, 19, 0);
			
			MerRecord mr2 = new MerRecord(bw);
			
			//System.out.println("New: " + mr2.toString());
			
			//if (mr2.id != mr.id ||
			//    mr2.isRC != mr.isRC ||
			//    mr2.isReference != mr.isReference ||
			//    DNAString.bytesToString(mr2.leftFlank).compareTo(DNAString.bytesToString(mr.leftFlank)) != 0 ||
			//    DNAString.bytesToString(mr2.rightFlank).compareTo(DNAString.bytesToString(mr.rightFlank)) != 0)
			//{
			//	
			//}
			
			if (mr2.id != mr.id ||
				mr2.isRC != mr.isRC ||
				mr2.isReference != mr.isReference)
			{
				throw new IOException("Mismatch!");
			}
		}
		
		System.out.println(num + " took:" + t.get());
	}
}
