package cloudBurst;

import org.apache.hadoop.io.Text;

public class SubstringTester {
	
	public static int KMER_LEN = 32;
	
	public static void version1(StringBuilder sb, Text mer)
	{
		Timer mertime = new Timer();
		int merlen = KMER_LEN;
		int end = sb.length() - merlen; 
		for (int start = 0; start < end; start++)
		{
			mer.set(sb.substring(start, start+merlen));	
		}
		System.out.println("version1 took: " + mertime.get());
	}
	
	public static void version2(StringBuilder sb, Text mer)
	{
		Timer mertime = new Timer();
		int merlen = KMER_LEN;
		int end = sb.length() - merlen;
		
		char [] merchar = new char[merlen];
		byte [] merbytes = new byte[merlen];
		
		System.out.println("version 2");
		
		for (int start = 0; start < end; start++)
		{
			sb.getChars(start, start+merlen, merchar, 0);
			for (int i = 0; i < merlen; i++)
			{
				merbytes[i] = (byte)merchar[i];
			}
			
			mer.set(merbytes);
			
			if (start < 10)
			{
				System.out.println("   mer[" + start + "]:" + mer.toString());
				
			}
		}
		System.out.println("total: " + mertime.get());
	}
	
	public static void version3(StringBuilder sb, Text mer)
	{
		System.out.println("version 3");
		
		Timer mertime = new Timer();
		int alllen = sb.length();
		int merlen = KMER_LEN;
		int end = alllen - merlen;
		
		byte [] merbytes = new byte[merlen];
		byte [] allbytes = new byte[alllen];
		
		for (int i = 0; i < alllen; i++)
		{
			allbytes[i] = (byte) sb.charAt(i);
		}
		
		
		for (int start = 0; start < end; start++)
		{
			for (int i = 0; i < merlen; i++)
			{
				merbytes[i] = allbytes[start+i];
			}
			
			mer.set(merbytes);
			
			if (start < 10)
			{
				System.out.println("   mer[" + start + "]:" + mer.toString());
			}
		}
		System.out.println("total: " + mertime.get());
	}


	
	public static void main(String[] args) 
	{
		String str = "acbdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890";
		
		Timer buildtime = new Timer();
		StringBuilder sb = new StringBuilder();
		int numcopies = 100000;
		for (int i = 0; i < numcopies; i++)
		{
			sb.append(str);
		}
		
		System.out.println("Constructed " + numcopies + " copies in " + buildtime.get());
		
		Text mer = new Text();
		
		//version1(sb, mer);
		version2(sb, mer);
		version3(sb, mer);

	}
}
