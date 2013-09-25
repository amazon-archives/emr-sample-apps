package cloudBurst;

import java.io.IOException;


///                  k*2+1   (k+1)^2
///   k=0      1       1        1 
///   k=1     123      3        4
///   k=2    12345     5        9
///   k=3   1234567    7        16
///   k=4  123456789   9        25



public final class LandauVishkin {

	private static AlignInfo noAlignment   = new AlignInfo(0,  0, null, null, 0);
	private static AlignInfo badAlignment  = new AlignInfo(-1,-1, null, null, 0);
	private static AlignInfo goodAlignment = new AlignInfo(0, 0, null, null, 0);
	
	private static int [][] L = null;
	private static int [][] B = null;
	private static int [] dist = null;
	private static int [] what = null;	
	
	
	//------------------------- configure --------------------------
	// initialize runtime buffers
	
	public static void configure(int k)
	{
		L    = new int [k*2+1][k+1];
		B    = new int [k*2+1][k+1];
		dist = new int [k+1];
		what = new int [k+1];
	}
	
	
	//------------------------- kmismatch --------------------------
	// count mismatches between ascii strings
	
	public static AlignInfo kmismatch(byte [] text, byte [] pattern, int k)
	{		
		int m = pattern.length;
		int n = text.length;
		
		if (m == 0 || n == 0)
		{
			return noAlignment;
		}
		
		int last = (m < n) ? m : n;
		int mm = 0;
		int match = 0;
		
		for (int pos = 0; pos < last; pos++)
		{
			if (text[pos] != pattern[pos])
			{
				what[mm] = 0;
				dist[mm] = match;
				match = 0;
				
				mm++;
				
				if (mm > k)
				{
					return badAlignment;
				}
			}
			
			match++;
		}
		
		dist[mm] = match;
		what[mm] = 2;
				
		goodAlignment.setVals(last, mm, dist, what, mm+1);	// say how far we reached in the text (reference)
		return goodAlignment;
	}

	
	//------------------------- kmismatch_bin --------------------------
	// count mismatches between 2 bases / byte binary strings
	
	public static AlignInfo kmismatch_bin(byte [] text, byte [] pattern, int k)
	{
		int m = pattern.length;
		int n = text.length;
		
		if (m == 0)
		{
			return noAlignment;
		}
		
		// require the entire query to align
		if (n < m)
		{
			return badAlignment;
		}
		
		int last = (m < n) ? m : n;
		int mm = 0;
		int match = 0;
		
		last--; // check the last position outside of the loop so we can check for a space
		
		int pos = 0;
		for (; pos < last; pos++)
		{
			if (text[pos] == pattern[pos])
			{
				match += 2;
			}
			else
			{
				if ((text[pos] & 0xF0) != (pattern[pos] & 0xF0))
				{
					dist[mm] = match;
					match = 0;
					mm++;
					
					if (mm > k)
					{
						return badAlignment;
					}
				}
				
				match++;
				
				if ((text[pos] & 0x0F) != (pattern[pos] & 0x0F))
				{
					dist[mm] = match;
					match = 0;
					mm++;
					
					if (mm > k)
					{
						return badAlignment;
					}
				}
				
				match++;
			}
		}
			
		int alignlen = last*2+1;

		// explicitly check the last 2 characters since last 1 may be a space
		if (((text[pos] & 0x0F) != DNAString.space) && ((pattern[pos] & 0x0F) != DNAString.space))
		{
			alignlen++;
			
			if (text[pos] == pattern[pos])
			{
				match += 2;
			}
			else
			{
				if ((text[pos] & 0xF0) != (pattern[pos] & 0xF0))
				{
					dist[mm] = match;
					match = 0;
					mm++;

					if (mm > k)
					{
						return badAlignment;
					}
				}

				match++;

				if ((text[pos] & 0x0F) != (pattern[pos] & 0x0F))
				{
					dist[mm] = match;
					match = 0;
					mm++;

					if (mm > k)
					{
						return badAlignment;
					}
				}

				match++;
			}
		}
		else
		{
			if ((text[pos] & 0xF0) != (pattern[pos] & 0xF0))
			{
				dist[mm] = match;
				match = 0;
				mm++;

				if (mm > k)
				{
					return badAlignment;
				}
			}

			match++;			
		}
		
		
		// only fill in 'what' if there are <= k mismatches
		for (int i = 0; i < mm; i++)
			what[i] = 0;
		
		dist[mm] = match;
		what[mm] = 2;
				
		goodAlignment.setVals(alignlen, mm, dist, what, mm+1);	// say how far we reached in the text (reference)
		return goodAlignment;
	}

	
	
	//------------------------- kdifference --------------------------
	// Landau-Vishkin k-difference algorithm to align strings
	
	public static AlignInfo kdifference(byte [] text, byte [] pattern, int k)
	{	
		int m = pattern.length;
		int n = text.length;
		
		if (m == 0 || n == 0)
		{
			return noAlignment;
		}
			
		// Compute the dynamic programming to see how the strings align
		for (int e = 0; e <= k; e++)
		{
			for (int d = -e; d <= e; d++)
			{
				int row = -1;
				
				if (e > 0)
				{
					if (java.lang.Math.abs(d) < e)
					{
						int up = L[k+d][e-1] + 1;
						if (up > row) { row = up; B[k+d][e] = 0; }
					}
					
					if (d > -(e-1))
					{
						int left = L[k+d-1][e-1];
						if (left > row) { row = left; B[k+d][e] = -1; }
					}
					
					if (d < e-1)
					{
						int right = L[k+d+1][e-1]+1;
						if (right > row) { row = right; B[k+d][e] = +1; }
					}
				}
				else
				{
					row = 0;
				}
				
				while ((row < m) && (row+d < n) && (pattern[row] == text[row+d]))
				{
					row++;
				}
				
				L[k+d][e] = row;
				
				//System.out.println("L: k:" + k + " d:" + d + " e:" + e + " = " + row);
				
				if ((row+d == n) || (row == m)) // reached the end of the pattern or text
				{		
					int distlen = e+1;
					
					int E = e;
					int D = d;
					
					what[E] = 2; // always end at end-of-string
					
					while (e >= 0)
					{
						int b = B[k+d][e];
						if (e > 0) { what[e-1] = b; }
						
						dist[e] = L[k+d][e];	
						if (e < E) { dist[e+1] -= dist[e]; }
						
						d += b;
						e--;	
					}
					
					goodAlignment.setVals(row+D, E, dist, what, distlen);	// say how far we reached in the text (reference)			
					return goodAlignment;				
				}
			}
		}
		
		return badAlignment;
	}
	
	
	//------------------------- extend --------------------------
	// align the strings either for either k-mismatch or k-difference
	
	public static AlignInfo extend(byte [] refbin, byte [] qrybin, int K, boolean ALLOW_DIFFERENCES) throws IOException
	{
		if (ALLOW_DIFFERENCES)
		{
			byte [] ref = DNAString.dnaToArr(refbin);
			byte [] qry = DNAString.dnaToArr(refbin);
			
			return kdifference(ref, qry, K);						
		}
		else
		{
			return kmismatch_bin(refbin, qrybin, K);
		}
	}
	
	
	
	
	
	
	/////////////////////////////////////////////////////////////////////
	//                    Debugging and Test code                      //
    /////////////////////////////////////////////////////////////////////
	

	//------------------------- debugAlignment --------------------------
	// run an alignment and generate some debugging info

	public static void debugAlignment(byte[] tp, byte[] pp, int k, int kmerlen) throws IOException
	{
		System.out.println("====   DEBUG  ====");
		System.out.print("t: "); for (int i = 0; i < tp.length; i++) { System.out.print((char)tp[i]); } System.out.println();
		System.out.print("p: " ); for (int i = 0; i < pp.length; i++) { System.out.print((char)pp[i]); } System.out.println();
		
		//alignPrint(tp, pp, k);
		
		AlignInfo a = kdifference(tp, pp, k);
		
		System.out.println("There is an alignment ending at: " + a.alignlen + " k: " + a.differences);
		
		for (int i = 0; i < a.distlen; i++)
		{
			System.out.println("dist: " + a.dist[i] + " what: " + a.what[i]);
		}
		
		a.printAlignment(tp, pp);
				
		AlignInfo mm = kmismatch(tp, pp, k);
		
		System.out.println("There is an alignment ending at: " + mm.alignlen + " k: " + mm.differences);
		
		for (int i = 0; i < mm.distlen; i++)
		{
			System.out.println("dist: " + mm.dist[i] + " what: " + mm.what[i]);
		}
		
		mm.printAlignment(tp, pp);
		
		System.out.println("Bazea-Yates seed: " + a.isBazeaYatesSeed(pp.length, kmerlen));
	}
	
	
	//------------------------- checkBYS --------------------------
	// check the Bazea-Yates seeds are correct
		
	public static void checkBYS(int k, byte [] t, byte [] p, boolean shouldbeseed, String name, int kmerlen) throws IOException
	{
		System.out.println("checking " + name + " (" + shouldbeseed + ")");
		
		if (p.length != 10) { throw new IOException("Read length wrong:" + p.length); }
		
		AlignInfo a = kdifference(t, p, k);
		a.printAlignment(t, p);
		if (a.isBazeaYatesSeed(p.length, kmerlen) != shouldbeseed)
		{
			a.printAlignment(t, p);
			throw new IOException("shouldmatch: " + shouldbeseed);
		}
		
		System.out.println();
	}

	
	//------------------------- checkBY --------------------------
	// check the Bazea-Yates seeds are correct
	
	public static void checkBY() throws IOException
	{
		System.out.println("Checking BY");
		
		// Set KMER_LEN = 5
		int k = 3;
		int KMER_LEN = 5;
		
		//exact
		byte[] t =     DNAString.stringToBytes("AACCGATTCCCAA");
		
		checkBYS(k, t, DNAString.stringToBytes("AACCGATTCC"), false, "exact", KMER_LEN);
		
		checkBYS(k, t, DNAString.stringToBytes("ACCCGATTCC"), false, "1-mismatch", KMER_LEN);
		checkBYS(k, t, DNAString.stringToBytes("ACCGATTCCC"), false, "1-del", KMER_LEN);
		checkBYS(k, t, DNAString.stringToBytes("AAACCGATTC"), false, "1-ins", KMER_LEN);
		
		checkBYS(k, t, DNAString.stringToBytes("AACCGATCCC"), false, "2-mismatch", KMER_LEN);
		checkBYS(k, t, DNAString.stringToBytes("AACCGTTCCC"), false, "2-del", KMER_LEN);
		checkBYS(k, t, DNAString.stringToBytes("AACCGATTTC"), false, "2-ins", KMER_LEN);
		
		checkBYS(k, t, DNAString.stringToBytes("ACCCGATCCC"), true,  "1-mis, 2-mis", KMER_LEN);
		checkBYS(k, t, DNAString.stringToBytes("ATCCGATTCA"), true,  "1-mis, 2-del", KMER_LEN);
		checkBYS(k, t, DNAString.stringToBytes("ATCCGATTTC"), true,  "1-mis, 2-ins", KMER_LEN); 
		
		checkBYS(k, t, DNAString.stringToBytes("ACCGATACCC"), true,  "1-del, 2-mis", KMER_LEN); 
		checkBYS(k, t, DNAString.stringToBytes("ACCGATCCCA"), true,  "1-del, 2-del", KMER_LEN);
		checkBYS(k, t, DNAString.stringToBytes("ACCGATTTCC"), true,  "1-del, 2-ins", KMER_LEN);
		
		checkBYS(k, t, DNAString.stringToBytes("TACCGTTCCA"), true,  "1 del boundary", KMER_LEN);
		checkBYS(k, t, DNAString.stringToBytes("AACCGTCCCA"), false, "1 del boundary", KMER_LEN);
		checkBYS(k, t, DNAString.stringToBytes("AACCGCCCAA"), false, "1 del boundary", KMER_LEN);	
	}

	
	//------------------------- main --------------------------
	// Make sure alignments are fast and correct

	public static void main(String[] args) throws IOException 
	{		
		//1left-del-3 -> 0;10;10;TTTCTCAACA;ACACAGTATATC;ref;36;TTTCTCAAACACCTATATTTTTTG;ACACAGTATATCGTGTTGTGGACA

		int k = 5;
		configure(k);
		
		
		byte[] text    = DNAString.stringToBytes("TTTCTCAAACACCTATATTTTTTGT");
		byte[] pattern = DNAString.stringToBytes("TTTCTCAAACACCTATATTTTTT");

		//byte[] text    = DNAString.stringToBytes("TTTCTCAAACACCTATATTTTTT");
		//byte[] pattern = DNAString.stringToBytes("TTTCTCAAACACCTATATTTTTT");

		byte [] text_b    = DNAString.arrToDNA(text);
		byte [] pattern_b = DNAString.arrToDNA(pattern);
		
		AlignInfo a = kmismatch(text,pattern, k);
		System.out.println("ASCII:  " + a.toString());
		a.printAlignment(text, pattern);
		
		AlignInfo b = kmismatch_bin(text_b, pattern_b, k);
		System.out.println("Binary: " + b.toString());
		b.printAlignment(text, pattern);


		int num = 10000000;
		
		int dd = 0;
		Timer ta = new Timer();
		for (int i = 0; i < num; i++)
		{
			AlignInfo c = kmismatch(text, pattern, k);
			//dd = c.differences;
		}
		System.out.println("ASCII " + num + " took:" + ta.get());
		

		Timer tb = new Timer();
		for (int i = 0; i < num; i++)
		{
			AlignInfo c = kmismatch_bin(text_b, pattern_b, k);
//			if (c.differences != dd)
//			{
//				System.out.println("WTF: " + dd + " " + c.differences);
//				return;
//			}
		}
		System.out.println("Binary " + num + " took:" + tb.get());
		
		Timer tc = new Timer();
		for (int i = 0; i < num; i++)
		{
			AlignInfo c = kmismatch(text, pattern, k);
		}
		System.out.println("ASCII " + num + " took:" + tc.get());

		//debugAlignment(text, pattern, k, 10);
		
		//checkBY();
	}
}
