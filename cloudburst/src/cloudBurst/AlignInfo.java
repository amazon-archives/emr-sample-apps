package cloudBurst;

public class AlignInfo {
	public int alignlen;
	public int differences;
	public int [] dist;
	public int [] what;
	public int distlen;
	
	private static final StringBuilder builder = new StringBuilder();
	
	
	//------------------------- Constructor --------------------------
	public AlignInfo(int len, int k, int [] pdist, int [] pwhat, int dlen)
	{
		setVals(len, k, pdist, pwhat, dlen);
	}

	
	//------------------------- setVals --------------------------
	public void setVals(int len, int k, int [] pdist, int [] pwhat, int dlen)
	{
		alignlen = len;
		differences = k;
		dist = pdist;
		what = pwhat;
		distlen = dlen;
	}
	
	
	//------------------------- isBazeaYatesSeed --------------------------
	// Since an alignment may be recompute k+1 times for each of the k+1 seeds,
	// see if the current alignment is the leftmost alignment by checking for
	// differences in the proceeding chunks of the query
	
	public boolean isBazeaYatesSeed(int qlen, int kmerlen)
	{	
		int numBuckets = qlen / kmerlen;
		
		int lastbucket = -1;
		int distdelta = 0;
		int pos = 0;
		
		for (int i = 0; i < distlen; i++)
		{
			pos += dist[i] + distdelta;
			
			distdelta = 0;
			if (what [i] == 2)
			{
				// end of string
				continue;
			}
			else if (what[i] == -1)
			{
				// gap character occurs between pos and pos+1
				if (pos % kmerlen == 0)
				{
					// occurs right between buckets, skip
					continue;
				}
			}
	
			int bucket = pos / kmerlen;
			if (bucket - lastbucket > 1) { return false; }
			lastbucket = bucket;
		}
		
		return (lastbucket == numBuckets-1);
	}


	//------------------------- isBazeaYatesSeed --------------------------
	// Run isBazeaYates seed, but output some debugging info
	
	public boolean isBazeaYatesSeedDebug(int qlen, int kmerlen)
	{	
		int numBuckets = qlen / kmerlen;
		
		System.out.println("KMER_LEN: " + kmerlen + " numbuckets: " + numBuckets);
		
		int lastbucket = -1;
		int distdelta = 0;
		int pos = 0;
		for (int i = 0; i < distlen; i++)
		{
			pos += dist[i] + distdelta;
			
			distdelta = 0;
			if (what [i] == 2)
			{
				// end of string
				continue;
			}
			else if (what[i] == 0)
			{
				
			}
			else if (what[i] == 1)
			{
				
			}
			else if (what[i] == -1)
			{
				// gap character occurs between pos and pos+1
				if (pos % kmerlen == 0)
				{
					// occurs right between buckets, skip
					System.out.println(i + ": pos: " + pos 
				             + " dist: " + dist[i] 
				             + " what: " + what[i] 
				             + " between bucket: " + pos/kmerlen);
					continue;
				}
			}

			
			int bucket = pos / kmerlen;
			
			System.out.println(i + ": pos: " + pos 
					             + " dist: " + dist[i] 
					             + " what: " + what[i] 
					             + " bucket: " + bucket);
			if (bucket - lastbucket > 1) { return false; }
			lastbucket = bucket;
		}
		
		return (lastbucket == numBuckets-1);		
	}

	

	//------------------------- toString --------------------------
	public String toString()
	{
		builder.setLength(0);
		
		builder.append(alignlen);    builder.append(';');
		builder.append(differences); builder.append(';');
		
		for (int i = 0; i < distlen; i++)
		{
			builder.append(dist[i]); builder.append(';');
		}
		
		for (int i = 0; i < distlen; i++)
		{
			builder.append(what[i]); builder.append(';');
		}
		
		return builder.toString();
	}

	
	//------------------------- printAlignment --------------------------
	// print out the aligned strings, with gaps as necessary
	
	public void printAlignment(byte[] t, byte[] p)
	{
		if (dist == null)
		{
			System.out.print("t: " );
			for (int i = 0; i < t.length; i++)
			{
				System.out.print((char) t[i]);
			}
		}
		else
		{
			System.out.print("a: ");
			int pos = 0;
			int nextstride = 0;
			for (int i = 0; i < distlen; i++)
			{
				if (what[i] == 2) { break; }
				int stride = dist[i] + nextstride;
				for (int j = 0; j < stride; j++) { System.out.print(" "); }
				System.out.print("*");
				
				nextstride = 0;
				if ((what[i] == 1) || (what[i] == 0)) { nextstride = -1; }
			}
				
			System.out.println();
			
			
			System.out.print("t: ");
			pos = 0;
			nextstride = 0;
			for (int i = 0; i < distlen; i++)
			{
				int stride = dist[i] + nextstride;
			
				if (what[i] == 1)  { nextstride = -1; }
				else               { nextstride = 0; }
			
				for (int j = 0; j < stride; j++, pos++)
				{
					System.out.print((char) t[pos]);
				}
			
				if (what[i] == 1)
				{
					System.out.print('-');
				}
				else if (what[i] == -1)
				{
					System.out.print((char) t[pos]);
					pos++;
				}
			}
		}	
		
		System.out.println();
		
		System.out.print("p: ");
		
		if (dist == null)
		{
			for (int i = 0; i < p.length; i++)
			{
				System.out.print((char) p[i]);
			}
		}
		else
		{
			int pos = 0;
			for (int i = 0; i < distlen; i++)
			{
				for (int j = 0; j < dist[i]; j++, pos++)
				{
					System.out.print((char) p[pos]);
				}
			
				if (what[i] == -1) { System.out.print('-'); }
			}
		}
		
		System.out.println();
	}
}
