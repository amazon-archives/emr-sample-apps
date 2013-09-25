package cloudBurst;

public class SharedSeedRecord 
{	
	public int     offset = 0;
    public int     seedLength = 0;
    public boolean isRC = false;
	public byte[]  leftFlank;
	public byte[]  rightFlank;
		
	public int     refID;
	public int     refOffset = 0;
	public byte[]  refLeftFlank;
	public byte[]  refRightFlank;
	
	SharedSeedRecord()
	{
		
	}
	
	private static final StringBuilder builder = new StringBuilder();
	
	public String toString()
	{
		builder.setLength(0);
		
		builder.append(isRC ? 1 : 0);                           builder.append(';');
		builder.append(offset);		                            builder.append(';');
		builder.append(seedLength);                             builder.append(';');
		builder.append(DNAString.bytesToString(leftFlank));     builder.append(';');
		builder.append(DNAString.bytesToString(rightFlank));    builder.append(';');
		builder.append(refID);                                  builder.append(';');
		builder.append(refOffset);                              builder.append(';');
		builder.append(DNAString.bytesToString(refLeftFlank));  builder.append(';');
		builder.append(DNAString.bytesToString(refRightFlank)); 
		
		return builder.toString();
	}
}