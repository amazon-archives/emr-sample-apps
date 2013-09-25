package cloudBurst;

public class Timer {
	
	public long starttime;
	public long endtime;
	
	Timer()
	{
		starttime = System.currentTimeMillis();
	}
	
	double get()
	{
		endtime = System.currentTimeMillis();
		return (endtime - starttime) / 1000.0;
	}

}
