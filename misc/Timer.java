package misc;

import java.text.DecimalFormat;

public class Timer {
	
	private long startPoint;
	private long runtime;
	
	private static DecimalFormat dec_format = new DecimalFormat("#0.000");
	
	public Timer() {
		startPoint = 0;
		runtime = 0;
		
	}
	
	public void start() {
		startPoint = System.currentTimeMillis();
	}
	
	public void stop() {
		runtime += System.currentTimeMillis() - startPoint;
	}
	
	public long getRuntime() {
		return runtime;
	}
	
	public void reset() {
		runtime = 0;
	}
	
	public String getRuntimeAsString() {
		return getTimeAsString(runtime);
	}
	
	public static String getTimeAsString (long time) {
		return dec_format.format((double)time/1000.0) + " sec.";
	}
	 
	
}
