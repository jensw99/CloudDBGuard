import java.util.HashMap;
import java.util.Iterator;
import java.util.Random;
import java.util.TreeSet;

import misc.Timer;
import enums.DistributionProfile;
import enums.TableProfile;

/**
 * Class for benchmarking OPE schemes
 * 
 * @author Tim Waage
 */
public class BenchOPE {

	private String opeKeyspace;
		
	private long[] sortedKeys;
	private long[] optimalOrderedKeys;
	
	private int pos;
	
	// API object
	private API api;
	
	// mode
	private String mode;
		
	
	
	/**
	 * Constructor
	 * @param _ks the keyspace this benchmark will write to within the target database
	 * @param _db the database the benchmark is going to use
	 * @param _seScheme the scheme for searchable encryption the benchmark is going to use
	 * @param _detScheme the scheme for deterministic encryption the benchmark is going to use
	 * @param _opeScheme the scheme for order preserving encryption the benchmark is going to use
	 */
	public BenchOPE(String _ks, String _mode) {
		
		opeKeyspace = _ks;
		mode = _mode;
		
		api = new API("/Users/michaelbrenner/CloudDBGuard/tim/TimDB/enron.xml", "password",  false);
		
		
	}
	
	
	public void encrypt(int n) {
		
		// drop old keyspace
		api.dropKeyspace(opeKeyspace);
		
		// create new table
		api.addKeyspace(opeKeyspace, new String[]{"Cassandra->127.0.0.1"/*, "HBase->127.0.0.1"*/}, null, "password");
		
		api.addTable(opeKeyspace, "numbers", TableProfile.FAST, DistributionProfile.ROUNDROBIN, new String[]
				{"encrypted->Integer->number->rowkey",
				 "unencrypted->String->string"});
					
		
				
		// benchmark
		System.out.println("Importing " + n + " vlaues...");			
		
		int maxRangeValue = (int)Math.pow(2, 28);
		
		// the array that will contain the random test numbers
		long[] testdata = null;
		Random r = new Random();
				
		System.out.print("Preparing benchmark...");
		
		// best case: pre order traversal order
		if(mode.equals("best")) {
			
			sortedKeys = new long[n];
			optimalOrderedKeys = new long[n];
			
			// use a treeset to order n values
			TreeSet<Integer> test = new TreeSet<Integer>();
			while(test.size() < n) test.add(r.nextInt(maxRangeValue));
						
			Iterator<Integer> it = test.iterator();
			for(int i=0; i<n; i++) sortedKeys[i] = it.next();
			
			// perform pre-order traversal
			pos = -1;
			reorder(0, n-1);
			
			testdata = optimalOrderedKeys;
		}
		
		// average case: uniformly distributed values
		if(mode.equals("average")) {
			testdata = new long[n];
			for(int i=0; i<n; i++) testdata[i] = r.nextInt(maxRangeValue);
		}
		
		// worst case: ordered input
		if(mode.equals("worst")) {
			testdata = new long[n];
			
			// use a treeset to order n values
			TreeSet<Integer> test = new TreeSet<Integer>();
			while(test.size() < n) test.add(r.nextInt(maxRangeValue));
			
			Iterator<Integer> it = test.iterator();
			for(int i=0; i<n; i++) testdata[i] = it.next();
		}
		
		// to test negative and positive values as well, comment out if not needed
		//for(int x=0; x<testdata.length; x++) testdata[x] -= (maxRangeValue/2);		
		
		// to test negative values only, comment out if not needed
		// for(int x=0; x<testdata.length; x++) testdata[x] -= (maxRangeValue);		
				

		System.out.print(" done. [");
		
		for(int i=0; i<5; i++) System.out.print(testdata[i] + " ");
		System.out.println("...]");
		
		
		// perform the actual benchmark
		Timer t = new Timer();
			
		for(int i=0; i<n; i++) {
			
			t.start();
			
			long x = testdata[i];
			
			api.insertRow(opeKeyspace, "numbers", 
					new HashMap<String, String>(){				
						{ put("string", String.valueOf(x));
						 
						 }
					},
					new HashMap<String, Long>(){				
						{
						 put("number", x);
						}
					},
					null,										
					null,
					null,										
					null										
				);
			
			t.stop();
			
		}

		System.out.println(n + " numbers encrypted, " + mode + " case, time: " + t.getRuntimeAsString());
				
		api.close();		
	}
	
	
	

	
	/**
	 * finds the optimal order for inserting the keys (which is a pre-order traversal of the tree),
	 * helper function for the encryption part of the benchmark
	 * @param start
	 * @param end
	 */
	private void reorder(int start, int end) {
		
		pos++;
		
		if(start == end) optimalOrderedKeys[pos] = sortedKeys[start];
		else {
			
			int middle = start + (int)Math.floor((end - start)/2.0);
	
			optimalOrderedKeys[pos] = sortedKeys[middle];
		
			if(middle > start) reorder(start, middle - 1);
			if(middle < end) reorder(middle + 1, end);
		}
	}
}
