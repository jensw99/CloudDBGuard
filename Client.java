import java.io.File;
import java.lang.reflect.Constructor;
import java.util.concurrent.ExecutorService;

import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.security.User;

import com.datastax.driver.core.Configuration;

import databases.DecryptedResults;
import enums.TableProfile;

/**
 * The client application
 * 
 * @author Tim Waage
 *
 *
 * Dependencies:
 * 
 * maximal Java SE 1.8u144 due to JCEKS keystore issue
 * 
 * 
 * bcprov-jdk15on-160.jar           
 * commons-codec-1.11.jar   
 * hadoop-common-2.7.7.jar  
 * jdom-2.0.6.jar          
 * netty-buffer-4.0.33.Final.jar  
 * netty-handler-4.0.33.Final.jar
 * cassandra-driver-core-3.0.1.jar  
 * commons-math3-3.6.1.jar  
 * hbase-client-2.1.2.jar   
 * log4j-1.2.17.jar        
 * netty-codec-4.0.33.Final.jar   
 * netty-transport-4.0.33.Final.jar
 * cassandra-driver-core-3.1.4.jar  
 * guava-16.0.1.jar         
 * hbase-common-2.1.2.jar   
 * metrics-core-3.1.2.jar  
 * netty-common-4.0.33.Final.jar  
 * slf4j-api-1.7.25.jar
 * 
 * 
 */
public class Client {
	
	

    /**
	 * entry point
	 * @param args see comments, help output
	 */
	public static void main(String[] args) {		
		
 		/*
 		
 		You can do whatever you want here, start like this:
				
		API api = new API("/some/path/to/an/xml", "password", false);
		
		DecryptedResults results = api.query(new String[]{"columns"}, // SELECT
				keyspace, table, 						    	      // FROM									
				new String[]{"attr1=x", "attr2=y"});				  // WHERE 
		
		
		api.close();
		
		Example Code for how to insert data can be found in BenchEnron.java
		
		*/
		
//		String className="org.apache.hadoop.hbase.client.ConnectionImplementation";
//		Class<?> clazz;
//	    try {
//	      clazz = Class.forName(className);
//	    } catch (ClassNotFoundException e) {
//	      throw new Exception(e);
//	    }
//	    try {
//	      
//	      Constructor<?> constructor = clazz.getDeclaredConstructor(Configuration.class,
//	        ExecutorService.class, User.class);
//	      constructor.setAccessible(true);
//	      Connection conn= (Connection) constructor.newInstance(conf, pool, user);
//	    } catch (Exception e) {
//	      throw new Exception(e);
//	    }
		
		if(args.length!=2)
		{
			System.out.println("Usage: Client CASSANDRA|HBASE SE|OW|OR");
			System.exit(-1);
		}
		
		int dbtype=0;
		
		if(args[0].equals("CASSANDRA"))
			dbtype=0;
		else
			dbtype=1;
		
		TableProfile tableprofile=null;
		
		if(args[1].equals("SE"))
			tableprofile=TableProfile.STORAGEEFFICIENT;
		else if(args[1].equals("OW"))
			tableprofile=TableProfile.OPTIMIZED_WRITING;
		else 
			tableprofile=TableProfile.OPTIMIZED_READING;
		
		BenchEnron be=new BenchEnron("enron","/Users/michaelbrenner/CloudDBGuard/tim/TimDB/enron.xml","password",dbtype,tableprofile);
		
		
		be.encrypt("/Users/michaelbrenner/CloudDBGuard/enron2015_10000");
		
		System.out.println("encrypt ok.");
	}	
		
}
