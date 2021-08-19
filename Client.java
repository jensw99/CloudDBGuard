import java.io.File;
import java.lang.reflect.Constructor;
import java.util.concurrent.ExecutorService;

import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.security.User;


import databases.DecryptedResults;
import enums.TableProfile;

/**
 * The client application
 * 
 * @author Tim Waage, Jens Weigel
 * 
 * Dependencies:
 * 
 * jdom-2.0.6.jar
 * Datastax Cassandra Java Driver 4.11.0 + their Dependencies
 * HBase Driver 2.4.4 + their Dependencies
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
		
		BenchEnron be=new BenchEnron("enron","C:/Users/Jens/OneDrive/Uni/Bachelor_Uni_Frankfurt/Bachelorarbeit/Metadata/enron.xml","password",dbtype,tableprofile);
		
		
		be.encrypt("C:/enron2015_10000");
		
		System.out.println("encrypt ok.");
	}	
		
}
