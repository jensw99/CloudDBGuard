import java.io.File;
import java.util.HashMap;
import java.util.HashSet;

import misc.Enron_Mail;
import misc.Timer;

import enums.DistributionProfile;
import enums.TableProfile;



/**
 * Class for encrypting and searching in the Enron data set, also compatible to the TREC spam corpus
 * @author Tim Waage
 *
 */
public class BenchEnron {

	private String enronKeyspaceName;
	
	// statistics
	private int filesAdded;
	private long timeForDBCommunication;
	private Timer timerWIthAPIOverhead;
	private Timer timerWithAPIOverheadAndParsing;
	
	// API object
	private API api;
	
	
	
	public BenchEnron(String _keyspace) {
				
		enronKeyspaceName = _keyspace;
		
		api = new API("/home/tim/TimDB/enron_se.xml", "password",  false);
	}
	
	
	public void encrypt(String path) {
		
		// reset everything
		filesAdded = 0;
		timeForDBCommunication = 0;
		timerWIthAPIOverhead = new Timer();
		timerWithAPIOverheadAndParsing = new Timer();
					
		// drop mails
		api.dropKeyspace(enronKeyspaceName);
		
		
		// create new enron mail table
		api.addKeyspace(enronKeyspaceName, new String[]{"Cassandra->127.0.0.1", "HBase->127.0.0.1"}, null, "password");
		
		api.addTable(enronKeyspaceName, "mail", TableProfile.STORAGEEFFICIENT, DistributionProfile.ROUNDROBIN, new String[]
				{"encrypted->String->id->rowkey",
				 "encrypted->String->sender",
				 "encrypted->String_set->receiver",
				 "encrypted->String_set->cc",
				 "encrypted->String_set->bcc",
				 "encrypted->String->subject",
				 "encrypted->String->body",
				 "encrypted->String->path",
				 "encrypted->Integer->year",
				 "encrypted->Integer->month",
				 "encrypted->Integer->day",
				 "encrypted->Integer->size",
				 "encrypted->Integer->timestamp",
				 "encrypted->String->xcc",
				 "encrypted->String->xfolder",
				 "encrypted->String->xorigin",
				 "encrypted->String->mimeversion",
				 "encrypted->String->xbcc",
				 "encrypted->String->xfilename",
				 "encrypted->String->xto",
				 "encrypted->String->cte",
				 "encrypted->String->xfrom",
				 "encrypted->String->contenttype",
				 "encrypted->String->writer",
				 });
					
		File dir = new File(path);
				
		// benchmark
		System.out.println("Importing " + dir.getAbsolutePath() + "...");			
		importDir(dir, dir.getAbsolutePath());
		
		api.close();
		
		// print statistics
		System.out.println("Insertion complete.");
		System.out.println(filesAdded + " files added");
		System.out.println("time for insertions excl. API overhead: " + Timer.getTimeAsString(timeForDBCommunication));
		System.out.println("time for insertions incl. API overhead: " + timerWIthAPIOverhead.getRuntimeAsString());
		System.out.println("time for insertions incl. parsing:      " + timerWithAPIOverheadAndParsing.getRuntimeAsString());
		
	}
	
	
	public void search() {
		
		// not needed anymore, as searching = querying via the API class
		
	}
	
	
	
	
	/**
	 * Populates the Database with (encrypted) testdata 
	 * @param input a File object representing a directory, should be the Enron root folder
	 * @param rootFolder the absolute path of the root folder, where the encryption starts
	 */
	public void importDir(File input, String rootFolder) {
		
		File[] inputFiles = input.listFiles();
		
		if (inputFiles != null) {
			for (int i = 0; i < inputFiles.length; i++) {
				
				if(inputFiles[i].isDirectory()) {
					importDir(inputFiles[i], rootFolder);
				}
				else {	
					importFile(inputFiles[i].getAbsolutePath());
					//System.out.println("Adding: " + inputFiles[i].getAbsolutePath());
					filesAdded++;
				}
		    }
		}
		
		
	}
	
	
	
	/**
	 * Encrypts a file according to the given parameters and the parameter specified in the constructor
	 * @param inputFilePath name of the input file
	 * @return time needed for insertion incl API overhead
	 */
	protected long importFile(String inputFilePath) {
		
		timerWithAPIOverheadAndParsing.start();
		Enron_Mail mail = Enron_Mail.parseFile(inputFilePath);
		
		timerWIthAPIOverhead.start();
		
		timeForDBCommunication += api.insertRow(enronKeyspaceName, "mail", 
				new HashMap<String, String>(){				// Strings
					{
					 if(mail.getID() != null) put("id", mail.getID());
					 if(mail.getFrom() != null) put("sender", mail.getFrom());
					 if(mail.getSubject() != null) put("subject", mail.getSubject());
					 if(mail.getBody() != null) put("body", mail.getBody());
					 if(mail.getPath() != null) put("path", mail.getPath());
					 if(mail.getXCc() != null) put("xcc", mail.getXCc());
					 if(mail.getXFolder() != null) put("xfolder", mail.getXFolder());
					 if(mail.getXOrigin() != null) put("xorigin", mail.getXOrigin());
					 if(mail.getMimeVersion() != null) put("mimeversion", mail.getMimeVersion());
					 if(mail.getXBcc() != null) put("xbcc", mail.getXBcc());
					 if(mail.getXFileName() != null) put("xfilename", mail.getXFileName());
					 if(mail.getXTo() != null) put("xto", mail.getXTo());
					 if(mail.getContentTransferEncoding() != null) put("cte", mail.getContentTransferEncoding());
					 if(mail.getXFrom() != null) put("xfrom", mail.getXFrom());
					 if(mail.getContentType() != null) put("contenttype", mail.getContentType());
					 if(mail.getWriter() != null) put("writer", mail.getWriter());
					 }
				},
				new HashMap<String, Long>(){				// Numerical Values
					{
					 put("size", mail.getSize());
					 put("year", mail.getDateYear());
					 put("month", mail.getDateMonth());
					 put("day", mail.getDateDay());
					 put("timestamp", mail.getTimestamp());
					}
				},
				null,										// Byte Blobs
				new HashMap<String, HashSet<String>>(){		// String sets
					{
					if(mail.getTo() != null) put("receiver", mail.getTo());
					if(mail.getCc() != null) put("cc", mail.getCc());
				    if(mail.getBcc() != null) put("bcc", mail.getBcc());
					}
				},
				null,										// Numerical sets
				null										// Byte Blob Sets
			);
		
		timerWIthAPIOverhead.stop();
		timerWithAPIOverheadAndParsing.stop();
		
		return timerWIthAPIOverhead.getRuntime();
	};

}
