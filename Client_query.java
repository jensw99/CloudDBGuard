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
 * Abh��ngigkeiten:
 * 
 * maximal Java SE 1.8u144 wegen JCEKS keystore issue
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
public class Client_query {
	
	

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
		
		
		//BenchEnron be=new BenchEnron("enron","/Users/michaelbrenner/CloudDBGuard/tim/TimDB/enron.xml","password",dbtype,tableprofile);
		
		//System.out.println("new BenchEnron ok.");
		
		API api = new API("/Users/michaelbrenner/CloudDBGuard/tim/TimDB/enron.xml", "password",  false);
		

		System.out.println("Q0:");
		
		api.query(new String[]{"id"},
		"enron", "mail",
		new String[]{"receiver=thompson@enron.com", "receiver=ttt@cpuc.ca.gov"});
		
		api.query(new String[]{"id"},
		"enron", "mail",
		new String[]{"receiver=all.ees@enron.com"});
		
		api.query(new String[]{"id"},
		"enron", "mail",
		new String[]{"receiver=bmckay@houston.rr.com"});
		
		api.query(new String[]{"id"},
		"enron", "mail",
		new String[]{"receiver=edward.attanasio@enron.com"});
		
		api.query(new String[]{"id"},
		"enron", "mail",
		new String[]{"receiver=gene.humphrey@enron.com"});
		
		api.query(new String[]{"id"},
		"enron", "mail",
		new String[]{"receiver=hgovenar@govadv.com, mday@gmssr.com, bhansen@lhom.com, jdasovic@enron.com,smara@enron.com, paul.kaufman@enron.com, michael.mcdonald@enron.com,sandra.mccubbin@enron.com, rshapiro@enron.com,james.d.steffes@enron.com, acomnes@enron.com,steven.j.kean@enron.com, kdenne@enron.com, harry.kingerski@enron.com,leslie.lawner@enron.com, rfrank@enron.com, janel.guerrero@enron.com,miyung.buster@enron.com, jennifer.thome@enron.com, eletke@enron.com,mary.schoen@enron.com, david.leboe@enron.com, ban.sharma@enron.com,mark.palmer@enron.com"});
		
		api.query(new String[]{"id"},
		"enron", "mail",
		new String[]{"receiver=iucgroup@email.com, nco1@pge.com, peay@chevron.com, bairdd@pom-emh1.army.mil,john_baird@rmiinc.com, girish_balachandran@city.palo-alto.ca.us,igsinc@ix.netcom.com, bbates@coral-energy.com,pbaughman@urmgroup.com, blaising@jps.net, enfile@csc.com,bfa@bfa.com, scott_r_bond@amoco.com, tod_bradley@powerspring.com,bradylaw@pacbell.net, info@toenergy.com, bbrunel@smud.org,jbyron@calpine.com, tcabral@mediaone.com, callco@netdex.com,rcampbell@wwdb.org, rcelio@apt4power.com, craigc@calpine.com,bchen@newenergy.com, mclark@dgs.ca.gov, nclement@sempra-slns.com,jcresap_energysolutions@email.msn.com, sheila@wma.org,douglass@arterhadden.com, eric.eisenman@gen.pge.com,katie_elder@rmiinc.com, devans@smurfit.com,difellman@energy-law-group.com, merilyn_ferrara@apses.com,lfinne@unitedcogen.com, bgaillar@ees.enron.com, gansecka@sce.com,rgladman@utiliticorp.com, greenep@fosterfarms.com,whall@ci.glendale.ca.us, hamoyd@liggett-emh1.army.mil, klc@aelaw.com,chawes@eesinc.com, dhenton2@csc.com, bhong@sanfrancisco.usgen.com,artscomm@worldnet.att.net, lhurley@ci.redding.ca.us,sra.corp@swgas.com, ericj@eslawfirm.com, mjaske@energy.state.gov.us,regaffairs@sf.whitecase.com, kkelly@tfsbrokers.com,chris.king@utility.com, ole.kjosnes@ci.seattle.wa.us,pkatkandk@aol.com, ckulmat@utilicorp.com, dlf@cpuc.ca.gov,sslavigne@duke-energy.com, lawlerem@sce.com, jleslie@luce.com,alin@newenergy.com, nmlittle@duanemorris.com, clloyd@bart.gov,jlogsdon@csc.com, wsm@cpuc.ca.gov, rmccoy@dgs.ca.gov,pmcdonnell@eesinc.com, richard.mcelroy@engageenergy.com,patrickm@crossborderenergy.com, rmckilli@csc.com, karl@ncpa.com,jmiller@caiso.com, jsmollon@newwestenergy.com, fwmonier@tid.org,pmoritzburke@cera.com, philm@scdenergy.com, jim.neidig@indsys.ge.com,robert.nicholson@bankamerica.com, rick_noger@praxair.com,w6bd@msn.com, padillaj@vafb1a.vafb.af.mil, hpatrick@gmssr.com,pauj@dwt.com, jmpa@dynegy.com, naomi@sierracc.com, porterdk@sce.com,poynts@adi-tetrad.com, janp@mid.org, rpurves@sdge.com,rochmanm@spurr.org, jronning@dgs.ca.gov, frontdesk@sierracc.com,msanders@exeterassociates.com, pgs@ieee.org, sschlott@csc.com,anne@plurimi.com, aseshan@fifthgeneration.com,askaff@energy-law-group.com, jskillman@prodigy.net,william.stancer@ipaper.com, cstanfor@energy.twc.com,lsturdevant@sempra-slns.com, jtachera@energy.state.ca.us,tetsunari@reconcorp.com, mgr-fr@cwscommunities.com,jteague@dgs.ca.gov, athomas@newenergy.com, ntoyama@smud.org,ipt@best.com, nubavi@dwp.ci.la.ca.us, jungvari@rcsis.com,lgurick@calpx.com, jvaleri@sempra.com, wware@siliconvalleypower.com,jweil@aglet.org, ewestby@aandellp.com, kbw@cpuc.ca.gov,bwhitehurst@ci.healdsburg.ca.us, mwiggins@sempra.com, mike@elite.net,gwilliams@sppc.com, marywong@sempra.com, bwood@energy.state.ca.us,ceyap@earthlink.net, ed@clfp.com"});
		
		
		api.query(new String[]{"id"},
		"enron", "mail",
		new String[]{"receiver=john.massey@enron.com"});
		
		
		api.query(new String[]{"id"},
		"enron", "mail",
		new String[]{"receiver=kenneth.lay@enron.com, jeff.skilling@enron.com, jeff.dasovich@enron.com,susan.mara@enron.com, alan.comnes@enron.com,richard.shapiro@enron.com, james.steffes@enron.com,harry.kingerski@enron.com, ken@kdscommunications.com,elizabeth.tilney@enron.com, paula.rieker@enron.com,mark.koenig@enron.com, janel.guerrero@enron.com,paul.kaufman@enron.com, leslie.lawner@enron.com, hgovenar@govadv.com,sgovenar@govadv.com, robert.frank@enron.com,janet.dietrich@enron.com, john.lavorato@enron.com,greg.whalley@enron.com, david.delainey@enron.com,steven.kean@enron.com"});
		
		
		api.query(new String[]{"id"},
		"enron", "mail",
		new String[]{"receiver= rick.buy@enron.com, s..bradford@enron.com"});





		System.out.println("Q1:");

		api.query(new String[]{"id"},
		"enron", "mail",
		new String[]{"timestamp>16601757","timestamp<16601843"});
		
		api.query(new String[]{"id"},
		"enron", "mail",
		new String[]{"timestamp>16810074","timestamp<16810089"});
		
		api.query(new String[]{"id"},
		"enron", "mail",
		new String[]{"timestamp>16632767","timestamp<16632808"});
		
		api.query(new String[]{"id"},
		"enron", "mail",
		new String[]{"timestamp>16800192","timestamp<16800211"});
		
		api.query(new String[]{"id"},
		"enron", "mail",
		new String[]{"timestamp>16310396","timestamp<16310400"});
		
		api.query(new String[]{"id"},
		"enron", "mail",
		new String[]{"timestamp>16864903","timestamp<16864918"});
		
		api.query(new String[]{"id"},
		"enron", "mail",
		new String[]{"timestamp>16527927","timestamp<16527941"});
		
		api.query(new String[]{"id"},
		"enron", "mail",
		new String[]{"timestamp>15660790","timestamp<15660861"});
		
		api.query(new String[]{"id"},
		"enron", "mail",
		new String[]{"timestamp>16734815","timestamp<16734851"});
		
		api.query(new String[]{"id"},
		"enron", "mail",
		new String[]{"timestamp>16523882","timestamp<16523898"});


		System.out.println("Q2:");

		api.query(new String[]{"id"},
		"enron", "mail",
		new String[]{"subject#'04"});
		
		api.query(new String[]{"id"},
		"enron", "mail",
		new String[]{"subject#bart"});
		
		api.query(new String[]{"id"},
		"enron", "mail",
		new String[]{"subject#enlighten"});
		
		api.query(new String[]{"id"},
		"enron", "mail",
		new String[]{"subject#hicks"});
		
		api.query(new String[]{"id"},
		"enron", "mail",
		new String[]{"subject#holst"});
		
		api.query(new String[]{"id"},
		"enron", "mail",
		new String[]{"subject#ip-tv"});
		
		api.query(new String[]{"id"},
		"enron", "mail",
		new String[]{"subject#relook"});
		
		api.query(new String[]{"id"},
		"enron", "mail",
		new String[]{"subject#rp97-288-017"});
		
		api.query(new String[]{"id"},
		"enron", "mail",
		new String[]{"subject#teldata"});
		
		api.query(new String[]{"id"},
		"enron", "mail",
		new String[]{"subject#themesong"});



		System.out.println("Q3:");

		api.query(new String[]{"id"},
		"enron", "mail",
		new String[]{"writer=allen-p","receiver=hargr@webtv.net"});
		
		api.query(new String[]{"id"},
		"enron", "mail",
		new String[]{"writer=allen-p","receiver=ray.alvarez@enron.com"});
		
		api.query(new String[]{"id"},
		"enron", "mail",
		new String[]{"writer=allen-p","receiver=tori.kuykendall@enron.com"});
		
		api.query(new String[]{"id"},
		"enron", "mail",
		new String[]{"writer=arnold-j","receiver=coopers@epenergy.com"});
		
		api.query(new String[]{"id"},
		"enron", "mail",
		new String[]{"writer=arnold-j","receiver=danaggie@hotmail.com"});
		
		api.query(new String[]{"id"},
		"enron", "mail",
		new String[]{"writer=arnold-j","receiver=edie.leschber@enron.com"});
		
		api.query(new String[]{"id"},
		"enron", "mail",
		new String[]{"writer=arnold-j","receiver=mike.maggi@enron.com, john.griffith@enron.com, john.disturnal@enron.com"});
		
		api.query(new String[]{"id"},
		"enron", "mail",
		new String[]{"writer=arnold-j","receiver=rvujtech@carrfut.com"});
		
		api.query(new String[]{"id"},
		"enron", "mail",
		new String[]{"writer=arnold-j","receiver=stevelafontaine@bankofamerica.com"});
		
		api.query(new String[]{"id"},
		"enron", "mail",
		new String[]{"writer=arnold-j","receiver=vince.kaminski@enron.com"});

		System.out.println("Q4:");

		api.query(new String[]{"id"},
		"enron", "mail",
		new String[]{"writer=shankman-j","timestamp>16187787","timestamp<16188943"});
		
		api.query(new String[]{"id"},
		"enron", "mail",
		new String[]{"writer=shankman-j","timestamp>16719689","timestamp<16720766"});
		
		api.query(new String[]{"id"},
		"enron", "mail",
		new String[]{"writer=shankman-j","timestamp>16731074","timestamp<16731184"});
		
		api.query(new String[]{"id"},
		"enron", "mail",
		new String[]{"writer=shankman-j","timestamp>16739638","timestamp<16739896"});
		
		api.query(new String[]{"id"},
		"enron", "mail",
		new String[]{"writer=sturm-f","timestamp>16702370","timestamp<16715239"});
		
		api.query(new String[]{"id"},
		"enron", "mail",
		new String[]{"writer=sturm-f","timestamp>16870529","timestamp<16870941"});
		
		api.query(new String[]{"id"},
		"enron", "mail",
		new String[]{"writer=schoolcraft-d","timestamp>16578053","timestamp<16583996"});
		
		api.query(new String[]{"id"},
		"enron", "mail",
		new String[]{"writer=schoolcraft-d","timestamp>16623014","timestamp<16638615"});
		
		api.query(new String[]{"id"},
		"enron", "mail",
		new String[]{"writer=schoolcraft-d","timestamp>16650231","timestamp<16651976"});
		
		api.query(new String[]{"id"},
		"enron", "mail",
		new String[]{"writer=schoolcraft-d","timestamp>16850257","timestamp<16850539"});


		System.out.println("Q5:");

		api.query(new String[]{"id"},
		"enron", "mail",
		new String[]{"writer=shankman-j","body#boling"});
		
		api.query(new String[]{"id"},
		"enron", "mail",
		new String[]{"writer=shankman-j","body#yvonne"});
		
		api.query(new String[]{"id"},
		"enron", "mail",
		new String[]{"writer=sturm-f","body#portfolio"});
		
		api.query(new String[]{"id"},
		"enron", "mail",
		new String[]{"writer=sturm-f","body#potential"});
		
		api.query(new String[]{"id"},
		"enron", "mail",
		new String[]{"writer=schoolcraft-d","body#brasher"});
		
		api.query(new String[]{"id"},
		"enron", "mail",
		new String[]{"writer=schoolcraft-d","body#lo"});
		
		api.query(new String[]{"id"},
		"enron", "mail",
		new String[]{"writer=parks-j","body#sh"});
		
		api.query(new String[]{"id"},
		"enron", "mail",
		new String[]{"writer=parks-j","body#shameless"});
		
		api.query(new String[]{"id"},
		"enron", "mail",
		new String[]{"writer=rapp-b","body#age(tm)"});
		
		api.query(new String[]{"id"},
		"enron", "mail",
		new String[]{"writer=rapp-b","body#intraday"});


		System.out.println("Q6:");

		api.query(new String[]{"id"},
		"enron", "mail",
		new String[]{"writer>hodge-j","writer<quigley-d","timestamp>15624539","timestamp<15626123"});
		
		api.query(new String[]{"id"},
		"enron", "mail",
		new String[]{"writer>pimenov-v","writer<solberg-g","timestamp>16315639","timestamp<16315785"});
		
		api.query(new String[]{"id"},
		"enron", "mail",
		new String[]{"writer>linder-e","writer<sager-e","timestamp>16733622","timestamp<16733641"});
		
		api.query(new String[]{"id"},
		"enron", "mail",
		new String[]{"writer>kean-s","writer<scholtes-d","timestamp>16628909","timestamp<16628965"});
		
		api.query(new String[]{"id"},
		"enron", "mail",
		new String[]{"writer>maggi-m","writer<mclaughlin-e","timestamp>16794645","timestamp<16795666"});
		
		api.query(new String[]{"id"},
		"enron", "mail",
		new String[]{"writer>jones-t","writer<mclaughlin-e","timestamp>16801760","timestamp<16801812"});
		
		api.query(new String[]{"id"},
		"enron", "mail",
		new String[]{"writer>platter-p","writer<steffes-j","timestamp>16732592","timestamp<16732610"});
		
		api.query(new String[]{"id"},
		"enron", "mail",
		new String[]{"writer>fischer-m","writer<sanders-r","timestamp>16836230","timestamp<16836250"});
		
		api.query(new String[]{"id"},
		"enron", "mail",
		new String[]{"writer>smith-m","writer<steffes-j","timestamp>16853256","timestamp<16853775"});
		
		api.query(new String[]{"id"},
		"enron", "mail",
		new String[]{"writer>semperger-c","writer<zipper-a","timestamp>15640504","timestamp<15640680"});


		System.out.println("Q7:");

		api.query(new String[]{"id"},
		"enron", "mail",
		new String[]{"body#bds@cpuc"});
		
		api.query(new String[]{"id"},
		"enron", "mail",
		new String[]{"body#gridbusiness"});
		
		api.query(new String[]{"id"},
		"enron", "mail",
		new String[]{"body#energy---which"});
		
		api.query(new String[]{"id"},
		"enron", "mail",
		new String[]{"body#uwaterloo"});
		
		api.query(new String[]{"id"},
		"enron", "mail",
		new String[]{"body#236-9999"});
		
		api.query(new String[]{"id"},
		"enron", "mail",
		new String[]{"body#january@enron"});
		
		api.query(new String[]{"id"},
		"enron", "mail",
		new String[]{"body#gasnews"});
		
		api.query(new String[]{"id"},
		"enron", "mail",
		new String[]{"body#sofas"});
		
		api.query(new String[]{"id"},
		"enron", "mail",
		new String[]{"body#22chec","timestamp>16853256","timestamp<16853775"});
		
		api.query(new String[]{"id"},
		"enron", "mail",
		new String[]{"body#shaw's","timestamp>16853256","timestamp<16853775"});


//		System.out.println("Q8:");
//
//		api.query(new String[]{"id"},
//		"enron", "mail",
//		new String[]{"subject#feb","body#7-29"});
//		
//		api.query(new String[]{"id"},
//		"enron", "mail",
//		new String[]{"subject#technology's","body#measure"});
//		
//		api.query(new String[]{"id"},
//		"enron", "mail",
//		new String[]{"subject#makeup","body#team"});
//		
//		api.query(new String[]{"id"},
//		"enron", "mail",
//		new String[]{"subject#head","body#came"});
//		
//		api.query(new String[]{"id"},
//		"enron", "mail",
//		new String[]{"subject#water","body#liquid"});
//		
//		api.query(new String[]{"id"},
//		"enron", "mail",
//		new String[]{"subject#arts","body#art"});
//		
//		api.query(new String[]{"id"},
//		"enron", "mail",
//		new String[]{"subject#elf","body#for"});
//		
//		api.query(new String[]{"id"},
//		"enron", "mail",
//		new String[]{"subject#interface","body#physical"});
//		
//		api.query(new String[]{"id"},
//		"enron", "mail",
//		new String[]{"subject#16th","body#keep"});
//		
//		api.query(new String[]{"id"},
//		"enron", "mail",
//		new String[]{"subject#zach","body#work"});

		System.out.println("Q8:");

		api.query(new String[]{"id"},
		"enron", "mail",
		new String[]{"body#7-29"});
		
		api.query(new String[]{"id"},
		"enron", "mail",
		new String[]{"body#measure"});
		
		api.query(new String[]{"id"},
		"enron", "mail",
		new String[]{"body#team"});
		
		api.query(new String[]{"id"},
		"enron", "mail",
		new String[]{"body#came"});
		
		api.query(new String[]{"id"},
		"enron", "mail",
		new String[]{"body#liquid"});
		
		api.query(new String[]{"id"},
		"enron", "mail",
		new String[]{"body#art"});
		
		api.query(new String[]{"id"},
		"enron", "mail",
		new String[]{"body#for"});
		
		api.query(new String[]{"id"},
		"enron", "mail",
		new String[]{"body#physical"});
		
		api.query(new String[]{"id"},
		"enron", "mail",
		new String[]{"body#keep"});
		
		api.query(new String[]{"id"},
		"enron", "mail",
		new String[]{"body#work"});
		
		
		

		System.out.println("Q9:");

		api.query(new String[]{"id"},
		"enron", "mail",
		new String[]{"writer=shankman-j","timestamp>16733960","timestamp<16771133","body#shares"});
		
		api.query(new String[]{"id"},
		"enron", "mail",
		new String[]{"writer=shankman-j","timestamp>16225560","timestamp<16310031","body#me?"});
		
		api.query(new String[]{"id"},
		"enron", "mail",
		new String[]{"writer=shankman-j","timestamp>16174930","timestamp<16721079","body#charged"});
		
		api.query(new String[]{"id"},
		"enron", "mail",
		new String[]{"writer=shankman-j","timestamp>16285438","timestamp<16370448","body#values"});
		
		api.query(new String[]{"id"},
		"enron", "mail",
		new String[]{"writer=shankman-j","timestamp>16256715","timestamp<16278658","body#forster"});
		
		api.query(new String[]{"id"},
		"enron", "mail",
		new String[]{"writer=sturm-f","timestamp>16813658","timestamp<16870941","body#following"});
		
		api.query(new String[]{"id"},
		"enron", "mail",
		new String[]{"writer=sturm-f","timestamp>16509155","timestamp<16630273","body#our"});
		
		api.query(new String[]{"id"},
		"enron", "mail",
		new String[]{"writer=schoolcraft-d","timestamp>16720705","timestamp<16944427","body#interests"});
		
		api.query(new String[]{"id"},
		"enron", "mail",
		new String[]{"writer=schoolcraft-d","timestamp>16671585","timestamp<16945407","body#parts"});
		
		api.query(new String[]{"id"},
		"enron", "mail",
		new String[]{"writer=schoolcraft-d","timestamp>16739349","timestamp<16906670","body#associated"});

		api.close();
		
	
		System.out.println("ok.");
	}	
		
}
