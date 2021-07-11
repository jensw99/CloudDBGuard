import misc.Timer;

public class ClientQueryUnencrypted {
	public static int counter = 0;
	

	public static void main(String[] args) {		

		// Select database
		//DBCassandraUnencrypted db = new DBCassandraUnencrypted("localhost", 9042);
		DBHBaseUnencrypted db = new DBHBaseUnencrypted("localhost", 2181);
		
		long time = 0;

		System.out.println("Q0:");
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"receiver=thompson@enron_unencrypted.com", "receiver=ttt@cpuc.ca.gov"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"receiver=all.ees@enron_unencrypted.com"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"receiver=bmckay@houston.rr.com"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"receiver=edward.attanasio@enron_unencrypted.com"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"receiver=gene.humphrey@enron_unencrypted.com"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"receiver=hgovenar@govadv.com, mday@gmssr.com, bhansen@lhom.com, jdasovic@enron_unencrypted.com,smara@enron_unencrypted.com, paul.kaufman@enron_unencrypted.com, michael.mcdonald@enron_unencrypted.com,sandra.mccubbin@enron_unencrypted.com, rshapiro@enron_unencrypted.com,james.d.steffes@enron_unencrypted.com, acomnes@enron_unencrypted.com,steven.j.kean@enron_unencrypted.com, kdenne@enron_unencrypted.com, harry.kingerski@enron_unencrypted.com,leslie.lawner@enron_unencrypted.com, rfrank@enron_unencrypted.com, janel.guerrero@enron_unencrypted.com,miyung.buster@enron_unencrypted.com, jennifer.thome@enron_unencrypted.com, eletke@enron_unencrypted.com,mary.schoen@enron_unencrypted.com, david.leboe@enron_unencrypted.com, ban.sharma@enron_unencrypted.com,mark.palmer@enron_unencrypted.com"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"receiver=iucgroup@eenron.com, nco1@pge.com, peay@chevron.com, bairdd@pom-emh1.army.mil,john_baird@rmiinc.com, girish_balachandran@city.palo-alto.ca.us,igsinc@ix.netcom.com, bbates@coral-energy.com,pbaughman@urmgroup.com, blaising@jps.net, enfile@csc.com,bfa@bfa.com, scott_r_bond@amoco.com, tod_bradley@powerspring.com,bradylaw@pacbell.net, info@toenergy.com, bbrunel@smud.org,jbyron@calpine.com, tcabral@mediaone.com, callco@netdex.com,rcampbell@wwtime += db.org, rcelio@apt4power.com, craigc@calpine.com,bchen@newenergy.com, mclark@dgs.ca.gov, nclement@sempra-slns.com,jcresap_energysolutions@eenron.msn.com, sheila@wma.org,douglass@arterhadden.com, eric.eisenman@gen.pge.com,katie_elder@rmiinc.com, devans@smurfit.com,difellman@energy-law-group.com, merilyn_ferrara@apses.com,lfinne@unitedcogen.com, bgaillar@ees.enron_unencrypted.com, gansecka@sce.com,rgladman@utiliticorp.com, greenep@fosterfarms.com,whall@ci.glendale.ca.us, hamoyd@liggett-emh1.army.mil, klc@aelaw.com,chawes@eesinc.com, dhenton2@csc.com, bhong@sanfrancisco.usgen.com,artscomm@worldnet.att.net, lhurley@ci.redding.ca.us,sra.corp@swgas.com, ericj@eslawfirm.com, mjaske@energy.state.gov.us,regaffairs@sf.whitecase.com, kkelly@tfsbrokers.com,chris.king@utility.com, ole.kjosnes@ci.seattle.wa.us,pkatkandk@aol.com, ckulmat@utilicorp.com, dlf@cpuc.ca.gov,sslavigne@duke-energy.com, lawlerem@sce.com, jleslie@luce.com,alin@newenergy.com, nmlittle@duanemorris.com, clloyd@bart.gov,jlogsdon@csc.com, wsm@cpuc.ca.gov, rmccoy@dgs.ca.gov,pmcdonnell@eesinc.com, richard.mcelroy@engageenergy.com,patrickm@crossborderenergy.com, rmckilli@csc.com, karl@ncpa.com,jmiller@caiso.com, jsmollon@newwestenergy.com, fwmonier@tid.org,pmoritzburke@cera.com, philm@scdenergy.com, jim.neidig@indsys.ge.com,robert.nicholson@bankamerica.com, rick_noger@praxair.com,w6bd@msn.com, padillaj@vafb1a.vafb.af.mil, hpatrick@gmssr.com,pauj@dwt.com, jmpa@dynegy.com, naomi@sierracc.com, porterdk@sce.com,poynts@adi-tetrad.com, janp@mid.org, rpurves@sdge.com,rochmanm@spurr.org, jronning@dgs.ca.gov, frontdesk@sierracc.com,msanders@exeterassociates.com, pgs@ieee.org, sschlott@csc.com,anne@plurimi.com, aseshan@fifthgeneration.com,askaff@energy-law-group.com, jskillman@prodigy.net,william.stancer@ipaper.com, cstanfor@energy.twc.com,lsturdevant@sempra-slns.com, jtachera@energy.state.ca.us,tetsunari@reconcorp.com, mgr-fr@cwscommunities.com,jteague@dgs.ca.gov, athomas@newenergy.com, ntoyama@smud.org,ipt@best.com, nubavi@dwp.ci.la.ca.us, jungvari@rcsis.com,lgurick@calpx.com, jvaleri@sempra.com, wware@siliconvalleypower.com,jweil@aglet.org, ewestby@aandellp.com, kbw@cpuc.ca.gov,bwhitehurst@ci.healdsburg.ca.us, mwiggins@sempra.com, mike@elite.net,gwilliams@sppc.com, marywong@sempra.com, bwood@energy.state.ca.us,ceyap@earthlink.net, ed@clfp.com"});
		
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"receiver=john.massey@enron_unencrypted.com"});
		
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"receiver=kenneth.lay@enron_unencrypted.com, jeff.skilling@enron_unencrypted.com, jeff.dasovich@enron_unencrypted.com,susan.mara@enron_unencrypted.com, alan.comnes@enron_unencrypted.com,richard.shapiro@enron_unencrypted.com, james.steffes@enron_unencrypted.com,harry.kingerski@enron_unencrypted.com, ken@kdscommunications.com,elizabeth.tilney@enron_unencrypted.com, paula.rieker@enron_unencrypted.com,mark.koenig@enron_unencrypted.com, janel.guerrero@enron_unencrypted.com,paul.kaufman@enron_unencrypted.com, leslie.lawner@enron_unencrypted.com, hgovenar@govadv.com,sgovenar@govadv.com, robert.frank@enron_unencrypted.com,janet.dietrich@enron_unencrypted.com, john.lavorato@enron_unencrypted.com,greg.whalley@enron_unencrypted.com, david.delainey@enron_unencrypted.com,steven.kean@enron_unencrypted.com"});
		
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"receiver= rick.buy@enron_unencrypted.com, s..bradford@enron_unencrypted.com"});
		
		
		System.out.println("Q0: "+ Timer.getTimeAsString(time));
		System.out.println("Treffer: "+ClientQueryUnencrypted.counter);
		ClientQueryUnencrypted.counter = 0;
		
		time = 0;
		
		System.out.println("Q'0:");
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"receiver=harry.arora@enron.com, suresh.raghavan@enron.com, pamela.chambers@enron.com"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"receiver=pushkar.shahi@enron.com"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"receiver=alan.aronowitz@enron.com, susan.bailey@enron.com, samantha.boyd@enron.com"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"receiver=jeff.bartlett@enron.com"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"receiver=john.gillespie@enron.com, derryl.cleaveland@enron.com,"
				+ "	robert.johansen@enron.com, calvin.eakins@enron.com,"
				+ "	kelly.higgason@enron.com, drew.ries@enron.com, trang.dinh@enron.com,"
				+ "	jennifer.medcalf@enron.com, george.wasaff@enron.com"});//
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"receiver=scrimale.bob@bcg.com, scullion.chuck@bcg.com, jcwwh@aol.com,"
				+ "	holsinger.jill@bcg.com, rudge.lori@bcg.com, pieroni.molly@bcg.com,"
				+ "	vanyo.rebecca@bcg.com, padgett.rebekah@bcg.com, hill.thad@bcg.com,"
				+ "	cox.john@bcg.com, pucket.j@bcg.com, nicol.ron@bcg.com,"
				+ "	balagopal.balu@bcg.com, waddy@earthcareus.com,"
				+ "	david.schaller.wg96@wharton.upenn.edu, karutz.george@bcg.com,"
				+ "	jasonreed@wingatepartners.com, jim@smartprice.com,"
				+ "	varadarajan.raj@bcg.com, jim@smartprice.com, john.arnold@enron.com,"
				+ "	michael.wong@enron.com, sjones@dfw.scaconsulting.com"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"receiver=brianherrick@email.msn.com, herriceu2@tdprs.state.tx.us,"
				+ "	robertherrick@bankunited.com, kristi.demaiolo@enron.com,"
				+ "	suresh.raghavan@enron.com, harry.arora@enron.com"});
		
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"receiver=harry.arora@enron.com"});
		
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"receiver=rebende@earthlink.net, jimgriffeth@compuserve.com,"
				+ "	kevinfinnan@compuserve.com"});
		
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"receiver= jennifer.medcalf@enron.com"});
		
		
		System.out.println("Q'0: "+ Timer.getTimeAsString(time));
		System.out.println("Treffer: "+ClientQueryUnencrypted.counter);
		ClientQueryUnencrypted.counter = 0;
		time = 0;


		System.out.println("Q1:");

		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"timestamp>16601757","timestamp<16601843"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"timestamp>16810074","timestamp<16810089"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"timestamp>16632767","timestamp<16632808"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"timestamp>16800192","timestamp<16800211"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"timestamp>16310396","timestamp<16310400"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"timestamp>16864903","timestamp<16864918"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"timestamp>16527927","timestamp<16527941"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"timestamp>15660790","timestamp<15660861"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"timestamp>16734815","timestamp<16734851"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"timestamp>16523882","timestamp<16523898"});
		
		System.out.println("Q1: "+ Timer.getTimeAsString(time));
		System.out.println("Treffer: "+ClientQueryUnencrypted.counter);
		ClientQueryUnencrypted.counter = 0;
		time = 0;
		
		System.out.println("Q'1:");

		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"timestamp>16801757","timestamp<16946568"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"timestamp>16800000","timestamp<16810089"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"timestamp>16603000","timestamp<16632808"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"timestamp>16770831","timestamp<16800211"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"timestamp>16250000","timestamp<16310400"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"timestamp>16762159","timestamp<16864918"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"timestamp>16447927","timestamp<16527941"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"timestamp>16866006","timestamp<16946568"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"timestamp>16500000","timestamp<16734851"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"timestamp>16611341","timestamp<16746568"});
		
		System.out.println("Q'1: "+ Timer.getTimeAsString(time));
		System.out.println("Treffer: "+ClientQueryUnencrypted.counter);
		ClientQueryUnencrypted.counter = 0;
		time = 0;

		System.out.println("Q2:");

		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"subject#'04"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"subject#bart"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"subject#enlighten"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"subject#hicks"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"subject#holst"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"subject#ip-tv"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"subject#relook"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"subject#rp97-288-017"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"subject#teldata"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"subject#themesong"});

		System.out.println("Q2: "+ Timer.getTimeAsString(time));
		System.out.println("Treffer: "+ClientQueryUnencrypted.counter);
		ClientQueryUnencrypted.counter = 0;
		time = 0;
		
		System.out.println("Q'2:");

		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"subject#Structure"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"subject#Lakewood"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"subject#Team"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"subject#Generation"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"subject#PARIBAS"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"subject#Brother"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"subject#25%"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"subject#Your"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"subject#morning"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"subject#Tour"});

		System.out.println("Q'2: "+ Timer.getTimeAsString(time));
		System.out.println("Treffer: "+ClientQueryUnencrypted.counter);
		ClientQueryUnencrypted.counter = 0;
		time = 0;

		System.out.println("Q3:");

		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"writer=allen-p","receiver=hargr@webtv.net"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"writer=allen-p","receiver=ray.alvarez@enron_unencrypted.com"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"writer=allen-p","receiver=tori.kuykendall@enron_unencrypted.com"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"writer=arnold-j","receiver=coopers@epenergy.com"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"writer=arnold-j","receiver=danaggie@hotenron.com"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"writer=arnold-j","receiver=edie.leschber@enron_unencrypted.com"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"writer=arnold-j","receiver=mike.maggi@enron_unencrypted.com, john.griffith@enron_unencrypted.com, john.disturnal@enron_unencrypted.com"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"writer=arnold-j","receiver=rvujtech@carrfut.com"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"writer=arnold-j","receiver=stevelafontaine@bankofamerica.com"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"writer=arnold-j","receiver=vince.kaminski@enron_unencrypted.com"});
		
		System.out.println("Q3: "+ Timer.getTimeAsString(time));
		System.out.println("Treffer: "+ClientQueryUnencrypted.counter);
		ClientQueryUnencrypted.counter = 0;
		time = 0;
		
		System.out.println("Q'3:");

		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"writer=allen-p","receiver=hargr@webtv.net"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"writer=arora-h","receiver=harry.arora@enron.com"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"writer=bailey-s","receiver=alan.aronowitz@enron.com, susan.bailey@enron.com, samantha.boyd@enron.com"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"writer=arnold-j","receiver=coopers@epenergy.com"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"writer=arora-h","receiver=louise.kitchen@enron.com"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"writer=arora-h","receiver=jeff.bartlett@enron.com"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"writer=arora-h","receiver=karissa.johnson@enron.com"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"writer=arnold-j","receiver=rvujtech@carrfut.com"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"writer=arnold-j","receiver=stevelafontaine@bankofamerica.com"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"writer=arnold-j","receiver=jennifer.medcalf@enron.com, david.spurlin@compaq.com, jeff.gooden@compaq.com"});
		
		System.out.println("Q'3: "+ Timer.getTimeAsString(time));
		System.out.println("Treffer: "+ClientQueryUnencrypted.counter);
		ClientQueryUnencrypted.counter = 0;
		time = 0;

		System.out.println("Q4:");

		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"writer=shankman-j","timestamp>16187787","timestamp<16188943"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"writer=shankman-j","timestamp>16719689","timestamp<16720766"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"writer=shankman-j","timestamp>16731074","timestamp<16731184"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"writer=shankman-j","timestamp>16739638","timestamp<16739896"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"writer=sturm-f","timestamp>16702370","timestamp<16715239"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"writer=sturm-f","timestamp>16870529","timestamp<16870941"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"writer=schoolcraft-d","timestamp>16578053","timestamp<16583996"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"writer=schoolcraft-d","timestamp>16623014","timestamp<16638615"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"writer=schoolcraft-d","timestamp>16650231","timestamp<16651976"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"writer=schoolcraft-d","timestamp>16850257","timestamp<16850539"});
		
		System.out.println("Q4: "+ Timer.getTimeAsString(time));
		System.out.println("Treffer: "+ClientQueryUnencrypted.counter);
		ClientQueryUnencrypted.counter = 0;
		time = 0;
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"writer=arora-h","timestamp>16187787","timestamp<16769840"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"writer=arora-h","timestamp>16719689","timestamp<16820766"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"writer=arora-h","timestamp>16631074","timestamp<16731184"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"writer=arora-h","timestamp>16730638","timestamp<16739896"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"writer=allen-p","timestamp>16702370","timestamp<16715239"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"writer=arora-h","timestamp>16800529","timestamp<16870941"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"writer=arora-h","timestamp>16578053","timestamp<16583996"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"writer=arora-h","timestamp>16623014","timestamp<16638615"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"writer=arora-h","timestamp>16650231","timestamp<16751976"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"writer=allen-p","timestamp>16700257","timestamp<16990539"});
		
		System.out.println("Q'4: "+ Timer.getTimeAsString(time));
		System.out.println("Treffer: "+ClientQueryUnencrypted.counter);
		ClientQueryUnencrypted.counter = 0;
		time = 0;

		System.out.println("Q5:");

		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"writer=shankman-j","body#boling"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"writer=shankman-j","body#yvonne"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"writer=sturm-f","body#portfolio"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"writer=sturm-f","body#potential"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"writer=schoolcraft-d","body#brasher"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"writer=schoolcraft-d","body#lo"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"writer=parks-j","body#sh"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"writer=parks-j","body#shameless"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"writer=rapp-b","body#age(tm)"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"writer=rapp-b","body#intraday"});
		
		System.out.println("Q5: "+ Timer.getTimeAsString(time));
		System.out.println("Treffer: "+ClientQueryUnencrypted.counter);
		ClientQueryUnencrypted.counter = 0;
		time = 0;
		
		System.out.println("Q'5:");

		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"writer=allen-p","body#He"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"writer=allen-p","body#Phantom"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"writer=allen-p","body#portfolio"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"writer=allen-p","body#potential"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"writer=allen-p","body#Section"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"writer=arnold-j","body#Management"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"writer=arnold-j","body#Risk"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"writer=arnold-j","body#less"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"writer=arnold-j","body#credits"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"writer=arnold-j","body#intraday"});
		
		System.out.println("Q'5: "+ Timer.getTimeAsString(time));
		System.out.println("Treffer: "+ClientQueryUnencrypted.counter);
		ClientQueryUnencrypted.counter = 0;
		time = 0;
		

		System.out.println("Q6:");

		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"writer>hodge-j","writer<quigley-d","timestamp>15624539","timestamp<15626123"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"writer>pimenov-v","writer<solberg-g","timestamp>16315639","timestamp<16315785"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"writer>linder-e","writer<sager-e","timestamp>16733622","timestamp<16733641"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"writer>kean-s","writer<scholtes-d","timestamp>16628909","timestamp<16628965"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"writer>maggi-m","writer<mclaughlin-e","timestamp>16794645","timestamp<16795666"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"writer>jones-t","writer<mclaughlin-e","timestamp>16801760","timestamp<16801812"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"writer>platter-p","writer<steffes-j","timestamp>16732592","timestamp<16732610"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"writer>fischer-m","writer<sanders-r","timestamp>16836230","timestamp<16836250"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"writer>smith-m","writer<steffes-j","timestamp>16853256","timestamp<16853775"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"writer>semperger-c","writer<zipper-a","timestamp>15640504","timestamp<15640680"});
		
		System.out.println("Q6: "+ Timer.getTimeAsString(time));
		System.out.println("Treffer: "+ClientQueryUnencrypted.counter);
		ClientQueryUnencrypted.counter = 0;
		time = 0;

		System.out.println("Q'6:");

		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"writer>allen-p","writer<bass-e","timestamp>15624539","timestamp<15926123"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"writer>allen-p","writer<bass-e","timestamp>16315639","timestamp<16415785"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"writer>allen-p","writer<bass-e","timestamp>16733622","timestamp<16833641"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"writer>allen-p","writer<bass-e","timestamp>16628909","timestamp<16728965"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"writer>allen-p","writer<bass-e","timestamp>16794645","timestamp<16895666"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"writer>allen-p","writer<mclaughlin-e","timestamp>16801760","timestamp<16811812"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"writer>allen-p","writer<bailey-s","timestamp>16732592","timestamp<16832610"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"writer>allen-p","writer<bailey-s","timestamp>16836230","timestamp<16936250"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"writer>allen-p","writer<bailey-s","timestamp>16853256","timestamp<16953775"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"writer>allen-p","writer<bailey-s","timestamp>15540504","timestamp<15940680"});
		
		System.out.println("Q'6: "+ Timer.getTimeAsString(time));
		System.out.println("Treffer: "+ClientQueryUnencrypted.counter);
		ClientQueryUnencrypted.counter = 0;
		time = 0;
		
		System.out.println("Q7:");

		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"body#bds@cpuc","timestamp>16271199", "timestamp<16568201"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"body#gridbusiness", "timestamp>16886332", "timestamp<16913712"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"body#energy---which", "timestamp>16644660","timestamp<16650463"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"body#uwaterloo", "timestamp>16248189", "timestamp<16640465"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"body#236-9999", "timestamp>16680405", "timestamp<16780139"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"body#january@enron_unencrypted", "timestamp>16758830", "timestamp<16905073"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"body#gasnews", "timestamp>16768572", "timestamp<16921711"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"body#sofas", "timestamp>16721061", "timestamp<16731122"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"body#22chec","timestamp>16773686", "timestamp<16776591"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"body#shaw's","timestamp>16349357","timestamp<16720746"});
		
		System.out.println("Q7: "+ Timer.getTimeAsString(time));
		System.out.println("Treffer: "+ClientQueryUnencrypted.counter);
		ClientQueryUnencrypted.counter = 0;
		time = 0;
		
		System.out.println("Q'7:");

		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"body#Questions","timestamp>16271199", "timestamp<16568201"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"body#Exposure", "timestamp>16886332", "timestamp<16913712"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"body#people", "timestamp>16644660","timestamp<16650463"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"body#good", "timestamp>16248189", "timestamp<16640465"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"body#Case", "timestamp>16680405", "timestamp<16780139"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"body#writer", "timestamp>16758830", "timestamp<16905073"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"body#Specials", "timestamp>16768572", "timestamp<16921711"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"body#of", "timestamp>16721061", "timestamp<16731122"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"body#Access","timestamp>16773686", "timestamp<16776591"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"body#file","timestamp>16349357","timestamp<16720746"});
		
		System.out.println("Q'7: "+ Timer.getTimeAsString(time));
		System.out.println("Treffer: "+ClientQueryUnencrypted.counter);
		ClientQueryUnencrypted.counter = 0;
		time = 0;

		System.out.println("Q8:");

		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"subject#feb","body#7-29"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"subject#technology's","body#measure"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"subject#makeup","body#team"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"subject#head","body#came"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"subject#water","body#liquid"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"subject#arts","body#art"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"subject#elf","body#for"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"subject#interface","body#physical"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"subject#16th","body#keep"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"subject#zach","body#work"});
		
		System.out.println("Q8: "+ Timer.getTimeAsString(time));
		System.out.println("Treffer: "+ClientQueryUnencrypted.counter);
		ClientQueryUnencrypted.counter = 0;
		time = 0;
		
		System.out.println("Q'8:");

		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"subject#Scheduling","body#request"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"subject#Member","body#transfer"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"subject#to","body#information"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"subject#Letter","body#draft"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"subject#CAISO","body#DMA"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"subject#IMPORTANT","body#of"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"subject#Generators:","body#market"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"subject#CSFB","body#New"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"subject#Mentions","body#Fitch"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"subject#Re:","body#phone"});
		
		System.out.println("Q'8: "+ Timer.getTimeAsString(time));
		System.out.println("Treffer: "+ClientQueryUnencrypted.counter);
		ClientQueryUnencrypted.counter = 0;
		time = 0;
/*
		System.out.println("Q8:");

		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"body#7-29"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"body#measure"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"body#team"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"body#came"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"body#liquid"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"body#art"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"body#for"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"body#physical"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"body#keep"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"body#work"});
*/		
		

		System.out.println("Q9:");

		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"writer=shankman-j","timestamp>16733960","timestamp<16771133","body#shares"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"writer=shankman-j","timestamp>16225560","timestamp<16310031","body#me?"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"writer=shankman-j","timestamp>16174930","timestamp<16721079","body#charged"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"writer=shankman-j","timestamp>16285438","timestamp<16370448","body#values"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"writer=shankman-j","timestamp>16256715","timestamp<16278658","body#forster"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"writer=sturm-f","timestamp>16813658","timestamp<16870941","body#following"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"writer=sturm-f","timestamp>16509155","timestamp<16630273","body#our"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"writer=schoolcraft-d","timestamp>16720705","timestamp<16944427","body#interests"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"writer=schoolcraft-d","timestamp>16671585","timestamp<16945407","body#parts"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"writer=schoolcraft-d","timestamp>16739349","timestamp<16906670","body#associated"});
		
		System.out.println("Q9: "+ Timer.getTimeAsString(time));
		System.out.println("Treffer: "+ClientQueryUnencrypted.counter);
		ClientQueryUnencrypted.counter = 0;
		time = 0;
		
		System.out.println("Q'9:");

		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"writer=allen-p","timestamp>16733960","timestamp<16771133","body#shares"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"writer=allen-p","timestamp>16225560","timestamp<16310031","body#me"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"writer=allen-p","timestamp>16174930","timestamp<16721079","body#charged"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"writer=allen-p","timestamp>16285438","timestamp<16370448","body#values"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"writer=allen-p","timestamp>16256715","timestamp<16278658","body#promotion"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"writer=arnold-j","timestamp>16813658","timestamp<16870941","body#following"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"writer=arnold-j","timestamp>16509155","timestamp<16630273","body#our"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"writer=arnold-j","timestamp>16720705","timestamp<16944427","body#interests"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"writer=arora-h","timestamp>16671585","timestamp<16945407","body#parts"});
		
		time += db.query(new String[]{"id"},
		"enron_unencrypted", "enron",
		new String[]{"writer=arora-h","timestamp>16739349","timestamp<16906670","body#associated"});
		
		System.out.println("Q'9: "+ Timer.getTimeAsString(time));
		System.out.println("Treffer: "+ClientQueryUnencrypted.counter);
		ClientQueryUnencrypted.counter = 0;

		db.close();
		
	
		System.out.println("ok.");
	}	
		
}

