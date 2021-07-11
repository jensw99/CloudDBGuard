
public class ClientUnencrypted {
	public static void main(String[] args) {
		BenchEnronUnencrypted beu = new BenchEnronUnencrypted("localhost", 9042);
		beu.upload("C:/enron2015_10000");
	}
}
