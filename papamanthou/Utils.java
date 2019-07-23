package papamanthou;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public class Utils {
	public static final int NOTFOUND = -1;
	public static final Charset utf8 = StandardCharsets.UTF_8;
	public static final byte pause = (byte) NOTFOUND;

	public static int byteArrayToInt(byte[] a) {
		int res = 0;
		int lastA = last(a);
		for (int i = 0; i <= lastA; i++) {
			res += Math.round(Math.pow(128, i)) * a[i];
		}
		return res;
	}

	public static byte[] intToByteArray(int a) {
		if (a == NOTFOUND) {
			byte[] notFound = { -1 };
			return notFound;
		}
		int size = (int) Math.max(Math.round(Math.ceil(Math.log(a) / Math.log(128))), 1);
		byte[] result = new byte[size];
		for (int i = 0; i < size; i++) {
			result[i] = (byte) (a % 128);
			a = a / 128;
		}
		return result;
	}

	public static int last(byte[] a) {
		int lastA;
		for (lastA = a.length - 1; a[lastA] == 0 && lastA > 0; lastA--) {
		}
		if (lastA == 0 && a[0] == 0) {
			lastA = -1;
		}
		return lastA;
	}

	public static byte[] xor(byte[] a, byte[] b) {
		int lastA = last(a);
		int lastB = last(b);
		int max = Math.max(lastA, lastB) + 1;

		byte[] amax = new byte[max];
		byte[] bmax = new byte[max];

		System.arraycopy(a, 0, amax, 0, lastA + 1);
		System.arraycopy(b, 0, bmax, 0, lastB + 1);

		byte[] c = new byte[max];
		for (int i = 0; i < max; i++) {
			int xor = amax[i] ^ bmax[i];
			c[i] = (byte) (0xff & xor);
		}

		return c;

	}
	
	public static byte[] concat(byte[] a, byte[] b)
	{
		byte[] result = new byte[a.length + b.length + 1];
		System.arraycopy(a, 0, result, 0, a.length);
		result[a.length] = pause;
		System.arraycopy(b, 0, result, a.length + 1, b.length);
		return result;
	}
	

}
