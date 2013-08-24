package edu.umass.cs.pig.test;

import java.math.BigInteger;

public class TestParse {
	// Long has a minimum value of -9,223,372,036,854,775,808 and
	// a maximum value of 9,223,372,036,854,775,807 (inclusive).
	public static void main(String[] args) {
		BigInteger b = new BigInteger("9223372036854775909");
		System.out.println(b.longValue());
	    System.out.println(Long.parseLong("-3"));
	    System.out.println(TestParse.parseLong("9223372036854775909", 10));
	}
	
	public static long parseLong(String s, int radix)
			throws NumberFormatException {
		if (s == null) {
			throw new NumberFormatException("null");
		}

		if (radix < Character.MIN_RADIX) {
			throw new NumberFormatException("radix " + radix
					+ " less than Character.MIN_RADIX");
		}
		if (radix > Character.MAX_RADIX) {
			throw new NumberFormatException("radix " + radix
					+ " greater than Character.MAX_RADIX");
		}

		long result = 0;
		boolean negative = false;
		int i = 0, len = s.length();
		long limit = -Long.MAX_VALUE;
		long multmin;
		int digit;

		if (len > 0) {
			char firstChar = s.charAt(0);
//			if (firstChar < '0') { // Possible leading "+" or "-"
//				if (firstChar == '-') {
//					negative = true;
//					limit = Long.MIN_VALUE;
//				} else if (firstChar != '+')
//					throw new NumberFormatException(s);
//
//				if (len == 1) // Cannot have lone "+" or "-"
//					throw new NumberFormatException(s);
//				i++;
//			}
			if(firstChar == '1') {
				negative = true;
				limit = Long.MIN_VALUE;
				i++;
			}
			multmin = limit / radix;
			while (i < len) {
				// Accumulating negatively avoids surprises near MAX_VALUE
				digit = Character.digit(s.charAt(i++), radix);
				if (digit < 0) {
					throw new NumberFormatException(s);
				}
				if (result < multmin) {
					throw new NumberFormatException(s);
				}
				result *= radix;
				if (result < limit + digit) {
					throw new NumberFormatException(s);
				}
				result -= digit;
			}
		} else {
			throw new NumberFormatException(s);
		}
		return negative ? result : -result;
	}

}
