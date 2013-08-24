package edu.umass.cs.pig.real;

/**
 * Do not use this class for now.  Apache Commons Math 
 * has had a Fraction class for quite some time.  
 * @author kaituo
 *
 */
public class ToFraction {
	public int[] toFraction(double d, int factor) {
//	    StringBuilder sb = new StringBuilder();
		boolean neg = false;
		int realParts[] = new int[2];
	    if (d < 0) {
//	        sb.append('-');
	    	neg = true;
	        d = -d;
	    }
	    long l = (long) d;
	    
	    d -= l;
	    double error = Math.abs(d);
	    int bestDenominator = 1;
	    for(int i=2;i<=factor;i++) {
	        double error2 = Math.abs(d - (double) Math.round(d * i) / i);
	        
	        if(Math.round((d + l) * bestDenominator) > 2147483647)
	        	break;
	        
	        if (error2 < error) {
	            error = error2;
	            bestDenominator = i;
	        }
	    }
	    if (bestDenominator > 1) {
	    	if (l != 0) {
//	    		sb.append(Math.round((d + l) * bestDenominator));
	    		realParts[0] = (int) Math.round((d + l) * bestDenominator);
	    	} else {
//	    		sb.append(Math.round(d * bestDenominator));
	    		realParts[0] = (int)Math.round(d * bestDenominator);
	    	}
//	        sb.append('/') .append(bestDenominator);
	    	realParts[1] = (int)bestDenominator;
	    } else {
//	    	sb.append(l);
	    	realParts[0] = (int)l;
	    }
	    
	    if(neg) {
	    	realParts[0] = realParts[0] * -1;
	    }
//	    return sb.toString();
	    return realParts;
	}
	
	public String toFractionString(double d, int factor) {
		int[] reals = toFraction(d, factor);
		StringBuilder sb = new StringBuilder();
		sb.append(reals[0]);
		if(reals[1] != 0)
			sb.append("/" + reals[1]);
		return sb.toString();
	}

	/*
	 *  4/3
	 *	8/7
	 *	PI 1: 3
	 *	PI 10: 22/7
	 *	PI 100: 311/99
	 *	PI 1000: 355/113
	 *	PI 10000: 355/113
	 *	PI 100000: 312689/99532
	 *	PI 1000000: 3126535/995207
	 *	PI 10000000: 5419351/1725033
	 */
	public static void main(String... args)  {
		ToFraction t = new ToFraction();
	    System.out.println(t.toFractionString(1.3333, 1000));
	    System.out.println(t.toFractionString(1.1428, 1000));
	    for(int i=1;i<100000000;i*=10) {
	        System.out.println("PI "+i+": "+t.toFractionString(3.1415926535897932385, i));
	    }
	}


}
