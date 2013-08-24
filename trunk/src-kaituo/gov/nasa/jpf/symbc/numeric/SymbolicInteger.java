//
//Copyright (C) 2006 United States Government as represented by the
//Administrator of the National Aeronautics and Space Administration
//(NASA).  All Rights Reserved.
//
//This software is distributed under the NASA Open Source Agreement
//(NOSA), version 1.3.  The NOSA has been approved by the Open Source
//Initiative.  See the file NOSA-1.3-JPF at the top of the distribution
//directory tree for the complete NOSA document.
//
//THE SUBJECT SOFTWARE IS PROVIDED "AS IS" WITHOUT ANY WARRANTY OF ANY
//KIND, EITHER EXPRESSED, IMPLIED, OR STATUTORY, INCLUDING, BUT NOT
//LIMITED TO, ANY WARRANTY THAT THE SUBJECT SOFTWARE WILL CONFORM TO
//SPECIFICATIONS, ANY IMPLIED WARRANTIES OF MERCHANTABILITY, FITNESS FOR
//A PARTICULAR PURPOSE, OR FREEDOM FROM INFRINGEMENT, ANY WARRANTY THAT
//THE SUBJECT SOFTWARE WILL BE ERROR FREE, OR ANY WARRANTY THAT
//DOCUMENTATION, IF PROVIDED, WILL CONFORM TO THE SUBJECT SOFTWARE.
//

package gov.nasa.jpf.symbc.numeric;

//import gov.nasa.jpf.symbc.SymbolicInstructionFactory;

import java.util.Map;
//import java.util.Random;

public class SymbolicInteger extends LinearIntegerExpression
{
	public static int UNDEFINED = Integer.MIN_VALUE;;
	public int _min = -1000000;
	public int _max = 1000000;
	public int solution = UNDEFINED; // C

	public static String SYM_INT_SUFFIX = "_SYMINT";
	private String name;

	public SymbolicInteger () {
		super();
		name = "INT_" + hashCode();
	}

	public SymbolicInteger (String s) {
		super();
		name = s;
	}

	public String getName() {
		return (name != null) ? name : "INT_" + hashCode();
	}

	public String stringPC () {
		return (name != null) ? name + "[" + solution + "]" :
				"INT_" + hashCode() + "[" + solution + "]";
	}

	public String toString () {
		return (name != null) ? name + "[" + solution + "]" :
				"INT_" + hashCode() + "[" + solution + "]";
	}

	public int solution() {
		return solution;
	}

    public void getVarsVals(Map<String,Object> varsVals) {
    	varsVals.put(fixName(name), solution);
    }

    private String fixName(String name) {
    	if (name.endsWith(SYM_INT_SUFFIX)) {
    		name = name.substring(0, name.lastIndexOf(SYM_INT_SUFFIX));
    	}
    	return name;
    }

    public boolean equals (Object o) {
        return (o instanceof SymbolicInteger) &&
               (this.equals((SymbolicInteger) o));
    }


    protected void finalize() throws Throwable {
    	//System.out.println("Finalized " + this);
    }
    
    @Override
	public void accept(ConstraintExpressionVisitor visitor) {
		visitor.preVisit(this);
		visitor.postVisit(this);
	}

	@Override
	public int compareTo(Expression expr) {
//		if (expr instanceof SymbolicInteger) {
//			SymbolicInteger e = (SymbolicInteger) expr;
//			int a = unique_id;
//			int b = e.unique_id;
//			return (a < b) ? -1 : (a > b) ? 1 : 0;
//		} else {
		return getClass().getCanonicalName().compareTo(expr.getClass().getCanonicalName());
		//}
	}
}
