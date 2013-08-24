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
import java.util.Random;

public class SymbolicReal extends RealExpression {
	public static double UNDEFINED = Double.MIN_VALUE;
	public double _min = -10000.0;
	public double _max = 10000.0;
	public double solution = UNDEFINED; // C
	public double solution_inf = UNDEFINED; // C
	public double solution_sup = UNDEFINED; // C

//	int unique_id;

	static String SYM_REAL_SUFFIX = "_SYMREAL";// C: what is this?
	private String name;

	public SymbolicReal () {
		super();
//		unique_id = MinMax.UniqueId++;
		//PathCondition.flagSolved = false;
		name = "REAL_" + hashCode();
//		_min = MinMax.getVarMinDouble(name);
//		_max = MinMax.getVarMaxDouble(name);
	}

	public SymbolicReal (String s) {
		super();
//		unique_id = MinMax.UniqueId++;
		//PathCondition.flagSolved = false;
		name = s;
//		_min = MinMax.getVarMinDouble(name);
//		_max = MinMax.getVarMaxDouble(name);
		//trackedSymVars.add(fixName(name));
	}

	public String getName() {
		return (name != null) ? name : "REAL_" + hashCode();
	}

	public String stringPC () {
		
		return (name != null) ? name + "[" + solution + /* "<" + solution_inf + "," + solution_sup + ">" + */  "]" :
			"REAL_" + hashCode() + "[" + solution + "]";
//			return (name != null) ? name + "[" + solution_inf + "," + solution_sup +  "]" :
//				"REAL_" + hashCode() + "[" + + solution_inf + "," + solution_sup +  "]";
	}

	public String toString () {
		return (name != null) ? name + "[" + solution + /* "<" + solution_inf + "," + solution_sup + ">" + */  "]" :
			"REAL_" + hashCode() + "[" + solution + "]";
//			return (name != null) ? name + "[" + solution_inf + "," + solution_sup +  "]" :
//				"REAL_" + hashCode() + "[" + + solution_inf + "," + solution_sup +  "]";
	}


	public double solution() {
		return solution;
	}

    public void getVarsVals(Map<String,Object> varsVals) {
    	varsVals.put(fixName(name), solution);
    }

    private String fixName(String name) {
    	if (name.endsWith(SYM_REAL_SUFFIX)) {
    		name = name.substring(0, name.lastIndexOf(SYM_REAL_SUFFIX));
    	}
    	return name;
    }

    public boolean equals (Object o) {
        return (o instanceof SymbolicReal) &&
               (this.equals((SymbolicReal) o));
    }
    
  
//	JacoGeldenhuys
	@Override
	public void accept(ConstraintExpressionVisitor visitor) {
		visitor.preVisit(this);
		visitor.postVisit(this);
	}

	@Override
	public int compareTo(Expression expr) {
		return getClass().getCanonicalName().compareTo(expr.getClass().getCanonicalName());
	}
}
