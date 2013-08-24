package edu.umass.cs.pig.numeric;

import java.util.HashMap;
import java.util.Map;

import org.apache.pig.data.DataType;

import gov.nasa.jpf.symbc.numeric.MinMax;
import gov.nasa.jpf.symbc.numeric.solvers.ProblemCoral;

public class CAndCCoral {
	ProblemCoral pb;
	protected Map<String, Object> symRealVar; // a map between symbolic real variables and DP variables
	protected Map<String, Object> symIntVar; // a map between symbolic variables and DP variables
	
	public CAndCCoral(ProblemCoral pb) {
		super();
		this.pb = pb;
		symRealVar = new HashMap<String, Object>();
		symIntVar = new HashMap<String,Object>();
	}


	public Object checkCreateInt(String alias) {
		Object dp_var = symIntVar.get(alias);
		if (dp_var == null) {
			dp_var = pb.makeIntVar(alias, MinMax.MININT, MinMax.MAXINT);
			symIntVar.put(alias, dp_var);
		}
		return dp_var;
	}
	
	public Object checkCreateReal(String alias) {
		Object dp_var = symRealVar.get(alias);
		if (dp_var == null) {
			dp_var = pb.makeRealVar(alias, MinMax.MINDOUBLE, MinMax.MAXDOUBLE);
			symRealVar.put(alias, dp_var);
		}
		return dp_var;
	}
	
	public Object checkCreate(String alias, byte t) {
		if(t == DataType.INTEGER)
			return checkCreateInt(alias);
		else if (t == DataType.DOUBLE)
			return checkCreateReal(alias);
		else
			throw new RuntimeException("Unsupported data types");
	}


	public Map<String, Object> getSymRealVar() {
		return symRealVar;
	}


	public Map<String, Object> getSymIntVar() {
		return symIntVar;
	}


	

}
