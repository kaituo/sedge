package edu.umass.cs.pig.numeric;

import edu.umass.cs.pig.sort.BitVector32Sort;
import edu.umass.cs.pig.sort.BitVector64Sort;
import edu.umass.cs.pig.sort.Fp32Sort;
import edu.umass.cs.pig.sort.Fp64Sort;
import edu.umass.cs.pig.z3.Z3Context;
import edu.umass.cs.z3.SWIGTYPE_p__Z3_ast;
import edu.umass.cs.z3.SWIGTYPE_p__Z3_symbol;


import java.util.HashMap;
import java.util.Map;

import org.apache.pig.data.DataType;

public class CAndCZ3 {
	final Z3Context z3;
	public static Map<String, SWIGTYPE_p__Z3_ast> symFloatVar; // a map between symbolic real variables and DP variables
	public static Map<String, SWIGTYPE_p__Z3_ast> symDoubleVar;
	public static Map<String, SWIGTYPE_p__Z3_ast> symIntVar; // a map between symbolic variables and DP variables
	public static Map<String, SWIGTYPE_p__Z3_ast> symLongVar;
	
	public CAndCZ3() {
		super();
		this.z3 = Z3Context.get();
		symFloatVar = new HashMap<String, SWIGTYPE_p__Z3_ast>();
		symIntVar = new HashMap<String,SWIGTYPE_p__Z3_ast>();
		symLongVar = new HashMap<String, SWIGTYPE_p__Z3_ast>();
		symDoubleVar = new HashMap<String, SWIGTYPE_p__Z3_ast>();
	}


	public SWIGTYPE_p__Z3_ast checkCreateInt(String alias) {
		SWIGTYPE_p__Z3_ast dp_var = symIntVar.get(alias);
		if (dp_var == null) {
			SWIGTYPE_p__Z3_symbol s = z3.mk_string_symbol(alias);
			dp_var = z3.mk_const(s, BitVector32Sort.getInstance().getZ3Sort());
			symIntVar.put(alias, dp_var);
		}
		return dp_var;
	}
	
	public SWIGTYPE_p__Z3_ast checkCreateLong(String alias) {
		SWIGTYPE_p__Z3_ast dp_var = symLongVar.get(alias);
		if (dp_var == null) {
			SWIGTYPE_p__Z3_symbol s = z3.mk_string_symbol(alias);
			dp_var = z3.mk_const(s, BitVector64Sort.getInstance().getZ3Sort());
			symIntVar.put(alias, dp_var);
		}
		return dp_var;
	}
	
	public SWIGTYPE_p__Z3_ast checkCreateFloat(String alias) {
		SWIGTYPE_p__Z3_ast dp_var = symFloatVar.get(alias);
		if (dp_var == null) {
			SWIGTYPE_p__Z3_symbol s = z3.mk_string_symbol(alias);
			dp_var = z3.mk_const(s, Fp32Sort.getInstance().getZ3Sort());
			symIntVar.put(alias, dp_var);
		}
		return dp_var;
	}
	
	public SWIGTYPE_p__Z3_ast checkCreateDouble(String alias) {
		SWIGTYPE_p__Z3_ast dp_var = symDoubleVar.get(alias);
		if (dp_var == null) {
			SWIGTYPE_p__Z3_symbol s = z3.mk_string_symbol(alias);
			dp_var = z3.mk_const(s, Fp64Sort.getInstance().getZ3Sort());
			symIntVar.put(alias, dp_var);
		}
		return dp_var;
	}
	
	public Object checkCreate(String alias, byte t) {
		if(t == DataType.INTEGER)
			return checkCreateInt(alias);
		else if (t == DataType.DOUBLE)
			return checkCreateDouble(alias);
		else if (t == DataType.LONG)
			return checkCreateLong(alias);
		else if (t == DataType.FLOAT)
			return checkCreateFloat(alias);
		else
			throw new RuntimeException("Unsupported data types");
	}

	

}
