package edu.umass.cs.pig.dataflow;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.newplan.logical.relational.LogicalSchema;
import org.apache.pig.newplan.logical.relational.LogicalSchema.LogicalFieldSchema;
import org.apache.pig.pen.util.ExampleTuple;

import edu.umass.cs.pig.cntr.CountConstraint;
import edu.umass.cs.pig.z3.ConstraintSolution;
import edu.umass.cs.pig.z3.Z3SolutionImpl;
import edu.umass.cs.pig.z3.model.ModelConstants;
import gnu.trove.TObjectDoubleHashMap;
import gnu.trove.TObjectFloatHashMap;
import gnu.trove.TObjectIntHashMap;
import gnu.trove.TObjectLongHashMap;

public class ValueExtractor {
	/*
	 * TODO: There should be another extractsGenVal for extracting value of reals.
	 */
//	public Map<Integer, Integer> extractsGenVal(LogicalSchema l, ConstraintSolution c) {
//    	TObjectIntHashMap<String> vars = ((Z3SolutionImpl)c).getVariablesAry1();
//    	Map<Integer, Integer> ret = new HashMap<Integer, Integer>();
//    	for (String name: vars.keys(new String[vars.size()])) {
//      		int position;
//      		int value;
//			position = extractPosition(l, name);
//			if(position != -1) {
//				value = vars.get(name);
//				ret.put(position, value);
//			}
//      		
//      	}
//    	return ret;
//    }
	
//	public Map<Integer, Object> extractsGenVal(LogicalSchema l, ConstraintSolution c) {
//		ModelConstants mc = ((Z3SolutionImpl)c).getVariablesAry1();
//    	Map<Integer, Object> ret = new HashMap<Integer, Object>();
//    	
//    	TObjectIntHashMap<String> intVars = mc.getIntStr();
//    	if(!intVars.isEmpty()) {
//    		for (String name: intVars.keys(new String[intVars.size()])) {
//          		int position;
//          		int value;
//    			position = extractPosition(l, name);
//    			if(position != -1) {
//    				value = intVars.get(name);
//    				ret.put(position, value);
//    			}
//          		
//          	}
//    	}
//    	
//    	
//    	TObjectLongHashMap<String> longVars = mc.getLongStr();
//    	if(!longVars.isEmpty()) {
//    		for (String name: longVars.keys(new String[longVars.size()])) {
//          		int position;
//          		long value;
//    			position = extractPosition(l, name);
//    			if(position != -1) {
//    				value = longVars.get(name);
//    				ret.put(position, value);
//    			}
//          		
//          	}
//    	}
//    	
//    	
//    	TObjectDoubleHashMap<String> doubleVars = mc.getDoubleStr();
//    	if(!doubleVars.isEmpty()) {
//    		for (String name: doubleVars.keys(new String[doubleVars.size()])) {
//          		int position;
//          		double value;
//    			position = extractPosition(l, name);
//    			if(position != -1) {
//    				value = doubleVars.get(name);
//    				ret.put(position, value);
//    			}
//          		
//          	}
//    	}
//    	
//    	return ret;
//    }
	
	public Map<Integer, Object> extractsGenVal(LogicalSchema l, ConstraintSolution c) {
		//ModelConstants mc = ((Z3SolutionImpl)c).getVariablesAry1();
		ModelConstants mc = c.getVariablesAry1();
		Map<String, Object> name2ValueMap = mc.getStrValMap(c);
    	Map<Integer, Object> ret = new HashMap<Integer, Object>();
    	
    	for (Entry<String, Object> entry : name2ValueMap.entrySet()) {
            String name = entry.getKey();
            int position = extractPosition(l, name);
            Object value = entry.getValue();
            if(position != -1) {
				ret.put(position, value);
			}
        }
    	
    	TObjectIntHashMap<String> intVars = mc.getIntStr();
    	if(intVars != null && !intVars.isEmpty()) {
    		for (String name: intVars.keys(new String[intVars.size()])) {
          		int position;
          		int value;
    			position = extractPosition(l, name);
    			if(position != -1) {
    				value = intVars.get(name);
    				ret.put(position, value);
    			}
          		
          	}
    	}
    	
    	TObjectLongHashMap<String> longVars = mc.getLongStr();
    	if(longVars != null && !longVars.isEmpty()) {
    		for (String name: longVars.keys(new String[longVars.size()])) {
          		int position;
          		long value;
    			position = extractPosition(l, name);
    			if(position != -1) {
    				value = longVars.get(name);
    				ret.put(position, value);
    			}
          		
          	}
    	}
    	
    	TObjectFloatHashMap<String> floatVars = mc.getFloatStr();
    	if(floatVars != null && !floatVars.isEmpty()) {
    		for (String name: floatVars.keys(new String[floatVars.size()])) {
          		int position;
          		float value;
    			position = extractPosition(l, name);
    			if(position != -1) {
    				value = floatVars.get(name);
    				ret.put(position, value);
    			}
          		
          	}
    	}
    	
    	TObjectDoubleHashMap<String> doubleVars = mc.getDoubleStr();
    	if(doubleVars != null && !doubleVars.isEmpty()) {
    		for (String name: doubleVars.keys(new String[doubleVars.size()])) {
          		int position;
          		double value;
    			position = extractPosition(l, name);
    			if(position != -1) {
    				value = doubleVars.get(name);
    				ret.put(position, value);
    			}
          		
          	}
    	}
    	
    	return ret;
    }
	
	public List<Integer> extractsGenVal(LogicalSchema l, CountConstraint c) {
		List<String> cNames = c.getCountFields();
    	List<Integer> ret = new ArrayList<Integer>();

		int size = cNames.size();
		for(int i=0; i<size; i++) {
			String name = cNames.get(i);
			int position = extractPosition(l, name);
			ret.add(position);
		}
    	
    	return ret;
    }
	
	/**
	 * 
	 * @param l: parent path list schema
	 * @param t: parent path list tuple
	 * @param var: the name of variable in a path list relation
	 * @return the value of var in the parent pathlist t
	 */
	public ParentValueTNype extractsGenVal(ParentEncapsulation p, Long var) {
    	
		    LogicalSchema l = p.getLschema();
		    ExampleTuple t = p.getT();
      		int position;
      		Object value;
      		byte type;
      		ParentValueTNype ret;
			try {
				position = extractPosition(l, var);
				if(position != -1) {
					value = t.get(position);
	      			type = t.getType(position);
	      			ret = new ParentValueTNype(value, type);
	      			return ret;
				}
			} catch (ExecException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
      		
      	
    	return null;
		
	}
	
	public ParentValueTNype extractsGenVal(ParentEncapsulation p, String var) {

		LogicalSchema l = p.getLschema();
		ExampleTuple t = p.getT();
		int position;
		Object value;
		byte type;
		ParentValueTNype ret;
		try {
			position = extractPosition(l, var);
			if (position != -1) {
				value = t.get(position);
				type = t.getType(position);
				ret = new ParentValueTNype(value, type);
				return ret;
			}
		} catch (ExecException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return null;

	}
	
	public ParentValueTNype extractsGenVal(ParentEncapsulation p, VarInfo vinfo) {
		return extractsGenVal(p.getLschema(), p.getT(), vinfo.getAlias(), vinfo.getType());
	}
	
	public ParentValueTNype extractsGenVal(LogicalSchema l, ExampleTuple t, String var, byte tp) {

//		LogicalSchema l = p.getLschema();
//		ExampleTuple t = p.getT();
		int position;
		Object value;
		
		ParentValueTNype ret;
		try {
			position = extractPosition(l, var);
			if (position != -1) {
				value = t.get(position);
				
				ret = new ParentValueTNype(value, tp);
				return ret;
			}
		} catch (ExecException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return null;

	}
	
	public ParentValueTNype extractsGenVal(ParentEncapSol p, VarInfo vinfo) {
		if(p == null)
			return null;
		else if(p.getC() == null)
			return null;
		else
			return extractsGenVal(p.getC(), vinfo.getAlias(), vinfo.getType());
	}
	
//	public ParentValueTNype extractsGenVal(ConstraintSolution t, String var, byte tp) {
//
//		Object value;
//		
//		ParentValueTNype ret = null;
//		TObjectIntHashMap<String> vars = ((Z3SolutionImpl)t).getVariablesAry1();
//		value = vars.get(var);
//		if(value != null) {
//			ret = new ParentValueTNype(value, tp);
//		}
//		
//		return ret;
//	}
	
	public ParentValueTNype extractsGenVal(ModelConstants vars, String var, byte tp) {

		Object value;
		
		ParentValueTNype ret = null;
		value = vars.get(var, tp);
		if(value != null) {
			ret = new ParentValueTNype(value, tp);
		}
		
		return ret;
	}
	
	public int extractPosition(LogicalSchema l, String alias) {
		LogicalFieldSchema lfs = null;
		try {
			lfs = l.getFieldSubNameMatch(alias);
		} catch (FrontendException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
//		if (lfs == null && alias.indexOf(":") != -1) {
//			try {
//				int lastIndex = alias.lastIndexOf(":");
//				String simplifiedAlias = alias.substring(lastIndex+1);
//				lfs = l.getFieldSubNameMatch(simplifiedAlias);
//			} catch (FrontendException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
//		}
		int position;
		if ( lfs != null) {
  			position = l.getFields().indexOf(lfs);
  			return position;
		} 
		return -1;
	}
	
	public int extractPosition(LogicalSchema l, VarInfo vinfo) {
		String alias = vinfo.getAlias();
		return extractPosition(l, alias);
	}
	
	public int extractPosition(LogicalSchema l, Long uid) {
		int position;
		position = l.findField(uid);
		return position;
	}
	
	

}