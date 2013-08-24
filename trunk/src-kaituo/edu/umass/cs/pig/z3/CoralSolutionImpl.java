package edu.umass.cs.pig.z3;

import java.util.Map;
import java.util.Map.Entry;

import org.apache.pig.data.Tuple;

import edu.umass.cs.pig.cntr.CountConstraint;
import edu.umass.cs.pig.numeric.CAndCCoral;
import edu.umass.cs.pig.z3.model.ModelConstants;
import edu.umass.cs.z3.SWIGTYPE_p__Z3_model;
import gnu.trove.TObjectDoubleHashMap;
import gnu.trove.TObjectFloatHashMap;
import gnu.trove.TObjectIntHashMap;
import gnu.trove.TObjectLongHashMap;
import gov.nasa.jpf.symbc.numeric.solvers.ProblemCoral;

public class CoralSolutionImpl implements ConstraintSolution {
	private final ModelConstants variablesAry1;
	private CAndCCoral cacc;
	private boolean duplicate;
	private CountConstraint cc = null;
//	private ProblemCoral pb = ProblemCoral.get();
	
	protected final ProblemCoral pb = ProblemCoral.get();
	
	public CoralSolutionImpl(CAndCCoral cacc, boolean d, CountConstraint c) {
		this.duplicate = d;
		this.cc = c;
		this.cacc = cacc;
		
		variablesAry1 = parse1AryVariables();
	}

	@Override
	public boolean isParsedOk() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public ModelConstants getVariablesAry1() {
		// TODO Auto-generated method stub
		return variablesAry1;
	}

	@Override
	public void deleteModel() {
		// TODO Auto-generated method stub
		
	}
	
	ModelConstants parse1AryVariables() {
		TObjectIntHashMap<String> intStr = new TObjectIntHashMap<String>(); 
		TObjectDoubleHashMap<String> doubleStr = new TObjectDoubleHashMap<String>();
		
		Map<String, Object> symRealVar = cacc.getSymRealVar();
		Map<String, Object> symIntVar = cacc.getSymIntVar();
		
		for (Entry<String, Object> entry : symRealVar.entrySet()) {
            String alias = entry.getKey();
            Object coralObj = entry.getValue();
            Object modelRealVal = pb.getRealValue(coralObj);
            if (modelRealVal != null) doubleStr.put(alias, (double) modelRealVal);
            else throw new RuntimeException("Unsolved variable for variable " + alias);
        }
		
		for (Entry<String, Object> entry : symIntVar.entrySet()) {
            String alias = entry.getKey();
            Object coralObj = entry.getValue();
            Object modelIntVal = pb.getIntValue(coralObj);
            if (modelIntVal != null) intStr.put(alias, (int) modelIntVal);
            else throw new RuntimeException("Unsolved variable for variable " + alias);
        }
		
		ModelConstants res = new ModelConstants(intStr, doubleStr, null, null);
		  
		return res;
	}

	@Override
	public CountConstraint getCc() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean isDuplicate() {
		// TODO Auto-generated method stub
		return false;
	}

}
