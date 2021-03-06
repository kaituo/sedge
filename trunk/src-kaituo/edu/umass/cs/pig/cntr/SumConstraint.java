package edu.umass.cs.pig.cntr;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.FrontendException;

import edu.umass.cs.pig.ast.ConstraintResult;
import edu.umass.cs.pig.dataflow.VarInfo;
import edu.umass.cs.pig.util.Allocator4Z3;
import edu.umass.cs.pig.z3.Z3Context;
import edu.umass.cs.z3.SWIGTYPE_p__Z3_ast;

/**
 * This is a constraint that adds two numbers together.
 * sum = childVar + value;
 * I don't know excatly how to deal with aggregates like SUM.  I cannot bound the length (the number of items to be added together).  
 * Previously I told you I can assume the length is 2.  This assumption only holds in the following case: 
 *
 *	A = LOAD ...
 *	A1 = GROUP A ...
 *	E = SUM(A1) 
 *	
 *	Since each group has exactly two rows in its nested table, we can say in E, SUM is applied two rows for each group.  
 *  But for the following example, this assumption doesn't hold:
 *	
 *	A = LOAD ...
 *	B = LOAD ...
 *	A1 = GROUP A ...
 *	B1 = GROUP B ...
 *	C = UNION(A1, B1)
 *	D = JOIN C with A
 *	E = SUM(D)
 *	
 *	    A            B
 *	    |  \           |
 *	   A1  \        B1
 *	     \    |       /
 *	      \    \    /
 *	           C
 *	            |
 *	           D
 *	            |
 *	           E
 *	
 *	The equivalence class definitions GROUP will force tables A1 and B1 to have two rows each.  Now C should have 4 rows.  
 *  Based on the definition of JOIN, D would not add any extra rows.  So  E would have 4 elements.
 *	
 *	There are three ways to handle aggregates that have unbounded length:
 *  (a) statically predict the maximum number of elements in a table;
 *  (b) introduce suitable length variables;
 *  (c) introduce case handling for all relevant size cases;
 *  
 *  I use option (a).  Whenever I see an aggregate function like SUM, I detect its predecessor operator P:
 *  (1) if P is GROUP, then I know the length is 2;
 *  (2) if P is UNION, then I need to do recursive down-top or top-down check to get the length number.  I don't bother to implement
 *  this option as I don't see any need involved.
 * @author kaituo
 *
 */
public class SumConstraint extends PltRelCntr implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	Object parentValue, sumValue;
	VarInfo parentVar;
	VarInfo childVar;
	VarInfo sum;
	
	//LogicalSchema schema, byte type, 
	public SumConstraint(List<VarInfo> parentVar, List<VarInfo> childVar,
			 List<VarInfo> sum, byte mType, byte rType) {
		this(null, null, parentVar, childVar, sum, mType, rType);
	}
	
	//LogicalSchema schema, 
	//byte type, 
	public SumConstraint(Object valueSum, Object valueParent, List<VarInfo> parentVar, List<VarInfo> childVar,
			List<VarInfo> sum, byte mType, byte rType) {
		super(mType, rType);
		this.sumValue = valueSum;
		this.parentValue = valueParent;
		this.childVar = getFirstElem(childVar);
		this.parentVar = getFirstElem(parentVar);
		this.sum = getFirstElem(sum);
	}

	public SWIGTYPE_p__Z3_ast asZ3ast(Z3Context z3, Allocator4Z3 alloc) throws FrontendException {
		//String childVar = findChildVarName(childVarUid);
		//int lcol = extr.extractPosition(schema, childVar);
//		if(childVar.getAlias().equals("x"))
//			return null;
		if(parentValue == null || sumValue == null)
			throw new FrontendException(
                    "parent or sum's value should be set before declaring the SumConstraint into Z3.");
		byte resultType = sum.getType();
		ConstraintResult sumAst = alloc.mallocInZ3(sum, resultType);
		ConstraintResult parentValueAst, sumValueAst;
		try {
			parentValueAst = alloc.mallocInZ3N2(parentValue, resultType);
			sumValueAst = alloc.mallocInZ3N2(sumValue, resultType);
		} catch (FrontendException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
		ConstraintResult childVarAst = alloc.mallocInZ3(childVar, resultType);;
		SWIGTYPE_p__Z3_ast rhs = Z3Context.get().mk_bvadd(
				parentValueAst.getZ3ast(), childVarAst.getZ3ast());
//	    OverflowUnderFlow flow = new OverflowUnderFlow();
//
//		flow.bvaddouflow(parentValueAst, childVarAst);
		
		// Assert sum = a + b
		SWIGTYPE_p__Z3_ast cntr = z3.mk_eq(sumAst.getZ3ast(), rhs);
		
		
		// Assert sum <= MAX_INT and sum >= MIN_INT
		if(sum.getType() !=  parentVar.getType()) {
			if(parentVar.getType() == DataType.INTEGER) {
				FakeLimit fl = new FakeLimit();
				boolean assertFl =  fl.assertFakeLimitIntConversion(childVarAst.getZ3ast());
//				boolean assertFl =  fl.assertFakeLimitLong(childVarAst.getZ3ast());

				if(!assertFl)
					throw new FrontendException(
							"Failed to assert fake integer limit.");
			}
		}
		
		// Assert sum = sum's value in the model
		SWIGTYPE_p__Z3_ast cntr2 = z3.mk_eq(sumAst.getZ3ast(), sumValueAst.getZ3ast());
		z3.assert_cnstr(cntr2);
			
		return cntr;
	}

	public Object getParentValue() {
		return parentValue;
	}

	public void setParentValue(Object parentValue) {
		this.parentValue = parentValue;
	}

	public Object getSumValue() {
		return sumValue;
	}

	public void setSumValue(Object sumValue) {
		this.sumValue = sumValue;
	}

	public VarInfo getSum() {
		return sum;
	}

	public void setSum(VarInfo sum) {
		this.sum = sum;
	}

	

//	@Override
//	public void accept(CntrVisitor v) throws FrontendException {
//		// TODO Auto-generated method stub
//		if (!(v instanceof PltRelCntrVisitor)) {
//			throw new FrontendException("Expected LogicalPlanVisitor", 2223);
//		}
//		((PltRelCntrVisitor) v).visit(this);
//	}
	
	// Don't support conversion between local names and global names yet
	@Override
	public void syncNameDB(ArrayList<String> ndb) {}

}
