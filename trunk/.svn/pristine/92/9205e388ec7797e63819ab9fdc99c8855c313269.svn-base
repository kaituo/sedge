package edu.umass.cs.pig.cntr;

import java.io.Serializable;
import java.util.ArrayList;

import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.newplan.logical.relational.LogicalSchema;

import edu.umass.cs.pig.ast.ConstraintResult;
import edu.umass.cs.pig.dataflow.VarInfo;
import edu.umass.cs.pig.util.Allocator4Z3;
import edu.umass.cs.pig.z3.Z3Context;
import edu.umass.cs.z3.SWIGTYPE_p__Z3_ast;

/**
 * childVar = value;
 * @author kaituo
 *
 */
public class EqualConstraint extends PltRelCntr implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	Object value;
	
	VarInfo childVar;
	VarInfo parentVar;
	
	public EqualConstraint(Object value, VarInfo childVar, VarInfo parentVar,
			LogicalSchema schema, byte mtype) {
		super(mtype, mtype);
		this.value = value;
		this.childVar = childVar;
		this.parentVar = parentVar;
	}
	
	public EqualConstraint(Object value, VarInfo childVar,
			LogicalSchema schema, byte mtype) {
		super(mtype, mtype);
		this.value = value;
		this.childVar = childVar;
		this.parentVar = null;
	}

	@Override
	public SWIGTYPE_p__Z3_ast asZ3ast(Z3Context z3, Allocator4Z3 alloc) {
		//String childVar = findChildVarName(childVarUid);
//		int lcol = extr.extractPosition(schema, childVar);
		ConstraintResult lhs = null;
		if(parentVar != null) {
			if(nameDB != null) {
				String aName = analyzeName(parentVar.getAlias());
				lhs = alloc.mallocInZ3(aName, memberType);
			}
			else
				lhs = alloc.mallocInZ3(parentVar, memberType);
		} else {
			if(nameDB != null) {
				String aName = analyzeName(childVar.getAlias());
				lhs = alloc.mallocInZ3(aName, memberType);
			}
			else
				lhs = alloc.mallocInZ3(childVar, memberType);
		}
		
		ConstraintResult assertAst;
		try {
			assertAst = alloc.mallocInZ3(value, memberType);
		} catch (FrontendException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
		SWIGTYPE_p__Z3_ast cntr = z3.mk_eq(lhs.getZ3ast(), assertAst.getZ3ast());
		return cntr;
	}
	
	
	
	public Object getValue() {
		return value;
	}
	public void setValue(Object value) {
		this.value = value;
	}
	
	public VarInfo getChildVar() {
		return childVar;
	}
	public void setChildVar(VarInfo childVar) {
		this.childVar = childVar;
	}
	
	/**
	 * Prove that a subclass of an abstract class is instanceof the abstract class.
	 * @param args
	 */
	public static void main(String[] args) {
		PltRelCntr e = new EqualConstraint(null, null, null, null, (byte) 0);
		if(e instanceof PltRelCntr)
			System.out.println("1");
		else
			System.out.println(e.getClass());
	}

//	@Override
//	public void accept(CntrVisitor v) throws FrontendException {
//		// TODO Auto-generated method stub
//		 if (!(v instanceof PltRelCntrVisitor)) {
//	            throw new FrontendException("Expected LogicalPlanVisitor", 2223);
//	        }
//	     ((PltRelCntrVisitor)v).visit(this);
//	}
	
	@Override
	public void syncNameDB(ArrayList<String> ndb) {
		nameDB = ndb;
	}

}
