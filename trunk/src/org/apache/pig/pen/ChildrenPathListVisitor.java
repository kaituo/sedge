package org.apache.pig.pen;

import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.PlanWalker;
import org.apache.pig.newplan.logical.relational.LOLoad;
import org.apache.pig.newplan.logical.relational.LogicalRelationalNodesVisitor;

public class ChildrenPathListVisitor extends LogicalRelationalNodesVisitor {
	
	protected ChildrenPathListVisitor(OperatorPlan plan, PlanWalker walker)
			throws FrontendException {
		super(plan, walker);
		// TODO Auto-generated constructor stub
	}

	@Override
    public void visit(LOLoad load) throws FrontendException {
		
	}

}
