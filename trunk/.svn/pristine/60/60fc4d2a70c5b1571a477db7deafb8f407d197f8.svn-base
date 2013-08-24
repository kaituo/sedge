package edu.umass.cs.pig.dataflow;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.pig.backend.executionengine.ExecException;

import edu.umass.cs.pig.cntr.PltRelCntr;
import edu.umass.cs.pig.util.DeepCopy;
import edu.umass.cs.pig.vm.PathBranch;
import edu.umass.cs.pig.vm.PathList;

public class PathBranchOp {
	JoinPointConnector jpc;
	
	public PathBranchOp() {
		super();
		this.jpc = new JoinPointConnector();
	}

	public void addParentWithDup(PathList outputConstraint, ArrayList<PathList> output) {
		PathBranch p;
		p = new PathBranch(true);
		outputConstraint.getMainConstraints().add(p);
		if (outputConstraint != null)
			output.add(outputConstraint);
	}
	
	public void addParentWithDup2(PathList outputConstraint, ArrayList<PathList> output) {
		PathBranch p;
		p = new PathBranch(true);
		outputConstraint.getMainConstraints().add(p);
		output.add(outputConstraint);
	}
	
	public void addChildrenWithDup(PathList outputConstraint, ArrayList<PathList> output, List<VarInfo> groupCols0, List<VarInfo> groupCols) {
		Object copy = DeepCopy.copy(outputConstraint);
		PathBranch p = new PathBranch(true);
		PathList copyList = (PathList)copy;
		copyList.getMainConstraints().add(p);
		if (copyList != null)
			output.add(copyList);
		jpc.connectJoinInput(outputConstraint, groupCols0,
				copyList, groupCols);
	}
	
	/**
	 * The different between addChildrenWithDup2 and addChildrenWithDup1 is that: addChildrenWithDup2
	 * don't have to deepcopy outputconstraint.
	 * @param parentOutputConstraint
	 * @param output
	 * @param groupCols0
	 * @param groupCols
	 */
	public void addChildrenWithDup2(PathList parentOutputConstraint, 
			ArrayList<PathList> output, List<VarInfo> groupCols0, List<VarInfo> groupCols) {
		PathList outputConstraint = new PathList();
		PathBranch p = new PathBranch(true);
		outputConstraint.getMainConstraints().add(p);
		output.add(outputConstraint);
		jpc.connectJoinInput(parentOutputConstraint, groupCols0,
				outputConstraint, groupCols);
	}
	
	
	
	public void addParentWithoutDup(PathList outputConstraint1, PathList outputConstraint2, 
			ArrayList<PathList> output, Map<VarInfo, PltRelCntr> relMap, List<VarInfo> groupCols0) {
		
		PathBranch p1;
		p1 = new PathBranch(false);
		
		PathBranch p2;
		p2 = new PathBranch(false);
		
		outputConstraint1.getMainConstraints().add(p1);
		outputConstraint2.getMainConstraints().add(p2);
		
		jpc.connectJoinInput(outputConstraint1, groupCols0, outputConstraint2, groupCols0);
		
		jpc.connectPathListWithAggr(outputConstraint1, relMap,
				outputConstraint2);

		if (outputConstraint1 != null)
			output.add(outputConstraint1);
		
		if (outputConstraint2 != null)
			output.add(outputConstraint2);
	}
	
	public void addParentWithoutDup2(PathList outputConstraint1, PathList outputConstraint2, 
			ArrayList<PathList> output, Map<VarInfo, PltRelCntr> relMap,
			List<VarInfo> groupCols0) {
		PathBranch p1;
		p1 = new PathBranch(false);
		
		PathBranch p2;
		p2 = new PathBranch(false);
		
		outputConstraint1.getMainConstraints().add(p1);
		outputConstraint2.getMainConstraints().add(p2);
		
		jpc.connectJoinInput(outputConstraint1, groupCols0, outputConstraint2, groupCols0);
		
		jpc.connectPathListWithAggr(outputConstraint1, relMap,
				outputConstraint2);

		output.add(outputConstraint1);
		
		output.add(outputConstraint2);
	}
	
	public void addChildrenWithoutDup(PathList outputConstraint, PathList outputConstraint2, 
			ArrayList<PathList> output, List<VarInfo> groupCols0, List<VarInfo> groupCols, 
			Map<VarInfo, PltRelCntr> relMap) {
		Object copy = DeepCopy.copy(outputConstraint);
		PathBranch p1 = new PathBranch(false);
		PathList copyList1 = (PathList)copy;
		copyList1.getMainConstraints().add(p1);
		
		Object copy2 = DeepCopy.copy(outputConstraint2);
		PathBranch p2 = new PathBranch(false);
		PathList copyList2 = (PathList)copy2;
		copyList2.getMainConstraints().add(p2);
		
		jpc.connectJoinInput(copyList1, groupCols, copyList2, groupCols);

		jpc.connectPathListWithAggr(copyList1, relMap,
				copyList2);
		
		if (copyList1 != null)
			output.add(copyList1);
		
		if (copyList2 != null)
			output.add(copyList2);
		
		jpc.connectJoinInput(outputConstraint, groupCols0,
				copyList1, groupCols);
		jpc.connectJoinInput(outputConstraint2, groupCols0,
				copyList2, groupCols);
	}
	
	public void addChildrenWithoutDup2(PathList outputConstraint, PathList outputConstraint2,
			ArrayList<PathList> output, List<VarInfo> groupCols0, List<VarInfo> groupCols,
			Map<VarInfo, PltRelCntr> relMap) {
		PathBranch p1 = new PathBranch(false);
		PathList copyList1 = new PathList();
		copyList1.getMainConstraints().add(p1);
		
		PathBranch p2 = new PathBranch(false);
		PathList copyList2 = new PathList();
		copyList2.getMainConstraints().add(p2);
		
		jpc.connectJoinInput(copyList1, groupCols, copyList2, groupCols);
		
		jpc.connectPathListWithAggr(copyList1, relMap,
				copyList2);
		
		if (copyList1 != null)
			output.add(copyList1);
		
		if (copyList2 != null)
			output.add(copyList2);
		
		jpc.connectJoinInput(outputConstraint, groupCols0,
				copyList1, groupCols);
		jpc.connectJoinInput(outputConstraint2, groupCols0,
				copyList2, groupCols);
	}
	

}
