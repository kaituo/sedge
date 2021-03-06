/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.pig.pen.util;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.pig.newplan.DepthFirstWalker;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.PlanVisitor;
import org.apache.pig.newplan.PlanWalker;
import org.apache.pig.pen.InvertConstraintVisitor;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.util.Utils;

import edu.umass.cs.pig.dataflow.LoadArgs;
import edu.umass.cs.pig.dataflow.ParentEncapSol;
import edu.umass.cs.pig.dataflow.ParentEncapsulation;
import edu.umass.cs.pig.vm.ComputeSolution;
import edu.umass.cs.pig.vm.PathList;
import edu.umass.cs.pig.vm.QueryManager;
import edu.umass.cs.pig.z3.Z3Context;

public class PreOrderDepthFirstWalker extends PlanWalker {
  
    private boolean branchFlag = false;

    
    /**
     * @param plan
     *            Plan for this walker to traverse.
     */
    public PreOrderDepthFirstWalker(OperatorPlan plan) {
        super(plan);
    }

    public void setBranchFlag() {
        branchFlag = true;
    }
    
    public boolean getBranchFlag() {
        return branchFlag;
    }
    
    /**
     * Begin traversing the graph.
     * TODO add an extra step at the very beginning of this method:
     * use a breadth-first search to determine how many elements can
     * be there for an operator: if it's an operator without parent operator,
     * assign it one;  if it's a group, assign it two;  if it has parents,
     * inheritance the max of its parents.  The above rules intersect each
     * other.
     * 
     * @param visitor
     *            Visitor this walker is being used by.
     * @throws VisitorException
     *             if an error is encountered while walking.
     * @author kaituo
     * @throws ExecException 
     */
    public void walk(PlanVisitor visitor) throws FrontendException {
        List<Operator> leaves = plan.getSinks();
        Set<Operator> seen = new HashSet<Operator>();
        
        

        depthFirst(null, leaves, seen, visitor);
        
        
        
        //process pathlists that have parent pathlist(s)
        if(visitor instanceof InvertConstraintVisitor) {
        	InvertConstraintVisitor ivisitor = (InvertConstraintVisitor)visitor;
        	Map<PathList, ParentEncapsulation> pMap = ivisitor.parentTupleMap;
//        	Map<PathList, List<ParentEncapsulation>> pMapDuo = ivisitor.parentTupleMapDuo;
        	Map<PathList, ParentEncapSol> pSol = ivisitor.parentSolMap;
        	ComputeSolution cpt = new ComputeSolution();
//        	Set<PathList> parentLists = pMap.keySet();
//        	Map<PathList, LoadArgs> childrenPathlistMap2 = new HashMap<PathList, LoadArgs>();
        	Map<PathList, LoadArgs> childPathMap = ivisitor.childrenPathlistMap;
        	while(childPathMap.size() != 0) {
        		for(Iterator<Map.Entry<PathList, LoadArgs>> it = childPathMap.entrySet().iterator(); it.hasNext(); ) {
        			Map.Entry<PathList, LoadArgs> entry = it.next();
//        		}
//        		for (Entry<PathList, LoadArgs> entry : childPathMap.entrySet()) {
            		PathList cInRel = entry.getKey();
            		LoadArgs larg = entry.getValue();
//            		if(cInRel.parents.size() != 0) {
//                		childrenPathlistMap2.put(cInRel, larg);
//                		continue;
//                	}
//            		if(!parentLists.containsAll(cInRel.getParentPathLists())) {
//            			childrenPathlistMap2.put(cInRel, larg);
//                		continue;
//            		}
            		QueryManager q = larg.getQ();
            		q.getC().resettOut();
            		q.getC().resetNullPos();
            		cInRel.mergeTuls(pMap, cInRel, larg, pSol, ivisitor.getAllSchema(), q.getC().gettOut());//, pMapDuo
                    cpt.compute(cInRel, larg.getExampleTuple(), q, larg.getSchema(), larg.getNewInputData(), 
                    		larg.getInputData(), larg.getAllSchema(), it, childPathMap, pMap, pSol);
                }
        	}
        	
//        	for (Entry<PathList, LoadArgs> entry : childrenPathlistMap2.entrySet()) {
//        		PathList cInRel = entry.getKey();
//        		LoadArgs larg = entry.getValue();
//        		
//        		cInRel.mergeTuls(pMap, cInRel, larg, pSol);
//                cpt.compute(cInRel, larg.getExampleTuple(), larg.getQ(), larg.getSchema(), larg.getNewInputData(), larg.getInputData());
//            }
        }
    }

    public PlanWalker spawnChildWalker(OperatorPlan plan) {
        return new DepthFirstWalker(plan);
    }

    private void depthFirst(Operator node, Collection<Operator> predecessors, Set<Operator> seen,
            PlanVisitor visitor) throws FrontendException {
        if (predecessors == null)
            return;

        boolean thisBranchFlag = branchFlag;
        for (Operator pred : predecessors) {
            if (seen.add(pred)) {
                branchFlag = thisBranchFlag;
                pred.accept(visitor);
                Collection<Operator> newPredecessors = Utils.mergeCollection(plan.getPredecessors(pred), plan.getSoftLinkPredecessors(pred));
                depthFirst(pred, newPredecessors, seen, visitor);
            }
        }
    }
}
