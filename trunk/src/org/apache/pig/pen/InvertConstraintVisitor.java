package org.apache.pig.pen;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;


import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.logical.expression.LogicalExpressionPlan;
import org.apache.pig.newplan.logical.expression.ProjectExpression;
import org.apache.pig.newplan.logical.relational.LOCogroup;
import org.apache.pig.newplan.logical.relational.LODistinct;
import org.apache.pig.newplan.logical.relational.LOFilter;
import org.apache.pig.newplan.logical.relational.LOForEach;
import org.apache.pig.newplan.logical.relational.LOGenerate;
import org.apache.pig.newplan.logical.relational.LOJoin;
import org.apache.pig.newplan.logical.relational.LOLimit;
import org.apache.pig.newplan.logical.relational.LOLoad;
import org.apache.pig.newplan.logical.relational.LOSort;
import org.apache.pig.newplan.logical.relational.LOSplit;
import org.apache.pig.newplan.logical.relational.LOSplitOutput;
import org.apache.pig.newplan.logical.relational.LOStore;
import org.apache.pig.newplan.logical.relational.LOUnion;
import org.apache.pig.newplan.logical.relational.LogicalPlan;
import org.apache.pig.newplan.logical.relational.LogicalRelationalOperator;
import org.apache.pig.newplan.logical.relational.LogicalSchema;
import org.apache.pig.newplan.logical.relational.LogicalSchema.LogicalFieldSchema;


import edu.umass.cs.pig.cntr.CountConstraint;
import edu.umass.cs.pig.cntr.PltRelCntr;
import edu.umass.cs.pig.dataflow.JoinPointConnector;
import edu.umass.cs.pig.dataflow.LoadArgs;
import edu.umass.cs.pig.dataflow.ParentEncapSol;
import edu.umass.cs.pig.dataflow.ParentEncapsulation;
import edu.umass.cs.pig.dataflow.PathBranchOp;
import edu.umass.cs.pig.dataflow.VarInfo;
import edu.umass.cs.pig.util.DeepCopy;
import edu.umass.cs.pig.util.DefaultTuple;
import edu.umass.cs.pig.util.NullAnalyzer;
import edu.umass.cs.pig.vm.ComputeSolution;
import edu.umass.cs.pig.vm.PathBranch;
import edu.umass.cs.pig.vm.PathList;
import edu.umass.cs.pig.vm.QueryManager;
import edu.umass.cs.userfunc.UDFAnalyzer;

/**
 * Solved: 
 * 1. Deal with integer overflow.
 * Problem:
 * For the following Pig programs:
 *      A = load 'tmp.txt'
                + " using PigStorage() as (x : int, y : int);
 
        B = filter A by x * y > 200;;
      
        C = FOREACH B GENERATE (x+1) as x1, (y+1) as y1;
     
        D = FOREACH C GENERATE (x1+1) as x2, (y1+1) as y2;
      
        E = DISTINCT D;
 * Here is the output of our generator:
 *		---------------------------------
		| A     | x:int      | y:int    |
		---------------------------------
		|       | 4          | 8        |
		|       | 2147483646 | 1        |
		---------------------------------
		---------------------------------
		| B     | x:int      | y:int    |
		---------------------------------
		|       | 2147483646 | 1        |
		---------------------------------
		----------------------------------
		| C     | x1:int     | y1:int    |
		----------------------------------
		|       | 2147483647 | 2         |
		----------------------------------
		-----------------------------------
		| D     | x2:int      | y2:int    |
		-----------------------------------
		|       | -2147483648 | 3         |
		-----------------------------------
		-----------------------------------
		| E     | x2:int      | y2:int    |
		-----------------------------------
		|       | -2147483648 | 3         |
		-----------------------------------       
 * Solution:
 * Putting in some `fake' constraints to keep the numbers lower, and relaxing those iteratively.
 * For example: `x < LIMIT && x > -LIMIT', then increase LIMIT if there is no solution. 
 * 2. One motivation to support floating point:
 * For the constraint: x<0.5 && x>0.4
 * The Pig Illustrator would return x=0.5
 * 
 * Note:
 * 1. The reason why I want to include InvertConstraintVisitor in pig's pakcage hierarchy is
 * that if the subclass is in the same package as its parent, it also inherits the 
 * package-private members of the parent.
 * @author kaituo
 *
 */
public class InvertConstraintVisitor extends AugmentBaseDataVisitor implements Serializable {
	
	private static final long serialVersionUID = 1L;
	Map<Operator, ArrayList<PathList>> operatorConstraintsMap = new HashMap<Operator, ArrayList<PathList>>();
	public Map<PathList, LoadArgs> childrenPathlistMap = new HashMap<PathList, LoadArgs>();
	public Map<PathList, ParentEncapsulation> parentTupleMap = new HashMap<PathList, ParentEncapsulation>();
//	public Map<PathList, List<ParentEncapsulation>> parentTupleMapDuo = new HashMap<PathList, List<ParentEncapsulation>>();
	public Map<PathList, ParentEncapSol> parentSolMap = new HashMap<PathList, ParentEncapSol>();
//	private Map<LOLoad, LogicalSchema> loLoadToSchemaMap = new HashMap<LOLoad, LogicalSchema>();
	private boolean containAggr;
	private LOGenerate lg;
	LogicalSchema fschema;
	private List<LogicalSchema> allSchema;
	private boolean isouterjoin;
	public static Map<String, Byte> narrowedType = new HashMap<String, Byte>();
	// deal with the case when join is followed by groupby, redundant records would be produced
//	private boolean isgroupby, isjoin;
	
	public InvertConstraintVisitor(OperatorPlan plan,
            Map<Operator, PhysicalOperator> logToPhysMap,
            Map<LOLoad, DataBag> baseData,
            Map<Operator, DataBag> derivedData,
            List<LogicalSchema> schemas) throws FrontendException {
    	super(plan, logToPhysMap, baseData, derivedData);
    	containAggr = false;
    	lg = null;
    	allSchema = schemas;
    	isouterjoin = false;
//    	isgroupby = false;
//    	isjoin = false;
    }
    
    @Override
    public void visit(LOLoad load) throws FrontendException {
//    	LogicalSchema schema = loLoadToSchemaMap.get(load);// uidOnlySchema;
    	LogicalSchema schema = load.getSchema();
        //uidOnlySchema = load.getUidOnlySchema();
//		if (schema == null)
//		    throw new RuntimeException(
//		            "Example Generator requires a schema. Please provide a schema while loading data");
        
    	QueryManager q = new QueryManager(schema);
        DataBag inputData = baseData.get(load);
        
        

        DataBag newInputData = newBaseData.get(load);
        if (newInputData == null) {
            newInputData = BagFactory.getInstance().newDefaultBag();
            newBaseData.put(load, newInputData);
        }

        
        
//        Tuple exampleTuple = inputData.iterator().next();
        
        ArrayList<PathList> outputConstraints = operatorConstraintsMap.get(load);
        outputConstraintsMap.remove(load);
        
        // first of all, we are required to guarantee that there is at least one
        // output tuple
        if (outputConstraints == null || outputConstraints.size() == 0) {
            // check if the inputData exists
             if (inputData == null || inputData.size() == 0) {
//                 log.error("No (valid) input data found!");
//                 throw new RuntimeException("No (valid) input data found!");
//             	inputData = BagFactory.getInstance().newDefaultBag();
             	DefaultTuple dt = new DefaultTuple(schema, allSchema);
             	Tuple defaultInputT = dt.newDefaultTuple();
//            	inputData.add(defaultInputT);
             	newInputData.add(defaultInputT);
            	
             }
             else {
            	 if(newInputData == null)
            		 for(Tuple exampleTuple: inputData)
            			 newInputData.add(exampleTuple); 
             }
             return;
        } 
        	

        ComputeSolution cpt = new ComputeSolution();
        cpt.compute(outputConstraints, q, schema, 
        		newInputData, inputData, childrenPathlistMap, parentTupleMap, parentSolMap, allSchema);//, parentTupleMapDuo
    }
    
    
    
    @Override
    public void visit(LOFilter filter) throws FrontendException {
        ArrayList<PathList> outputConstraints = operatorConstraintsMap.get(filter);
        operatorConstraintsMap.remove(filter);

        LogicalExpressionPlan filterCond = filter.getFilterPlan();
        Operator input = filter.getInput((LogicalPlan) plan);
        ArrayList<PathList> inputConstraints = operatorConstraintsMap.get(input);
        if (inputConstraints == null) {
            inputConstraints = new ArrayList<PathList>();
            operatorConstraintsMap.put(filter.getInput((LogicalPlan) plan), inputConstraints);
        }

        DataBag outputData = derivedData.get(filter);
        DataBag inputData = derivedData.get(filter.getInput((LogicalPlan) plan));
        //List<PathBranch> branchlist = new ArrayList<PathBranch>();
        
        LogicalSchema schema = filter.getSchema();
        
        try {
        	UDFAnalyzer ualz = new UDFAnalyzer();

        	if (outputConstraints != null && outputConstraints.size() > 0) { // there
                // 's
                // one
                // or
                // more
                // output
                // constraints
                // ;
                // generate
                // corresponding
                // input
                // constraints
        		
                for (Iterator<PathList> it = outputConstraints.iterator(); it
                        .hasNext();) {
                	PathList outputConstraint = it.next();
                	if(outputConstraint != null) {
                		PathBranch p = new PathBranch(filterCond, false);
                        outputConstraint.getMainConstraints().add(p);
                        inputConstraints.add(outputConstraint);
                	}    
                }
            } else if (outputData.size() == 0) { // no output constraints, but
                // output is empty; generate
                // one input that will pass the
                // filter
            	// just add count constraint analysis for
            	// this branch only for pigmix's script 5
            	
            	CountConstraint cc = ualz.containCount(filterCond, false, schema);
            	PathBranch p2;
            	if(cc != null) {
            		p2 = new PathBranch(cc, false);
            	}
            	else {
            		p2 = new PathBranch(filterCond, false);
            	}
//            	outputConstraint2.add(p2);
            	PathList outputConstraint2 = new PathList();
            	outputConstraint2.getMainConstraints().add(p2);
                inputConstraints.add(outputConstraint2);
            }
        	

            // if necessary, insert a negative example (i.e. a tuple that does
            // not pass the filter)
            if (outputData.size() == inputData.size()) { // all tuples pass the
                // filter; generate one
                // input that will not
                // pass the filter

//            	ArrayList<PathBranch> outputConstraint3 = new ArrayList<PathBranch>();
            	
            	CountConstraint cc = ualz.containCount(filterCond, true, schema);
            	PathBranch p3;
            	if(cc != null) {
            		p3 = new PathBranch(cc, true);
            	}
            	else {
            		p3 = new PathBranch(filterCond, true);
            	}
            	
            	PathList outputConstraint3 = new PathList();
//            	PathBranch p3 = new PathBranch(filterCond, true);
            	outputConstraint3.getMainConstraints().add(p3);
                inputConstraints.add(outputConstraint3);

            } else if (input instanceof LogicalRelationalOperator && ualz.contains((LogicalRelationalOperator)input)) {
            	// if there are UDFs in the input operator like foreach,
            	// need to generate one input that will not pass the filter whatever
            	PathList outputConstraint3 = new PathList();
            	PathBranch p3 = new PathBranch(filterCond, true);
            	outputConstraint3.getMainConstraints().add(p3);
                inputConstraints.add(outputConstraint3);
            }
            
        } catch (Exception e) {
            log
                    .error("Error visiting Load during Augmentation phase of Example Generator! "
                            + e.getMessage(), e);
            throw new FrontendException(
                    "Error visiting Load during Augmentation phase of Example Generator! "
                            + e.getMessage(), e);
        }
    }
    
//    LogicalExpressionPlan ConjunctMatchingConstraint(
//    		LogicalExpressionPlan outputConstraint1, LogicalExpressionPlan outputConstraint2, boolean invert) throws Exception {
//    	LogicalExpressionPlan result = new LogicalExpressionPlan();
//        LogicalExpression root1 = (LogicalExpression)outputConstraint1.getSources().get( 0 );
//        LogicalExpression root2 = (LogicalExpression)outputConstraint2.getSources().get( 0 );
//        AndExpression conjunct;
//        NotExpression not2;
//        LogicalExpression newRoot2;
//        if(invert) {
//        	newRoot2 = root2.deepCopy(result);
//        	not2 = new NotExpression(result, newRoot2) ;
//        	conjunct = new AndExpression(result, root1, not2);
//        } else {
//        	conjunct = new AndExpression(result, root1, root2);
//        }
//        
//        result.add( conjunct );
//        return result;
//    	
//    }
    
    @Override
    public void visit(LOForEach forEach) throws FrontendException {
    	LogicalSchema foreachSchema = forEach.getSchema();
    	ArrayList<PathList> outputConstraints = operatorConstraintsMap.get(forEach);
    	operatorConstraintsMap.remove(forEach);
    	UDFAnalyzer ual = new UDFAnalyzer();
    	
        
//        DataBag outputData = derivedData.get(filter);
//        DataBag inputData = derivedData.get(filter.getInput((LogicalPlan) plan));
//		  List<PathBranch> branchlist = new ArrayList<PathBranch>();
        
        LogicalPlan innerPlan = forEach.getInnerPlan();
        Operator opr = innerPlan.getSinks().get(0);
        if(!(opr instanceof LOGenerate))
        	return;
        
        LOGenerate gen = (LOGenerate)opr;
        
        try {
        	if (outputConstraints != null && outputConstraints.size() > 0) { // there
                // 's
                // one
                // or
                // more
                // output
                // constraints
                // ;
                // generate
                // corresponding
                // input
                // constraints;
        		// if there is no output constraints, no need to remember the side effects
        		// in the middle
        		Operator op = plan.getPredecessors(forEach).get(0);
//        		if(op instanceof LOLoad) {
//        			LOLoad ld = (LOLoad)op;
//        			loLoadToSchemaMap.put(ld, forEach.getSchema());
//        		}
        		ArrayList<PathList> inputConstraints = operatorConstraintsMap.get(op);
                if (inputConstraints == null) {
                    inputConstraints = new ArrayList<PathList>();
                    operatorConstraintsMap.put(plan.getPredecessors(forEach).get(0), inputConstraints);
                }
                
                if(ual.contains(gen)) {
                	for (Iterator<PathList> it = outputConstraints
    						.iterator(); it.hasNext();) {
    					PathList outputConstraint = it.next();
    					if (outputConstraint != null) {
    				        // store gen and push it upstream for COGroup's processing
    						if(!containAggr) {
	    						containAggr = true;
	    						lg = gen;
	    						fschema = foreachSchema;
    						}
    						inputConstraints.add(outputConstraint);
    					}

    				}
                } else {
                	for (Iterator<PathList> it = outputConstraints
    						.iterator(); it.hasNext();) {
    					PathList outputConstraint = it.next();
    					if (outputConstraint != null) {
    						PathBranch p = new PathBranch(gen);
    						outputConstraint.getMainConstraints().add(p);
    						inputConstraints.add(outputConstraint);
    					}

    				}
                }
				
            } 
            
        } catch (Exception e) {
            log
                    .error("Error visiting Load during Augmentation phase of Example Generator! "
                            + e.getMessage(), e);
            throw new FrontendException(
                    "Error visiting Load during Augmentation phase of Example Generator! "
                            + e.getMessage(), e);
        }
    	
        

        
    }
    

	@Override
	public void visit(LODistinct dt) throws FrontendException {
		ArrayList<PathList> outputConstraints = operatorConstraintsMap.get(dt);
		operatorConstraintsMap.remove(dt);

		ArrayList<PathList> inputConstraints = operatorConstraintsMap.get(dt
				.getInput((LogicalPlan) plan));
		if (inputConstraints == null) {
			inputConstraints = new ArrayList<PathList>();
			operatorConstraintsMap.put(dt.getInput((LogicalPlan) plan),
					inputConstraints);
		}

		DataBag outputData = derivedData.get(dt);
		DataBag inputData2 = BagFactory.getInstance().newDefaultBag();

		if (outputData != null && outputData.size() > 0) {
			for (Iterator<Tuple> it = outputData.iterator(); it.hasNext();) {
				inputData2.add(it.next());
			}
		}

//		boolean emptyInputConstraints = inputData2.size() == 0;
//		if (emptyInputConstraints) {
//			DataBag inputData = derivedData
//					.get(dt.getInput((LogicalPlan) plan));
//			for (Iterator<Tuple> it = inputData.iterator(); it.hasNext();) {
//				inputData2.add(it.next());
//			}
//		}
		Set<Tuple> distinctSet = new HashSet<Tuple>();
		Iterator<Tuple> it;
		boolean repSeen = false;
		for (it = inputData2.iterator(); it.hasNext();) {
			if (!distinctSet.add(it.next())) {
				repSeen = true;
				break;
			}
		}
		if (!repSeen) {
			// no duplicates found: generate one
//			if (inputData2.size() > 0) {
//				Tuple src = ((ExampleTuple) inputData2.iterator().next())
//						.toTuple(), tgt = TupleFactory.getInstance().newTuple(
//						src.getAll());
//				ExampleTuple inputConstraint = new ExampleTuple(tgt);
//				inputConstraint.synthetic = true;
//				
//				if (outputConstraints != null && outputConstraints.size() > 0) { // there
//					// 's
//					// one
//					// or
//					// more
//					// output
//					// constraints
//					// ;
//					// generate
//					// corresponding
//					// input
//					// constraints
//					for (Iterator<PathList> itp = outputConstraints.iterator(); itp
//							.hasNext();) {
//						PathList outputConstraint = itp.next();
//						if (outputConstraint != null) {
//							outputConstraint.getDuplicateTuples().add(inputConstraint);
//							inputConstraints.add(outputConstraint);
//						}
//					}
//				} else if (outputConstraints == null) { // no output constraints,
//														// but
//					// output is empty; generate
//					// one input that will pass the
//					// filter
//					PathList outputConstraint2 = new PathList();
//					outputConstraint2.getDuplicateTuples().add(inputConstraint);
//					inputConstraints.add(outputConstraint2);
//				}
//			} else if (emptyInputConstraints) {
				if (outputConstraints != null && outputConstraints.size() > 0) { // there
					// 's
					// one
					// or
					// more
					// output
					// constraints
					// ;
					// generate
					// corresponding
					// input
					// constraints
					for (Iterator<PathList> itp = outputConstraints.iterator(); itp
							.hasNext();) {
						PathList outputConstraint = itp.next();
						if (outputConstraint != null) {
							PathBranch p = new PathBranch(true);
							outputConstraint.getMainConstraints().add(p);
							inputConstraints.add(outputConstraint);
						}
					}
				} else  { // if (outputData.size() == 0) no output constraints,
														// but
					// output is empty; generate
					// one input that will pass the
					// filter
					PathList outputConstraint2 = new PathList();
					PathBranch p2 = new PathBranch(true);
					outputConstraint2.getMainConstraints().add(p2);
					inputConstraints.add(outputConstraint2);
				}

			//}
			inputData2.clear();
		}
	}
	
	/**
	 * 1.  Typical cogroup output would be like:
	 *  -------------------------------------------------------------------------------------------------------------------------------------------------------------------
	 *	| C     | group:tuple(x:bytearray,y:bytearray)          | A:bag{:tuple(x:bytearray,y:bytearray)}             | B:bag{:tuple(x:bytearray,y:bytearray)}             | 
	 *	-------------------------------------------------------------------------------------------------------------------------------------------------------------------
	 *	|       | (1, 6)                                        | {(1, 6), (1, 6)}                                   | {(1, 6), (1, 6)}                                   | 
	 *	-------------------------------------------------------------------------------------------------------------------------------------------------------------------
     *  For each nested table from an input such as A:bag, there are two tuples.
     *   Since each PathList represents a tuple to be generated synthetically, and we need to have two records that share the same group by column value,
     *   so we add a PathBranch(isDuplicate=true).  It actually would generate two same records.	
     * 2.  The difference between visitWithAggr and visitWithoutAggr methods is that: visitWithoutAggr uses PathBranch.isDuplicate
     * 		to generate two identical rows in one nested table of a group; but visitWithAggr needs to generate two different rows (
     * 		maybe still same in some cases) and these two rows need to satisfy some constraint between them.
     * 		Graphically,
     * 		visitWithoutAggr is like:
     * 		  pathbranches  --(parent)--> pathbranches
     *      visitWithAggr is like:
     *        pathbranch   --(parent)-->  pathbranch
     *            |                          | 
     *         (parent)(Group by)         (parent)(Group by)
     *         (parent)(Aggregate)        (parent)(Aggregates)
     *           \|/                        \|/ 
     *            |                          | 
     *        pathbranch   --(parent)-->  pathbranch    
     *       To explore the above graph, we'd better use a bread-first search.  Exlore a root node,
     *       then all nodes whose parent is known, etc.
     * 3. 
     *   TODO: 
	 *		1.
	 *       if downstream pass has accumulated one tuple, generate another one;
	 *		 if downstream pass has accumulated none, generate one pathbranch with isDuplicate true;
	 *		 if downstream pass has accumulated two tuple, don't generate another one; need to tell 
	 *		 the children pathlist what to generate;  
	 *		 Right now we depend on upstream pruning (the 4th phase) to prune out redundant records;
	 *		 I assume upstream pruning keeps only two records in each nested bag.
	 *   
	 */
	@Override
    public void visit(LOCogroup cg) throws FrontendException {
//	   isgroupby = true;
       if(containAggr) {
    	   visitWithAggr(cg);
    	   containAggr = false;
       }
       else
    	   visitWithoutAggr(cg);
    }
	
	public void visitWithAggr(LOCogroup cg) throws FrontendException {
        // we first get the outputconstraints for the current cogroup
        ArrayList<PathList> outputConstraints = operatorConstraintsMap.get(cg);
        operatorConstraintsMap.remove(cg);
        boolean ableToHandle = true;
        PathBranchOp pOp = new PathBranchOp();
        LogicalSchema cogroupSchema = cg.getSchema();
        UDFAnalyzer uAnl = new UDFAnalyzer();
        Map<VarInfo, PltRelCntr> relMap = uAnl.analyzeLOGeneratewithAggr(lg, cogroupSchema, fschema);
        
        // we then check if we can handle this cogroup and try to collect some
        // information about grouping
        List<List<VarInfo>> groupSpecs = new LinkedList<List<VarInfo>>();
        int numCols = -1;

        for (int index = 0; index < cg.getInputs((LogicalPlan)plan).size(); ++index) {
            Collection<LogicalExpressionPlan> groupByPlans = 
                cg.getExpressionPlans().get(index);
            List<VarInfo> groupCols = new ArrayList<VarInfo>();
            for (LogicalExpressionPlan plan : groupByPlans) {
                Operator leaf = plan.getSinks().get(0);
                if (leaf instanceof ProjectExpression) {
                	LogicalFieldSchema leafP = ((ProjectExpression) leaf).getFieldSchema();
					VarInfo vinfo = new VarInfo(leafP.alias, leafP.type);
					groupCols
							.add(vinfo);
                } else {
                    ableToHandle = false;
                    break;
                }
            }
            if (numCols == -1) {
                numCols = groupCols.size();
            }
            if (groupCols.size() != groupByPlans.size()
                    || groupCols.size() != numCols) {
                // we came across an unworkable cogroup plan
                break;
            } else {
                groupSpecs.add(groupCols);
            }
        }

        // we should now have some workable data at this point to synthesize
        // tuples
        try {
            if (ableToHandle) {
                // we need to go through the output constraints first
                int numInputs = cg.getInputs((LogicalPlan) plan).size();
//                DataBag outputData = derivedData.get(cg);
                if (outputConstraints != null) {
                    for (Iterator<PathList> it = outputConstraints.iterator(); it
                            .hasNext();) {
                    	PathList outputConstraint = it.next();
                    	
                    	Object copy = DeepCopy.copy(outputConstraint);
                		PathList outputConstraint2 = (PathList)copy;
                		
                    	List<VarInfo> groupCols0 = groupSpecs.get(0);
						ArrayList<PathList> output = operatorConstraintsMap
								.get(cg.getInputs((LogicalPlan) plan).get(0));
						if (output == null) {
							output = new ArrayList<PathList>();
							operatorConstraintsMap.put(
									cg.getInputs((LogicalPlan) plan).get(0),
									output);
						}

						
//						Iterator<Tuple> it_data = outputData.iterator(); 
//						Tuple groupTup;
//						int numTupsToAdd;
//						if(it_data.hasNext()) {
//							groupTup = it_data.next();
//							numTupsToAdd = getTupsToAdd(cg, groupTup, 0);
//						}
//						else
//							throw new FrontendException(
//									"The concrete tuple number is not equal to the symbolic tuple number!");
						
//						if(numTupsToAdd == 0) {
//							p = new PathBranch(false);
//							
//						}
//						else
						pOp.addParentWithoutDup(outputConstraint, outputConstraint2, output, relMap, groupCols0);
						
						

						for (int input = 1; input < numInputs; input++) {

//                          int numInputFields = ((LogicalRelationalOperator) cg.getInputs((LogicalPlan) plan).get(input))
//                                  .getSchema().size();
//                          Object copy = DeepCopy.copy(outputConstraint);
							List<VarInfo> groupCols = groupSpecs.get(input);

							ArrayList<PathList> output2 = operatorConstraintsMap
									.get(cg.getInputs((LogicalPlan) plan)
											.get(input));
							if (output2 == null) {
								output2 = new ArrayList<PathList>();
								operatorConstraintsMap.put(
										cg.getInputs((LogicalPlan) plan).get(
												input), output2);
							}
							pOp.addChildrenWithoutDup(outputConstraint, outputConstraint2, output2, groupCols0, groupCols, relMap);
//							PathBranch p2 = new PathBranch(true);
//							PathList copyList = (PathList)copy;
//							copyList.getMainConstraints().add(p2);
//							ConnectJoinInput(outputConstraint, groupCols0,
//									copyList, groupCols);
//							if (outputConstraint != null)
//								output2.add(copyList);
							
                         
                      }
                    }
                }
                // then, go through all organic data groups and add input
                // constraints to make each group big enough
                else {
					PathList outputConstraint3 = new PathList();
					PathList outputConstraint4 = new PathList();
					List<VarInfo> groupCols0 = groupSpecs.get(0);
					ArrayList<PathList> output = operatorConstraintsMap
							.get(cg.getInputs((LogicalPlan) plan).get(0));
					if (output == null) {
						output = new ArrayList<PathList>();
						operatorConstraintsMap.put(
								cg.getInputs((LogicalPlan) plan).get(0),
								output);
					}

					// if downstream pass has accumulated one tuple, generate another one;
					// if downstream pass has accumulated two tuple, don't generate another one; don't do anything else
					// we have covered the case in InvertConstraintVisitor; so we only need to consider
					// the following case:
					// if downstream pass has accumulated none, generate one pathbranch with isDuplicate true
					// the difference between if and else branch above is that the if branch need to pass constraints up
//					PathBranch p3 = new PathBranch(true);
//					outputConstraint2.getMainConstraints().add(p3);
//					
//					output.add(outputConstraint2);
					pOp.addParentWithoutDup2(outputConstraint3, outputConstraint4, output, relMap, groupCols0);

					for (int input = 1; input < numInputs; input++) {
						

						
						List<VarInfo> groupCols = groupSpecs.get(input);

						ArrayList<PathList> output3 = operatorConstraintsMap
								.get(cg.getInputs((LogicalPlan) plan).get(
										input));
						if (output3 == null) {
							output3 = new ArrayList<PathList>();
							operatorConstraintsMap.put(
									cg.getInputs((LogicalPlan) plan).get(
											input), output3);
						}

//						PathBranch p4 = new PathBranch(true);
//						outputConstraint3.getMainConstraints().add(p4);
//						ConnectJoinInput(outputConstraint2, groupCols0,
//								outputConstraint3, groupCols);

//						output3.add(outputConstraint3);
						pOp.addChildrenWithoutDup2(outputConstraint3, outputConstraint4, output3, groupCols0, groupCols, relMap);
					}
				}
            }
        } catch (Exception e) {
            log
                    .error("Error visiting Cogroup during Augmentation phase of Example Generator! "
                            + e.getMessage());
            throw new FrontendException(
                    "Error visiting Cogroup during Augmentation phase of Example Generator! "
                            + e.getMessage());
        }
    }
	
    public void visitWithoutAggr(LOCogroup cg) throws FrontendException {
        // we first get the outputconstraints for the current cogroup
        ArrayList<PathList> outputConstraints = operatorConstraintsMap.get(cg);
        operatorConstraintsMap.remove(cg);
        boolean ableToHandle = true;
        PathBranchOp pOp = new PathBranchOp();
        
        // we then check if we can handle this cogroup and try to collect some
        // information about grouping
        List<List<VarInfo>> groupSpecs = new LinkedList<List<VarInfo>>();
        int numCols = -1;

        for (int index = 0; index < cg.getInputs((LogicalPlan)plan).size(); ++index) {
            Collection<LogicalExpressionPlan> groupByPlans = 
                cg.getExpressionPlans().get(index);
            List<VarInfo> groupCols = new ArrayList<VarInfo>();
            for (LogicalExpressionPlan plan : groupByPlans) {
                Operator leaf = plan.getSinks().get(0);
                if (leaf instanceof ProjectExpression) {
                	LogicalFieldSchema leafP = ((ProjectExpression) leaf).getFieldSchema();
					VarInfo vinfo = new VarInfo(leafP.alias, leafP.type);
					groupCols
							.add(vinfo);
                } else {
                    ableToHandle = false;
                    break;
                }
            }
            if (numCols == -1) {
                numCols = groupCols.size();
            }
            if (groupCols.size() != groupByPlans.size()
                    || groupCols.size() != numCols) {
                // we came across an unworkable cogroup plan
                break;
            } else {
                groupSpecs.add(groupCols);
            }
        }

        // we should now have some workable data at this point to synthesize
        // tuples
        try {
            if (ableToHandle) {
                // we need to go through the output constraints first
                int numInputs = cg.getInputs((LogicalPlan) plan).size();
//                DataBag outputData = derivedData.get(cg);
                if (outputConstraints != null) {
                    for (Iterator<PathList> it = outputConstraints.iterator(); it
                            .hasNext();) {
                    	PathList outputConstraint = it.next();
                    	List<VarInfo> groupCols0 = groupSpecs.get(0);
						ArrayList<PathList> output = operatorConstraintsMap
								.get(cg.getInputs((LogicalPlan) plan).get(0));
						if (output == null) {
							output = new ArrayList<PathList>();
							operatorConstraintsMap.put(
									cg.getInputs((LogicalPlan) plan).get(0),
									output);
						}

						
//						Iterator<Tuple> it_data = outputData.iterator(); 
//						Tuple groupTup;
//						int numTupsToAdd;
//						if(it_data.hasNext()) {
//							groupTup = it_data.next();
//							numTupsToAdd = getTupsToAdd(cg, groupTup, 0);
//						}
//						else
//							throw new FrontendException(
//									"The concrete tuple number is not equal to the symbolic tuple number!");
						
//						if(numTupsToAdd == 0) {
//							p = new PathBranch(false);
//							
//						}
//						else
						pOp.addParentWithDup(outputConstraint, output);

                        for (int input = 1; input < numInputs; input++) {

//                            int numInputFields = ((LogicalRelationalOperator) cg.getInputs((LogicalPlan) plan).get(input))
//                                    .getSchema().size();
//                            Object copy = DeepCopy.copy(outputConstraint);
							List<VarInfo> groupCols = groupSpecs.get(input);

							ArrayList<PathList> output2 = operatorConstraintsMap
									.get(cg.getInputs((LogicalPlan) plan)
											.get(input));
							if (output2 == null) {
								output2 = new ArrayList<PathList>();
								operatorConstraintsMap.put(
										cg.getInputs((LogicalPlan) plan).get(
												input), output2);
							}
							pOp.addChildrenWithDup(outputConstraint, output2, groupCols0, groupCols);
//							PathBranch p2 = new PathBranch(true);
//							PathList copyList = (PathList)copy;
//							copyList.getMainConstraints().add(p2);
//							ConnectJoinInput(outputConstraint, groupCols0,
//									copyList, groupCols);
//							if (outputConstraint != null)
//								output2.add(copyList);
							
                           
                        }
                    }
                }
                // then, go through all organic data groups and add input
                // constraints to make each group big enough
                else {
					PathList outputConstraint2 = new PathList();
					List<VarInfo> groupCols0 = groupSpecs.get(0);
					ArrayList<PathList> output = operatorConstraintsMap
							.get(cg.getInputs((LogicalPlan) plan).get(0));
					if (output == null) {
						output = new ArrayList<PathList>();
						operatorConstraintsMap.put(
								cg.getInputs((LogicalPlan) plan).get(0),
								output);
					}

					// if downstream pass has accumulated one tuple, generate another one;
					// if downstream pass has accumulated two tuple, don't generate another one; don't do anything else
					// we have covered the case in InvertConstraintVisitor; so we only need to consider
					// the following case:
					// if downstream pass has accumulated none, generate one pathbranch with isDuplicate true
					// the difference between if and else branch above is that the if branch need to pass constraints up
//					PathBranch p3 = new PathBranch(true);
//					outputConstraint2.getMainConstraints().add(p3);
//					
//					output.add(outputConstraint2);
					pOp.addParentWithDup2(outputConstraint2, output);

					for (int input = 1; input < numInputs; input++) {
						

						
						List<VarInfo> groupCols = groupSpecs.get(input);

						ArrayList<PathList> output3 = operatorConstraintsMap
								.get(cg.getInputs((LogicalPlan) plan).get(
										input));
						if (output3 == null) {
							output3 = new ArrayList<PathList>();
							operatorConstraintsMap.put(
									cg.getInputs((LogicalPlan) plan).get(
											input), output3);
						}

//						PathBranch p4 = new PathBranch(true);
//						outputConstraint3.getMainConstraints().add(p4);
//						ConnectJoinInput(outputConstraint2, groupCols0,
//								outputConstraint3, groupCols);

//						output3.add(outputConstraint3);
						pOp.addChildrenWithDup2(outputConstraint2, output3, groupCols0, groupCols);
					}
				}
            }
        } catch (Exception e) {
            log
                    .error("Error visiting Cogroup during Augmentation phase of Example Generator! "
                            + e.getMessage());
            throw new FrontendException(
                    "Error visiting Cogroup during Augmentation phase of Example Generator! "
                            + e.getMessage());
        }
    }
	
//	int getTupsToAdd(LOCogroup cg, Tuple groupTup, int input) throws FrontendException  {
////		int numInputs = cg.getInputs((LogicalPlan) plan).size();
//		// constraints to make each group big enough
////        DataBag outputData = derivedData.get(cg);
//
////        for (Iterator<Tuple> it = outputData.iterator(); it.hasNext();) {
////            Tuple groupTup = it.next();
//
////            for (int input = 0; input < numInputs; input++) {
//                
//            int numTupsToAdd;
//			try {
//				numTupsToAdd = 2
//				        - (int) ((DataBag) groupTup.get(input + 1))
//				                .size();
//			} catch (ExecException e) {
//				log.error("Error visiting Cogroup during Augmentation phase of Example Generator! "
//						+ e.getMessage());
//				throw new FrontendException(
//						"Error visiting Cogroup during Augmentation phase of Example Generator! "
//								+ e.getMessage());
//			}
//            return numTupsToAdd;
////            }
////        }
//	}
	
//	Tuple GetGroupByInput(Object groupLabel, List<VarInfo> groupCols,
//            int numFields) throws ExecException {
//        Tuple t = TupleFactory.getInstance().newTuple(numFields);
//
//        if (groupCols.size() == 1) {
//            // GroupLabel would be a data atom
//            t.set(groupCols.get(0), groupLabel);
//        } else {
//            if (!(groupLabel instanceof Tuple))
//                throw new RuntimeException("Unrecognized group label!");
//            Tuple group = (Tuple) groupLabel;
//            for (int i = 0; i < groupCols.size(); i++) {
//                t.set(groupCols.get(i), group.get(i));
//            }
//        }
//
//        return t;
//    }
    
	
	/**
     * The equi-join of A and B on column 0 can be expressed as follows:

	 *	JOIN A BY $0, B BY $0;
		
	 *	which is equivalent to:
		
	 *	X = COGROUP A BY $0 INNER, B BY $0 INNER;
	 *	FOREACH X GENERATE FLATTEN(A), FLATTEN(B);
	 *
	 * We do two things here:
	 * First, pass the constraints upstream;
	 * Second, remember the parent and child relationships between different path lists. 
     */
	@Override
	public void visit(LOJoin join) throws FrontendException {
//		isjoin = true;
		// we first get the outputconstraints for the current cogroup
		ArrayList<PathList> outputConstraints = operatorConstraintsMap
				.get(join);
		operatorConstraintsMap.remove(join);
		boolean ableToHandle = true;
		// we then check if we can handle this cogroup and try to collect some
		// information about grouping
		List<List<VarInfo>> groupSpecs = new LinkedList<List<VarInfo>>();
		int numCols = -1;

		for (int index = 0; index < join.getInputs((LogicalPlan) plan).size(); ++index) {
			Collection<LogicalExpressionPlan> groupByPlans = join
					.getExpressionPlans().get(index);
			List<VarInfo> groupCols = new ArrayList<VarInfo>();
			for (LogicalExpressionPlan plan : groupByPlans) {
				Operator leaf = plan.getSinks().get(0);
				if (leaf instanceof ProjectExpression) {
					LogicalFieldSchema leafP = ((ProjectExpression) leaf).getFieldSchema();
					VarInfo vinfo = new VarInfo(leafP.alias, leafP.type);
					groupCols
							.add(vinfo);//.alias
				} else {
					ableToHandle = false;
					break;
				}
			}
			if (numCols == -1) {
				numCols = groupCols.size();
			}
			if (groupCols.size() != groupByPlans.size()
					|| groupCols.size() != numCols) {
				// we came across an unworkable cogroup plan
				break;
			} else {
				groupSpecs.add(groupCols);
			}
		}

		// we should now have some workable data at this point to synthesize
		// tuples
		try {
			if (ableToHandle) {
				// we need to go through the output constraints first
				int numInputs = join.getInputs((LogicalPlan) plan).size();
				DataBag outputData = derivedData.get(join);
				if (outputConstraints != null) {
					for (Iterator<PathList> it = outputConstraints.iterator(); it
							.hasNext();) {
						PathList outputConstraint = it.next();
						List<VarInfo> groupCols0 = groupSpecs.get(0);
						ArrayList<PathList> output = operatorConstraintsMap
								.get(join.getInputs((LogicalPlan) plan).get(0));
						if (output == null) {
							output = new ArrayList<PathList>();
							operatorConstraintsMap.put(
									join.getInputs((LogicalPlan) plan).get(0),
									output);
						}

						if (outputConstraint != null)
							output.add(outputConstraint);

						for (int input = 1; input < numInputs; input++) {

							Object copy = DeepCopy.copy(outputConstraint);
							PathList pOutCtr = (PathList) copy;
							
//							List<Relation> childrenPL = outputConstraint.children;
//							pOutCtr.children = childrenPL;
//							for(Relation pl : childrenPL) {
//								Relation plnew = (Relation)DeepCopy.copy(pl);
//								plnew.parent = pOutCtr;
//								plnew.child = pl.child;
//								pl.child.parents.add(plnew);
//							}
							
						    List<VarInfo> groupCols = groupSpecs.get(input);

							ArrayList<PathList> output2 = operatorConstraintsMap
									.get(join.getInputs((LogicalPlan) plan)
											.get(input));
							if (output2 == null) {
								output2 = new ArrayList<PathList>();
								operatorConstraintsMap.put(
										join.getInputs((LogicalPlan) plan).get(
												input), output2);
							}

							JoinPointConnector pOp = new JoinPointConnector();
							pOp.connectJoinInput(outputConstraint, groupCols0,
									pOutCtr, groupCols);
							if (outputConstraint != null)
								output2.add(pOutCtr);
						}
					}
				} 
				else if (outputData.size() == 0 ) {
					PathList outputConstraint2 = new PathList();
					List<VarInfo> groupCols0 = groupSpecs.get(0);
					ArrayList<PathList> output = operatorConstraintsMap
							.get(join.getInputs((LogicalPlan) plan).get(0));
					if (output == null) {
						output = new ArrayList<PathList>();
						operatorConstraintsMap.put(
								join.getInputs((LogicalPlan) plan).get(0),
								output);
					}

					output.add(outputConstraint2);

					for (int input = 1; input < numInputs; input++) {

						PathList outputConstraint3 = new PathList();
						List<VarInfo> groupCols = groupSpecs.get(input);

						ArrayList<PathList> output3 = operatorConstraintsMap
								.get(join.getInputs((LogicalPlan) plan).get(
										input));
						if (output3 == null) {
							output3 = new ArrayList<PathList>();
							operatorConstraintsMap.put(
									join.getInputs((LogicalPlan) plan).get(
											input), output3);
						}

						JoinPointConnector pOp = new JoinPointConnector();
						pOp.connectJoinInput(outputConstraint2, groupCols0,
								outputConstraint3, groupCols);

						output3.add(outputConstraint3);
					}
				} else if (NullAnalyzer.containsNull(outputData)) {
					isouterjoin = true;
					PathList outputConstraint2 = new PathList(true);
					List<VarInfo> groupCols0 = groupSpecs.get(0);
					ArrayList<PathList> output = operatorConstraintsMap
							.get(join.getInputs((LogicalPlan) plan).get(0));
					if (output == null) {
						output = new ArrayList<PathList>();
						operatorConstraintsMap.put(
								join.getInputs((LogicalPlan) plan).get(0),
								output);
					}

					output.add(outputConstraint2);

					for (int input = 1; input < numInputs; input++) {

						PathList outputConstraint3 = new PathList();
						List<VarInfo> groupCols = groupSpecs.get(input);

						ArrayList<PathList> output3 = operatorConstraintsMap
								.get(join.getInputs((LogicalPlan) plan).get(
										input));
						if (output3 == null) {
							output3 = new ArrayList<PathList>();
							operatorConstraintsMap.put(
									join.getInputs((LogicalPlan) plan).get(
											input), output3);
						}

						JoinPointConnector pOp = new JoinPointConnector();
						pOp.connectJoinInput(outputConstraint2, groupCols0,
								outputConstraint3, groupCols);

						output3.add(outputConstraint3);
					}
				}

			}
		} catch (Exception e) {
			log.error("Error visiting Cogroup during Augmentation phase of Example Generator! "
					+ e.getMessage());
			throw new FrontendException(
					"Error visiting Cogroup during Augmentation phase of Example Generator! "
							+ e.getMessage());
		}
	}
    
    @Override
    public void visit(LOStore store) throws FrontendException {
        ArrayList<PathList> outputConstraints = operatorConstraintsMap.get(store);
//        if (outputConstraints == null) {
//        	ArrayList<PathList> inputConstraints = new ArrayList<PathList>();
//            operatorConstraintsMap.put(plan.getPredecessors(store)
//                    .get(0), inputConstraints);
//            
//        } else {
    	operatorConstraintsMap.remove(store);
    	operatorConstraintsMap.put(plan.getPredecessors(store)
                .get(0), outputConstraints);
//        }
    }
    
    @Override
    public void visit(LOUnion u) throws FrontendException {
        ArrayList<PathList> outputConstraints = operatorConstraintsMap.get(u);
        outputConstraintsMap.remove(u);
        if (outputConstraints == null || outputConstraints.size() == 0) {
            return;
        }

        // since we have some outputConstraints, we apply them to the inputs
        // round-robin
        int count = 0;
        List<Operator> inputs = u.getInputs(((LogicalPlan) plan));
        int noInputs = inputs.size();

        for (Operator op : inputs) {
        	ArrayList<PathList> constraint = new ArrayList<PathList>();
        	operatorConstraintsMap.put(op, constraint);
        }
        for (Iterator<PathList> it = outputConstraints.iterator(); it.hasNext();) {
        	ArrayList<PathList> constraint = operatorConstraintsMap.get(inputs.get(count));
            constraint.add(it.next());
            count = (count + 1) % noInputs;
        }

    }
    
    @Override
    public void visit(LOLimit lm) throws FrontendException {
    }
    
    @Override
    public void visit(LOSplit split) throws FrontendException {
    	ArrayList<PathList> outputConstraints = operatorConstraintsMap.get(split);
//      if (outputConstraints == null) {
//      	ArrayList<PathList> inputConstraints = new ArrayList<PathList>();
//          operatorConstraintsMap.put(plan.getPredecessors(store)
//                  .get(0), inputConstraints);
//          
//      } else {
	  	operatorConstraintsMap.remove(split);
	  	operatorConstraintsMap.put(plan.getPredecessors(split)
                .get(0), outputConstraints);
    }
    
    @Override
    public void visit(LOSplitOutput splitout) throws FrontendException {
    	ArrayList<PathList> outputConstraints = operatorConstraintsMap.get(splitout);
        operatorConstraintsMap.remove(splitout);

        LogicalExpressionPlan filterCond = splitout.getFilterPlan();
        Operator input = splitout.getInput((LogicalPlan) plan);
        ArrayList<PathList> inputConstraints = operatorConstraintsMap.get(input);
        if (inputConstraints == null) {
            inputConstraints = new ArrayList<PathList>();
            operatorConstraintsMap.put(splitout.getInput((LogicalPlan) plan), inputConstraints);
        }

        DataBag outputData = derivedData.get(splitout);
        DataBag inputData = derivedData.get(splitout.getInput((LogicalPlan) plan));
        //List<PathBranch> branchlist = new ArrayList<PathBranch>();
        try {
        	if (outputConstraints != null && outputConstraints.size() > 0) { // there
                // 's
                // one
                // or
                // more
                // output
                // constraints
                // ;
                // generate
                // corresponding
                // input
                // constraints
                for (Iterator<PathList> it = outputConstraints.iterator(); it
                        .hasNext();) {
                	PathList outputConstraint = it.next();
                	if(outputConstraint != null) {
                		PathBranch p = new PathBranch(filterCond, false);
                        outputConstraint.getMainConstraints().add(p);
                        inputConstraints.add(outputConstraint);
                	}    
                }
            } else if (outputData.size() == 0) { // no output constraints, but
                // output is empty; generate
                // one input that will pass the
                // filter
//            	ArrayList<PathBranch> outputConstraint2 = new ArrayList<PathBranch>();
            	PathBranch p2 = new PathBranch(filterCond, false);
//            	outputConstraint2.add(p2);
            	PathList outputConstraint2 = new PathList();
            	outputConstraint2.getMainConstraints().add(p2);
                inputConstraints.add(outputConstraint2);
            }
            
            // Since the input is LOSplit which doesn't contain any data, so we
            // have to generate one input that will not pass the filter whatever
            PathList outputConstraint3 = new PathList();
        	PathBranch p3 = new PathBranch(filterCond, true);
        	outputConstraint3.getMainConstraints().add(p3);
            inputConstraints.add(outputConstraint3);
            
        } catch (Exception e) {
            log
                    .error("Error visiting Load during Augmentation phase of Example Generator! "
                            + e.getMessage(), e);
            throw new FrontendException(
                    "Error visiting Load during Augmentation phase of Example Generator! "
                            + e.getMessage(), e);
        }
    }
    
    @Override
    public void visit(LOSort s) throws FrontendException {
        ArrayList<PathList> outputConstraints = operatorConstraintsMap.get(s);
        operatorConstraintsMap.remove(s);

        if (outputConstraints == null)
        	operatorConstraintsMap.put(s.getInput((LogicalPlan) plan), new ArrayList<PathList>());
        else
        	operatorConstraintsMap.put(s.getInput((LogicalPlan) plan), outputConstraints);
    }

	public List<LogicalSchema> getAllSchema() {
		return allSchema;
	}

	public void setAllSchema(List<LogicalSchema> allSchema) {
		this.allSchema = allSchema;
	}

	public boolean isIsouterjoin() {
		return isouterjoin;
	}

	public void setIsouterjoin(boolean isouterjoin) {
		this.isouterjoin = isouterjoin;
	}

//	public boolean isIsgroupby() {
//		return isgroupby;
//	}
//
//	public void setIsgroupby(boolean isgroupby) {
//		this.isgroupby = isgroupby;
//	}
//
//	public boolean isIsjoin() {
//		return isjoin;
//	}
//
//	public void setIsjoin(boolean isjoin) {
//		this.isjoin = isjoin;
//	}
    
}
