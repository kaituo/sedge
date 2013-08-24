package edu.umass.cs.userfunc;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.pig.FuncSpec;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.logical.expression.DereferenceExpression;
import org.apache.pig.newplan.logical.expression.LogicalExpressionPlan;
import org.apache.pig.newplan.logical.expression.ProjectExpression;
import org.apache.pig.newplan.logical.expression.UserFuncExpression;
import org.apache.pig.newplan.logical.relational.LOForEach;
import org.apache.pig.newplan.logical.relational.LOGenerate;
import org.apache.pig.newplan.logical.relational.LogicalPlan;
import org.apache.pig.newplan.logical.relational.LogicalRelationalOperator;
import org.apache.pig.newplan.logical.relational.LogicalSchema;
import org.apache.pig.newplan.logical.relational.LogicalSchema.LogicalFieldSchema;
import org.apache.pig.pen.InvertConstraintVisitor;

import edu.umass.cs.pig.cntr.CountConstraint;
import edu.umass.cs.pig.cntr.CountConstraintCmpr;
import edu.umass.cs.pig.cntr.PltRelCntr;
import edu.umass.cs.pig.cntr.SumConstraint;
import edu.umass.cs.pig.dataflow.VarInfo;


public class UDFAnalyzer {
	
	/*
	 * Test if a logical operator contains UDF.
	 */
	public boolean contains(LogicalRelationalOperator lo) {
        if ((lo == null)) {
            //log.debug("obj1 or obj2 are null.");
            return false;
        }

        if (lo instanceof LOGenerate) {
           return containUDF((LOGenerate)lo);
        } else if (lo instanceof LOForEach) {
        	LogicalPlan innerPlan = ((LOForEach)lo).getInnerPlan();
            Operator opr = innerPlan.getSinks().get(0);
            if(!(opr instanceof LOGenerate))
            	return false;
            
            LOGenerate gen = (LOGenerate)opr;
            return containUDF(gen);
        }

        //log.debug("obj1 does not contain obj2: " + obj1 + ", " + obj2);
        return false;
    }
	
	public CountConstraint containCount(LogicalExpressionPlan filterPlan,
			boolean invert, LogicalSchema filterSchema) throws FrontendException {
		Iterator<Operator> it = filterPlan.getOperators();
		CountConstraint ret = null;
		while (it.hasNext()) {
			Operator op = it.next();
			if (op instanceof UserFuncExpression) {
				UserFuncExpression udfCount = ((UserFuncExpression) op);
				FuncSpec fs = udfCount.getFuncSpec();
				String className = fs.getClassName();

				if (className.equals("org.apache.pig.builtin.COUNT")) {
					ret = new CountConstraint();
					List<Operator> op2 = filterPlan.getSuccessors(udfCount);
					//Precondition: op2 should be an instance of ProjectExpression
					ProjectExpression prj = (ProjectExpression) (op2.get(0));
					String bagAlias = prj.getFieldSchema().alias;
					LogicalFieldSchema lfieldSchema = filterSchema.getFieldSubNameMatch(bagAlias);
					LogicalSchema bagSchema = null;
					
					if (lfieldSchema.type == DataType.BAG) {
						bagSchema = lfieldSchema.schema;
						int size = bagSchema.size();
						for(int i=0; i<size; i++) {
							LogicalFieldSchema f = bagSchema.getField(0);
							LogicalSchema fschema = f.schema;
							LogicalFieldSchema fschemaschema = fschema.getField(0);
							ret.addCountFields(fschemaschema.alias);
						}
					}
					
					LogicalExpressionPlan countPlan = (LogicalExpressionPlan) udfCount
							.getPlan();
					
					CountConstraintCmpr ccc = new CountConstraintCmpr();
					ccc.GenerateMatchingTuple(countPlan, invert, ret);
					
				}
			}
		}
		return ret;
	}
	
	public boolean containUDF(LOGenerate lgen) {
		List<LogicalExpressionPlan> gens = lgen.getOutputPlans();

		for (Iterator<LogicalExpressionPlan> it = gens.iterator(); it.hasNext();) {
			LogicalExpressionPlan genExp = it.next();
			Operator op = genExp.getSources().get(0);
			if (op instanceof UserFuncExpression) {
				return true;
			}
		}

		// log.debug("obj1 does not contain obj2: " + obj1 + ", " + obj2);
		return false;
    }
	
	private boolean containUDF(LogicalExpressionPlan filterPlan) {
        Iterator<Operator> it = filterPlan.getOperators();
        while( it.hasNext() ) {
            if( it.next() instanceof UserFuncExpression )
                return true;
        }
        return false;
    }
	
	/**
	 * 1. An example of udf logical plan structure:
	 * Pig Script:
	 *    pigServer.registerQuery("A = load " + F.toString() + " using PigStorage() as (name: chararray, x : int, y : int);");
	      pigServer.registerQuery("B = group A by name;");
	      pigServer.registerQuery("C = foreach B generate group, SUM(A.x) as xx, SUM(A.y) as yy;");
	      pigServer.registerQuery("D = filter C by xx > 100 and yy > 100;");
	      pigServer.registerQuery("store D into '" +  out.getAbsolutePath() + "';");
	   
	   Logical Plan:
	 * #-----------------------------------------------
		# New Logical Plan:
		#-----------------------------------------------
		1-1: (Name: LOStore Schema: group#49:chararray,xx#56:long,yy#59:long)
		|
		|---D: (Name: LOFilter Schema: group#49:chararray,xx#56:long,yy#59:long)
		    |   |
		    |   (Name: And Type: boolean Uid: 64)
		    |   |
		    |   |---(Name: GreaterThan Type: boolean Uid: 61)
		    |   |   |
		    |   |   |---xx:(Name: Project Type: long Uid: 56 Input: 0 Column: 1)
		    |   |   |
		    |   |   |---(Name: Cast Type: long Uid: 60)
		    |   |       |
		    |   |       |---(Name: Constant Type: int Uid: 60)
		    |   |
		    |   |---(Name: GreaterThan Type: boolean Uid: 63)
		    |       |
		    |       |---yy:(Name: Project Type: long Uid: 59 Input: 0 Column: 2)
		    |       |
		    |       |---(Name: Cast Type: long Uid: 62)
		    |           |
		    |           |---(Name: Constant Type: int Uid: 62)
		    |
		    |---C: (Name: LOForEach Schema: group#49:chararray,xx#56:long,yy#59:long)
		        |   |
		        |   (Name: LOGenerate[false,false,false] Schema: group#49:chararray,xx#56:long,yy#59:long)
		        |   |   |
		        |   |   group:(Name: Project Type: chararray Uid: 49 Input: 0 Column: (*))
		        |   |   |
		        |   |   (Name: UserFunc(org.apache.pig.builtin.LongSum) Type: long Uid: 56)
		        |   |   |
		        |   |   |---(Name: Dereference Type: bag Uid: 55 Column:[1])
		        |   |       |
		        |   |       |---A:(Name: Project Type: bag Uid: 52 Input: 1 Column: (*))
		        |   |   |
		        |   |   (Name: UserFunc(org.apache.pig.builtin.LongSum) Type: long Uid: 59)
		        |   |   |
		        |   |   |---(Name: Dereference Type: bag Uid: 58 Column:[2])
		        |   |       |
		        |   |       |---A:(Name: Project Type: bag Uid: 52 Input: 2 Column: (*))
		        |   |
		        |   |---(Name: LOInnerLoad[0] Schema: group#49:chararray)
		        |   |
		        |   |---A: (Name: LOInnerLoad[1] Schema: name#49:chararray,x#50:int,y#51:int)
		        |   |
		        |   |---A: (Name: LOInnerLoad[1] Schema: name#49:chararray,x#50:int,y#51:int)
		        |
		        |---B: (Name: LOCogroup Schema: group#49:chararray,A#52:bag{#66:tuple(name#49:chararray,x#50:int,y#51:int)})
		            |   |
		            |   name:(Name: Project Type: chararray Uid: 49 Input: 0 Column: 0)
		            |
		            |---A: (Name: LOLoad Schema: name#49:chararray,x#50:int,y#51:int)RequiredFields:null


        2. 
        	LogicalSchema cogroupSchema = = cogroup.getSchema();
        3. 
        	LogicalRelationalOperator fe = 
            (LogicalRelationalOperator) newLogicalPlan.getSuccessors(load).get(0);
	        assertEquals( LOForEach.class, fe.getClass() );
	        LOForEach forEach = (LOForEach)fe;
	        
	        LogicalPlan innerPlan = 
	            forEach.getInnerPlan();
	        assertEquals( 1, innerPlan.getSinks().size() );        
	        assertEquals( LOGenerate.class, innerPlan.getSinks().get(0).getClass() );
	        LOGenerate gen = (LOGenerate)innerPlan.getSinks().get(0);
	        assertEquals( 2, gen.getOutputPlans().size() );
        
	 * @param lgen
	 * @see org.apache.pig.test.TestNewPlanLogToPhyTranslationVisitor#testCogroupSchema1()
	 * @see org.apache.pig.test.TestNewPlanLogToPhyTranslationVisitor#testCogroupSchema2()
	 * @see org.apache.pig.test.TestNewPlanLogToPhyTranslationVisitor#testPlanwithUserFunc()
	 * @see org.apache.pig.test.TestNewPlanLogToPhyTranslationVisitor#testPlanwithUserFunc2()
	 * @return
	 */
	public Map<VarInfo, PltRelCntr> analyzeLOGeneratewithAggr(LOGenerate gen,
			LogicalSchema cogroupSchema, LogicalSchema fSchema) throws FrontendException {
		List<LogicalExpressionPlan> outputPlans = gen.getOutputPlans();
		Map<VarInfo, PltRelCntr> relMap = new HashMap<VarInfo, PltRelCntr>(); 
		for (int i = 0; i < outputPlans.size(); i++) {
			LogicalExpressionPlan genExp = gen.getOutputPlans().get(i);
			if (genExp.getSources().size() != 1)
				throw new FrontendException(
						"The size of LOGenerate's 1st output plan should be 1.");
			Operator op = genExp.getSources().get(0);
			// if(op instanceof ProjectExpression) {
			// ProjectExpression prj1 = (ProjectExpression)op;
			// long uid1 = prj1.getFieldSchema().uid;
			// continue;
			// } else
			if (op instanceof UserFuncExpression) {
				Operator op2 = genExp.getSinks().get(0);
				//Precondition: op2 should be an instance of ProjectExpression
				ProjectExpression prj = (ProjectExpression) op2;
				String bagAlias = prj.getFieldSchema().alias;
				DereferenceExpression deref = null;
				int column = -1;
				Operator predecessorprj = genExp.getPredecessors(prj).get(0);
				if (predecessorprj.getClass().equals(
						DereferenceExpression.class)) {
					deref = (DereferenceExpression) predecessorprj;
					column = (int) deref.getBagColumns().get(0);
				}
				LogicalFieldSchema lfieldSchema = cogroupSchema.getFieldSubNameMatch(bagAlias);
				LogicalSchema bagSchema = null;
				String alias = null;
				LogicalFieldSchema sum = null;
				String sumAlias = null;
				byte tp = -1;
				byte sumTp = -1;
				LogicalSchema tupleSchema = null;
				if (lfieldSchema.type == DataType.BAG) {
					bagSchema = lfieldSchema.schema;
					// find the alias of input parameters of udf. Now only works
					// for one parameter.
					if (deref != null && column != -1) {
						bagSchema = lfieldSchema.schema;
						tupleSchema = bagSchema.getField(0).schema;
						LogicalFieldSchema arg = tupleSchema.getField(column);
						alias = arg.alias; // this is a
																	// field
																// alias
						tp = arg.type;
						
						sum = ((UserFuncExpression) op).getFieldSchema();
						
						int sumCol = fSchema.findField(sum.uid);
						LogicalFieldSchema sumSchema = fSchema.getField(sumCol);
						sumAlias = sumSchema.alias;
						sumTp = sum.type;
						FuncSpec fs = ((UserFuncExpression) op).getFuncSpec();
						String className = fs.getClassName();
						List<VarInfo> parentCols = new ArrayList<VarInfo>();
						List<VarInfo> childCols = new ArrayList<VarInfo>();
						List<VarInfo> aggrCols = new ArrayList<VarInfo>();
						if (className.equals("org.apache.pig.builtin.LongSum")) {
//							PathList parent = new PathList();
//							PathList child = new PathList();
//							PltAggrRelation plr = new PltAggrRelation(parent, child);
							VarInfo pvinfo = new VarInfo(alias, tp);
							parentCols.add(pvinfo);
							VarInfo cvinfo = new VarInfo(alias, tp);
							childCols.add(cvinfo);
							VarInfo avinfo = new VarInfo(sumAlias, sumTp);
							aggrCols.add(avinfo);
							PltRelCntr aggr = new SumConstraint(parentCols, childCols, aggrCols, tp, sumTp);
							relMap.put(pvinfo, aggr);
							
							if(tp != sumTp)
								InvertConstraintVisitor.narrowedType.put(sumAlias, tp);
						}
					}
					// Don't do anything for the else branch.
					// else {
					// alias = lfieldSchema.alias;
					// }
					
				}

			} 
		}
		return relMap;

	}
	
	
	/**
	 * 
	 * @param gen
	 * @param outputConstraints
	 * @return
	 * @throws FrontendException
	 * @see org.apache.pig.test.TestNewPlanLogToPhyTranslationVisitor#testCogroupSchema1()
	 * @see org.apache.pig.test.TestNewPlanLogToPhyTranslationVisitor#testCogroupSchema2()
	 * @see org.apache.pig.test.TestNewPlanLogToPhyTranslationVisitor#testPlanwithUserFunc()
	 * @see org.apache.pig.test.TestNewPlanLogToPhyTranslationVisitor#testPlanwithUserFunc2()
	 */
//	public List<PltAggrRelation> analyzeLOGeneratewithAggr2(LOGenerate gen, ArrayList<PathList> outputConstraints) throws FrontendException{
//		List<LogicalExpressionPlan> outputPlans = gen.getOutputPlans();
//		for(int i=0; i<outputPlans.size(); i++) {
//			LogicalExpressionPlan genExp = gen.getOutputPlans().get(i);
//			if(genExp.getSources().size() != 1)
//				throw new FrontendException(
//	                    "The size of LOGenerate's 1st output plan should be 1.");
//			Operator op = genExp.getSources().get(0);
//            if(op instanceof ProjectExpression) {
//            	continue;
////            	ProjectExpression prj1  = (ProjectExpression)op;
////            	long uid1 = prj1.getFieldSchema().uid;
//            } else if (op instanceof UserFuncExpression) {
//            	Operator op2 = genExp.getSinks().get(0);
//            	ProjectExpression prj2 = (ProjectExpression)op2;
//            	long uid2 = prj2.getFieldSchema().uid;
//            	DereferenceExpression def = (DereferenceExpression)genExp.getPredecessors(prj2).get(0);
//            	Integer col = def.getBagColumns().get(0);
//            	FuncSpec fs = ((UserFuncExpression)op).getFuncSpec();
//            	String className = fs.getClassName();
//            	if(className.equals("org.apache.pig.builtin.LongSum")) {
//            		
//            	}
//            	
//            } else
//            	throw new FrontendException(
//            			"Unsupported operator inside LOGenerate.");
//        }
//		
//		
//		
//	}

}
