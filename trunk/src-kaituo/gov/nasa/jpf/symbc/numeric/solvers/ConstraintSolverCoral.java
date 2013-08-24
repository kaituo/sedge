package gov.nasa.jpf.symbc.numeric.solvers;

import static edu.umass.cs.pig.util.Assertions.check;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import nl.flotsam.xeger.Xeger;

import org.apache.commons.math3.fraction.Fraction;
import org.apache.pig.FuncSpec;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.logical.expression.AddExpression;
import org.apache.pig.newplan.logical.expression.AndExpression;
import org.apache.pig.newplan.logical.expression.BinaryExpression;
import org.apache.pig.newplan.logical.expression.CastExpression;
import org.apache.pig.newplan.logical.expression.ConstantExpression;
import org.apache.pig.newplan.logical.expression.DivideExpression;
import org.apache.pig.newplan.logical.expression.EqualExpression;
import org.apache.pig.newplan.logical.expression.GreaterThanEqualExpression;
import org.apache.pig.newplan.logical.expression.GreaterThanExpression;
import org.apache.pig.newplan.logical.expression.IsNullExpression;
import org.apache.pig.newplan.logical.expression.LessThanEqualExpression;
import org.apache.pig.newplan.logical.expression.LessThanExpression;
import org.apache.pig.newplan.logical.expression.LogicalExpression;
import org.apache.pig.newplan.logical.expression.LogicalExpressionPlan;
import org.apache.pig.newplan.logical.expression.ModExpression;
import org.apache.pig.newplan.logical.expression.MultiplyExpression;
import org.apache.pig.newplan.logical.expression.NotEqualExpression;
import org.apache.pig.newplan.logical.expression.NotExpression;
import org.apache.pig.newplan.logical.expression.OrExpression;
import org.apache.pig.newplan.logical.expression.ProjectExpression;
import org.apache.pig.newplan.logical.expression.RegexExpression;
import org.apache.pig.newplan.logical.expression.SubtractExpression;
import org.apache.pig.newplan.logical.expression.UserFuncExpression;
import org.apache.pig.newplan.logical.relational.LOGenerate;
import org.apache.pig.newplan.logical.relational.LogicalSchema;
import org.apache.pig.pen.InvertConstraintVisitor;
import org.apache.pig.pen.util.ExampleTuple;

import edu.umass.cs.pig.ast.ConstraintResult;
import edu.umass.cs.pig.cntr.EqualConstraint;
import edu.umass.cs.pig.cntr.FakeLimit;
import edu.umass.cs.pig.cntr.OverflowUnderFlow;
import edu.umass.cs.pig.cntr.PltRelCntr;
import edu.umass.cs.pig.cntr.SyncNameDB;
import edu.umass.cs.pig.dataflow.ValueExtractor;
import edu.umass.cs.pig.dataflow.VarInfo;
import edu.umass.cs.pig.numeric.CAndCCoral;
import edu.umass.cs.pig.numeric.SymbolicVar;
import edu.umass.cs.pig.sort.BitVector32Sort;
import edu.umass.cs.pig.sort.BitVector64Sort;
import edu.umass.cs.pig.sort.Fp32Sort;
import edu.umass.cs.pig.sort.Fp64Sort;
import edu.umass.cs.pig.util.Allocator4Z3;
import edu.umass.cs.pig.util.DataByteArrayConverter;
import edu.umass.cs.pig.vm.CntrTruth;
import edu.umass.cs.pig.z3.Z3Context;
import edu.umass.cs.userfunc.InputOutput;
import edu.umass.cs.userfunc.UninterpretedMaker;
import edu.umass.cs.userfunc.UserFuncValue;
import edu.umass.cs.z3.SWIGTYPE_p__Z3_ast;
import edu.umass.cs.z3.SWIGTYPE_p__Z3_sort;
import edu.umass.cs.z3.SWIGTYPE_p__Z3_symbol;

import gov.nasa.jpf.symbc.numeric.MinMax;
import gov.nasa.jpf.symbc.numeric.SymbolicInteger;
import gov.nasa.jpf.symbc.numeric.SymbolicReal;

/**
 * TODO: implicit type conversion from int to double when
 * I'm doing multiplication and division of floats and ints 
 * and I forget the implicit conversion rules (and the words
 *  in the question seem too vague to google more quickly than asking here).
 * @author kaituo
 *
 */
public class ConstraintSolverCoral {
	LogicalSchema schema;
//	List<LogicalFieldSchema> schema;
	ExampleTuple tOut;
	List<Integer> nullPos;
//	Collection<Object> userFuncVal;
	public static ArrayList<String> nameDeclared = new ArrayList<String>();
    Set<Object> seen = new HashSet<Object>();
//   public boolean success = false;
    Tuple defaultTuple;
//    Allocator4Z3 alloc;
    ProblemCoral alloc;
    ValueExtractor extr;
    OverflowUnderFlow flow;
    SyncNameDB sync;
//    DataByteArrayConverter cvtr;
    CAndCCoral cccoral;
    

	public ConstraintSolverCoral(LogicalSchema schema) {
		super();
		this.schema = schema;
		defaultTuple = TupleFactory.getInstance()
				.newTuple(schema.getFields().size()); 
		//tOut = new ExampleTuple(defaultTuple);
//		userFuncVal = null;
//		alloc = new Allocator4Z3();
		alloc = ProblemCoral.get();
		extr = new ValueExtractor();
		flow = new OverflowUnderFlow();
		nullPos = new ArrayList<Integer>();
		sync = new SyncNameDB();
//		cvtr = new DataByteArrayConverter();
		cccoral = new CAndCCoral(alloc);
	}
	
	public ConstraintSolverCoral() {
//		userFuncVal = null;
//		alloc = new Allocator4Z3();
		alloc = ProblemCoral.get();
		extr = new ValueExtractor();
		nullPos = new ArrayList<Integer>();
		sync = new SyncNameDB();
//		cvtr = new DataByteArrayConverter();
		cccoral = new CAndCCoral(alloc);
	}

	// generate a constraint tuple that conforms to the schema and passes the
	// predicate
	// (or null if unable to find such a tuple)

	// ExampleTuple GenerateMatchingTuple(LogicalSchema schema,
	// LogicalExpressionPlan plan,
	// boolean invert) throws FrontendException, ExecException {
	// return GenerateMatchingTuple(plan, invert);
	// }

	// generate a constraint tuple that conforms to the constraint and passes
	// the predicate
	// (or null if unable to find such a tuple)
	//
	// for now, constraint tuples are tuples whose fields are a blend of actual
	// data values and nulls,
	// where a null stands for "don't care"
	//
	// in the future, may want to replace "don't care" with a more rich
	// constraint language; this would
	// help, e.g. in the case of two filters in a row (you want the downstream
	// filter to tell the upstream filter
	// what predicate it wants satisfied in a given field)
	//

	public ExampleTuple gettOut() {
		return tOut;
	}
	
	public void resettOut() {
		Tuple newTuple = TupleFactory.getInstance()
				.newTuple(schema.getFields().size()); 
		this.tOut = new ExampleTuple(newTuple);
		this.tOut.synthetic = false;
	}
	
	public void resetNullPos() {
		nullPos.clear();
	}

	ConstraintResultCoral GenerateLhsColumn(long uid, List<LogicalSchema> ls) {
		ConstraintResultCoral ret = null;
		for(Iterator<LogicalSchema> ils = ls.iterator(); ils.hasNext();) {
			LogicalSchema element = ils.next();
			List<LogicalSchema.LogicalFieldSchema> fields = element.getFields();
			for(Iterator<LogicalSchema.LogicalFieldSchema> ilfs = fields.iterator(); ilfs.hasNext();) {
				LogicalSchema.LogicalFieldSchema fieldInfo = ilfs.next();
				if(uid == fieldInfo.uid) {
					ret = mallocInCoral(fieldInfo);
					return ret;
				}
			}
		}
		return null;
	}
	
	
	List<ConstraintResultCoral> GenerateMatchingTuple(LOGenerate lgen)
			throws ExecException, FrontendException {
		List<LogicalExpressionPlan> gens = lgen.getOutputPlans();
		final Z3Context z3 = Z3Context.get();
		List<LogicalSchema> ls = lgen.getOutputPlanSchemas(); 
		List<ConstraintResultCoral> retAst = new ArrayList<ConstraintResultCoral>();
		
		for(Iterator<LogicalExpressionPlan> it = gens.iterator(); it
                    .hasNext();) {
			LogicalExpressionPlan genExp = it.next();
			Operator op = genExp.getSources().get(0);
			ConstraintResultCoral assertAst;
			
			if (op instanceof AddExpression) {
				assertAst = GenerateMatchingTupleHelper((AddExpression) op);
				long fuid = ((AddExpression) op).getFieldSchema().uid;
				ConstraintResultCoral lhs = GenerateLhsColumn(fuid, ls);
				if(lhs == null)
					return null;
				Object cntr = alloc.eq(lhs.getCoralast(), assertAst.getCoralast());
				alloc.post(cntr);
				ConstraintResultCoral r = new ConstraintResultCoral(cntr, DataType.BOOLEAN, CntrTruth.TRUE);
				retAst.add(r);
			} else if (op instanceof SubtractExpression) {
				assertAst = GenerateMatchingTupleHelper((SubtractExpression) op);
				long fuid = ((SubtractExpression) op).getFieldSchema().uid;
				ConstraintResultCoral lhs = GenerateLhsColumn(fuid, ls);
				if(lhs == null)
					return null;
				Object cntr = alloc.eq(lhs.getCoralast(), assertAst.getCoralast());
				alloc.post(cntr);
				ConstraintResultCoral r = new ConstraintResultCoral(cntr, DataType.BOOLEAN, CntrTruth.TRUE);
				retAst.add(r);
			} else if (op instanceof MultiplyExpression) {
				assertAst = GenerateMatchingTupleHelper((MultiplyExpression) op);
				long fuid = ((MultiplyExpression) op).getFieldSchema().uid;
				ConstraintResultCoral lhs = GenerateLhsColumn(fuid, ls);
				if(lhs == null)
					return null;
				Object cntr = alloc.eq(lhs.getCoralast(), assertAst.getCoralast());
				alloc.post(cntr);
				ConstraintResultCoral r = new ConstraintResultCoral(cntr, DataType.BOOLEAN, CntrTruth.TRUE);
				retAst.add(r);
			} else if (op instanceof DivideExpression) {
				assertAst = GenerateMatchingTupleHelper((DivideExpression) op);
				long fuid = ((DivideExpression) op).getFieldSchema().uid;
				ConstraintResultCoral lhs = GenerateLhsColumn(fuid, ls);
				if(lhs == null)
					return null;
				Object cntr = alloc.eq(lhs.getCoralast(), assertAst.getCoralast());
				alloc.post(cntr);
				ConstraintResultCoral r = new ConstraintResultCoral(cntr, DataType.BOOLEAN, CntrTruth.TRUE);
				retAst.add(r);
			} else if (op instanceof ModExpression) {
				assertAst = GenerateMatchingTupleHelper((ModExpression) op);
				long fuid = ((ModExpression) op).getFieldSchema().uid;
				ConstraintResultCoral lhs = GenerateLhsColumn(fuid, ls);
				if(lhs == null)
					return null;
				Object cntr = alloc.eq(lhs.getCoralast(), assertAst.getCoralast());
				alloc.post(cntr);
				ConstraintResultCoral r = new ConstraintResultCoral(cntr, DataType.BOOLEAN, CntrTruth.TRUE);
				retAst.add(r);
			} else if (op instanceof UserFuncExpression) {
				// has been taken care of using data flow hierarchy
				break;
			} else
				return null;
		}
		
//		tOut.synthetic = true;
		return retAst;
	}

	public ConstraintResultCoral GenerateMatchingTuple(LogicalExpressionPlan predicate, boolean invert)
			throws ExecException, FrontendException {
		// Tuple t = TupleFactory.getInstance().newTuple(constraint.size());
		//
		// for (int i = 0; i < t.size(); i++)
		// tOut.set(i, constraint.get(i));
		ConstraintResultCoral ret = GenerateMatchingTupleHelper(predicate.getSources().get(0), invert);
//		tOut.synthetic = true;
		return ret;
	}

	ConstraintResultCoral GenerateMatchingTupleHelper(Operator pred, boolean invert) throws FrontendException,
			ExecException {
		ConstraintResultCoral ret = null;
		if (pred instanceof BinaryExpression)
			ret = GenerateMatchingTupleHelper((BinaryExpression) pred, invert);
		else if (pred instanceof NotExpression) {
			ret = GenerateMatchingTupleHelper((NotExpression) pred, invert);
		}
		else if (pred instanceof IsNullExpression) {
			GenerateMatchingTupleHelper((IsNullExpression) pred, invert);
		}
		else if (pred instanceof UserFuncExpression) {
			ret = GenerateMatchingTupleHelper((UserFuncExpression)pred, invert);
		}
		else {
			throw new FrontendException("Unknown operator in filter predicate");
		}
		if(ret == null)
			ret = new ConstraintResultCoral();
		return ret;
	}

	ConstraintResultCoral GenerateMatchingTupleHelper(BinaryExpression pred,
			boolean invert) throws FrontendException, ExecException {
		final Z3Context z3 = Z3Context.get();
		ConstraintResultCoral lhsAst = null, rhsAst = null, retAst = null;
		Object cntr = null;
		LogicalExpression leftHs = pred.getLhs(), rightHs = pred.getRhs();

		if (pred instanceof AndExpression) {
			retAst = GenerateMatchingTupleHelper((AndExpression) pred, invert);
			return retAst;
		} else if (pred instanceof OrExpression) {
			retAst = GenerateMatchingTupleHelper((OrExpression) pred, invert);
			return retAst;
		} else if (pred instanceof RegexExpression) {
			retAst = GenerateMatchingTupleHelper((RegexExpression)pred, invert);
			return retAst;
		}

		// now we are sure that the expression operators are the roots of the
		// plan

		// boolean leftIsConst = false, rightIsConst = false;
		// Object leftConst = null, rightConst = null;
		// byte leftDataType = 0, rightDataType = 0;

		// int leftCol = -1, rightCol = -1;

		// if (pred instanceof AddExpression || pred instanceof
		// SubtractExpression
		// || pred instanceof MultiplyExpression || pred instanceof
		// DivideExpression
		// || pred instanceof ModExpression || pred instanceof RegexExpression)
		// return; // We don't try to work around these operators right now

		lhsAst = GenerateMatchingColumnExpression(leftHs);
		if (lhsAst == null) {
			if (leftHs instanceof AddExpression) {
				lhsAst = GenerateMatchingTupleHelper((AddExpression) leftHs);
			} else if (leftHs instanceof SubtractExpression) {
				lhsAst = GenerateMatchingTupleHelper((SubtractExpression) leftHs);
			} else if (leftHs instanceof MultiplyExpression) {
				lhsAst = GenerateMatchingTupleHelper((MultiplyExpression) leftHs);
			} else if (leftHs instanceof DivideExpression) {
				lhsAst = GenerateMatchingTupleHelper((DivideExpression) leftHs);
			} else if (leftHs instanceof ModExpression) {
				lhsAst = GenerateMatchingTupleHelper((ModExpression) leftHs);
			} else if (leftHs instanceof UserFuncExpression) {
				lhsAst = GenerateMatchingTupleHelper((UserFuncExpression)leftHs, invert);
			} else
				return null;
		}

		rhsAst = GenerateMatchingColumnExpression(rightHs);
		if (rhsAst == null) {
			if (rightHs instanceof AddExpression) {
				rhsAst = GenerateMatchingTupleHelper((AddExpression) rightHs);

			} else if (rightHs instanceof SubtractExpression) {
				rhsAst = GenerateMatchingTupleHelper((SubtractExpression) rightHs);

			} else if (rightHs instanceof MultiplyExpression) {
				rhsAst = GenerateMatchingTupleHelper((MultiplyExpression) rightHs);

			} else if (rightHs instanceof DivideExpression) {
				rhsAst = GenerateMatchingTupleHelper((DivideExpression) rightHs);

			} else if (rightHs instanceof ModExpression) {
				rhsAst = GenerateMatchingTupleHelper((ModExpression) rightHs);

			} else
				return null;
		}

		boolean isCLhs = lhsAst.isConst();
		boolean isCRhs = rhsAst.isConst();
		Object valueLhs = lhsAst.getConstVal();
		Object objectLhs = lhsAst.getCoralast();
		Object valueRhs = rhsAst.getConstVal();
		Object objectRhs = rhsAst.getCoralast(); 
		// now we try to change some nulls to constants

		// convert some nulls to constants
		if (!invert) {
			if (pred instanceof EqualExpression) {
				if (lhsAst.getType() == rhsAst.getType()) {
					if (lhsAst.getType() == DataType.INTEGER) {
						if(isCLhs && !isCRhs) {
							cntr = alloc.eq(((Integer)valueLhs).intValue(), objectRhs);
							alloc.post(cntr);
						} else if(!isCLhs && isCRhs) {
							cntr = alloc.eq(objectLhs, ((Integer)valueRhs).intValue());
							alloc.post(cntr);
						}
						else  {
							cntr = alloc.eq(lhsAst.getCoralast(), rhsAst.getCoralast());
							alloc.post(cntr);
						}
						
						retAst = new ConstraintResultCoral(cntr, DataType.BOOLEAN, CntrTruth.TRUE);
						return retAst;
					} else if (lhsAst.getType() == DataType.DOUBLE) {
						if(isCLhs && !isCRhs) {
							cntr = alloc.eq(((Double)valueLhs).doubleValue(), objectRhs);
							alloc.post(cntr);
						}
						else if(!isCLhs && isCRhs) {
							cntr = alloc.eq(objectLhs, ((Double)valueRhs).doubleValue());
							alloc.post(cntr);
						}
						else {
							cntr = alloc.eq(lhsAst.getCoralast(), rhsAst.getCoralast());
							alloc.post(cntr);
						}
						
						retAst = new ConstraintResultCoral(cntr, DataType.BOOLEAN, CntrTruth.TRUE);
						return retAst;
					} else if (lhsAst.getType() == DataType.CHARARRAY || lhsAst.getType() == DataType.BOOLEAN) {
						if (lhsAst.isConst()) {
		                    tOut.set(rhsAst.getCol(), generateData(rhsAst.getType(), lhsAst.getConstVal()
		                            .toString()));
		                } else if (rhsAst.isConst()) {
		                    tOut.set(lhsAst.getCol(), generateData(lhsAst.getType(), rhsAst.getConstVal()
		                            .toString()));
		                } else {
		                    tOut.set(lhsAst.getCol(), generateData(lhsAst.getType(), "0"));
		                    tOut.set(rhsAst.getCol(), generateData(rhsAst.getType(), "0"));
		                }
					}
				} else
					return null;
			} else if (pred instanceof NotEqualExpression) {
				if (lhsAst.getType() == rhsAst.getType()) {
					if (lhsAst.getType() == DataType.INTEGER) {
						if(isCLhs && !isCRhs) {
							cntr = alloc.neq(((Integer)valueLhs).intValue(), objectRhs);
							alloc.post(cntr);
						}
						else if(!isCLhs && isCRhs) {
							cntr = alloc.neq(objectLhs, ((Integer)valueRhs).intValue());
							alloc.post(cntr);
						}
						else {
							cntr = alloc.neq(lhsAst.getCoralast(), rhsAst.getCoralast());
							alloc.post(cntr);
						}
						
						retAst = new ConstraintResultCoral(cntr, DataType.BOOLEAN, CntrTruth.TRUE);
						return retAst;
					} else if (lhsAst.getType() == DataType.DOUBLE) {
						if(isCLhs && !isCRhs) {
							cntr = alloc.neq(((Double)valueLhs).doubleValue(), objectRhs);
							alloc.post(cntr);
						}
						else if(!isCLhs && isCRhs) {
							cntr = alloc.neq(objectLhs, ((Double)valueRhs).doubleValue());
							alloc.post(cntr);
						}
						else {
							cntr = alloc.neq(lhsAst.getCoralast(), rhsAst.getCoralast());
							alloc.post(cntr);
						}
						
						retAst = new ConstraintResultCoral(cntr, DataType.BOOLEAN, CntrTruth.TRUE);
						return retAst;
					}  else if (lhsAst.getType() == DataType.CHARARRAY || lhsAst.getType() == DataType.BOOLEAN) {
						if (lhsAst.isConst()) {
		                    tOut.set(rhsAst.getCol(), generateData(rhsAst.getType(),
		                            GetUnequalValue(lhsAst.getConstVal()).toString()));
		                } else if (rhsAst.isConst()) {
		                	tOut.set(lhsAst.getCol(), generateData(lhsAst.getType(), GetUnequalValue(
		                			rhsAst.getConstVal()).toString()));
		                } else {
		                	tOut.set(lhsAst.getCol(), generateData(lhsAst.getType(), "0"));
		                	tOut.set(rhsAst.getCol(), generateData(rhsAst.getType(), "1"));
		                }
					}
				} else
					return null;
			} else if (pred instanceof GreaterThanExpression) {
				if (lhsAst.getType() == rhsAst.getType()) {
					if (lhsAst.getType() == DataType.INTEGER) {
						if(isCLhs && !isCRhs) {
							cntr = alloc.gt(((Integer)valueLhs).intValue(), objectRhs);
							alloc.post(cntr);
						}
						else if(!isCLhs && isCRhs) {
							cntr = alloc.gt(objectLhs, ((Integer)valueRhs).intValue());
							alloc.post(cntr);
						}
						else {
							cntr = alloc.gt(lhsAst.getCoralast(), rhsAst.getCoralast());
							alloc.post(cntr);
						}
						
						retAst = new ConstraintResultCoral(cntr, DataType.BOOLEAN, CntrTruth.TRUE);
						return retAst;
					} else if (lhsAst.getType() == DataType.DOUBLE) {
						if(isCLhs && !isCRhs) {
							cntr = alloc.gt(((Double)valueLhs).doubleValue(), objectRhs);
							alloc.post(cntr);
						}
						else if(!isCLhs && isCRhs) {
							cntr = alloc.gt(objectLhs, ((Double)valueRhs).doubleValue());
							alloc.post(cntr);
						}
						else {
							cntr = alloc.gt(lhsAst.getCoralast(), rhsAst.getCoralast());
							alloc.post(cntr);
						}
						
						retAst = new ConstraintResultCoral(cntr, DataType.BOOLEAN, CntrTruth.TRUE);
						return retAst;
					} 
					else if (lhsAst.getType() == DataType.CHARARRAY || lhsAst.getType() == DataType.BOOLEAN) {
						if (lhsAst.isConst()) {
		                    tOut.set(rhsAst.getCol(), generateData(rhsAst.getType(),
		                            GetSmallerValue(lhsAst.getConstVal()).toString()));
		                } else if (rhsAst.isConst()) {
		                	tOut.set(lhsAst.getCol(), generateData(lhsAst.getType(), GetLargerValue(
		                			rhsAst.getConstVal()).toString()));
		                } else {
		                	tOut.set(lhsAst.getCol(), generateData(lhsAst.getType(), "1"));
		                	tOut.set(rhsAst.getCol(), generateData(rhsAst.getType(), "0"));
		                }
					} else {
						check(false);
						return null;
					}
				} else
					return null;
			} else if (pred instanceof GreaterThanEqualExpression) {
				if (lhsAst.getType() == rhsAst.getType()) {
					if (lhsAst.getType() == DataType.INTEGER) {
						if(isCLhs && !isCRhs) {
							cntr = alloc.geq(((Integer)valueLhs).intValue(), objectRhs);
							alloc.post(cntr);
						}
						else if(!isCLhs && isCRhs) {
							cntr = alloc.geq(objectLhs, ((Integer)valueRhs).intValue());
							alloc.post(cntr);
						}
						else {
							cntr = alloc.geq(lhsAst.getCoralast(), rhsAst.getCoralast());
							alloc.post(cntr);
						}
						
						retAst = new ConstraintResultCoral(cntr, DataType.BOOLEAN, CntrTruth.TRUE);
						return retAst;
					} else if (lhsAst.getType() == DataType.DOUBLE) {
						if(isCLhs && !isCRhs) {
							cntr = alloc.geq(((Double)valueLhs).doubleValue(), objectRhs);
							alloc.post(cntr);
						}
						else if(!isCLhs && isCRhs) {
							cntr = alloc.geq(objectLhs, ((Double)valueRhs).doubleValue());
							alloc.post(cntr);
						}
						else {
							cntr = alloc.geq(lhsAst.getCoralast(), rhsAst.getCoralast());
							alloc.post(cntr);
						}
						
						retAst = new ConstraintResultCoral(cntr, DataType.BOOLEAN, CntrTruth.TRUE);
						return retAst;
					} 
					else if (lhsAst.getType() == DataType.CHARARRAY || lhsAst.getType() == DataType.BOOLEAN) {
						if (lhsAst.isConst()) {
		                    tOut.set(rhsAst.getCol(), generateData(rhsAst.getType(),
		                            GetSmallerValue(lhsAst.getConstVal()).toString()));
		                } else if (rhsAst.isConst()) {
		                	tOut.set(lhsAst.getCol(), generateData(lhsAst.getType(), GetLargerValue(
		                			rhsAst.getConstVal()).toString()));
		                } else {
		                	tOut.set(lhsAst.getCol(), generateData(lhsAst.getType(), "1"));
		                	tOut.set(rhsAst.getCol(), generateData(rhsAst.getType(), "0"));
		                }
					} else {
						check(false);
						return null;
					}
				} else
					return null;
			} else if (pred instanceof LessThanExpression) {
				if (lhsAst.getType() == rhsAst.getType()) {
					if (lhsAst.getType() == DataType.INTEGER) {
						if(isCLhs && !isCRhs) {
							cntr = alloc.lt(((Integer)valueLhs).intValue(), objectRhs);
							alloc.post(cntr);
						}
						else if(!isCLhs && isCRhs) {
							cntr = alloc.lt(objectLhs, ((Integer)valueRhs).intValue());
							alloc.post(cntr);
						}
						else {
							cntr = alloc.lt(lhsAst.getCoralast(), rhsAst.getCoralast());
							alloc.post(cntr);
						}
						
						retAst = new ConstraintResultCoral(cntr, DataType.BOOLEAN, CntrTruth.TRUE);
						return retAst;
					} else if (lhsAst.getType() == DataType.DOUBLE) {
						if(isCLhs && !isCRhs) {
							cntr = alloc.lt(((Double)valueLhs).doubleValue(), objectRhs);
							alloc.post(cntr);
						}
						else if(!isCLhs && isCRhs) {
							cntr = alloc.lt(objectLhs, ((Double)valueRhs).doubleValue());
							alloc.post(cntr);
						}
						else {
							cntr = alloc.lt(lhsAst.getCoralast(), rhsAst.getCoralast());
							alloc.post(cntr);
						}
						
						retAst = new ConstraintResultCoral(cntr, DataType.BOOLEAN, CntrTruth.TRUE);
						return retAst;
					} 
					else if (lhsAst.getType() == DataType.CHARARRAY || lhsAst.getType() == DataType.BOOLEAN) {
						if (lhsAst.isConst()) {
		                    tOut.set(rhsAst.getCol(), generateData(rhsAst.getType(), GetLargerValue(
		                    		lhsAst.getConstVal()).toString()));
		                } else if (rhsAst.isConst()) {
		                    tOut.set(lhsAst.getCol(), generateData(lhsAst.getType(), GetSmallerValue(
		                    		rhsAst.getConstVal()).toString()));
		                } else {
		                    tOut.set(lhsAst.getCol(), generateData(lhsAst.getType(), "0"));
		                    tOut.set(rhsAst.getCol(), generateData(rhsAst.getType(), "1"));
		                }
					} else {
						check(false);
						return null;
					}
				} else
					return null;
			} else if (pred instanceof LessThanEqualExpression) {
				if (lhsAst.getType() == rhsAst.getType()) {
					if (lhsAst.getType() == DataType.INTEGER) {
						if(isCLhs && !isCRhs) {
							cntr = alloc.leq(((Integer)valueLhs).intValue(), objectRhs);
							alloc.post(cntr);
						}
						else if(!isCLhs && isCRhs) {
							cntr = alloc.leq(objectLhs, ((Integer)valueRhs).intValue());
							alloc.post(cntr);
						}
						else {
							cntr = alloc.leq(lhsAst.getCoralast(), rhsAst.getCoralast());
							alloc.post(cntr);
						}
						
						retAst = new ConstraintResultCoral(cntr, DataType.BOOLEAN, CntrTruth.TRUE);
						return retAst;
					} else if (lhsAst.getType() == DataType.DOUBLE) {
						if(isCLhs && !isCRhs) {
							cntr = alloc.leq(((Double)valueLhs).doubleValue(), objectRhs);
							alloc.post(cntr);
						}
						else if(!isCLhs && isCRhs) {
							cntr = alloc.leq(objectLhs, ((Double)valueRhs).doubleValue());
							alloc.post(cntr);
						}
						else {
							cntr = alloc.leq(lhsAst.getCoralast(), rhsAst.getCoralast());
							alloc.post(cntr);
						}
						
						retAst = new ConstraintResultCoral(cntr, DataType.BOOLEAN, CntrTruth.TRUE);
						return retAst;
					}  else if (lhsAst.getType() == DataType.CHARARRAY || lhsAst.getType() == DataType.BOOLEAN) {
						if (lhsAst.isConst()) {
		                    tOut.set(rhsAst.getCol(), generateData(rhsAst.getType(), GetLargerValue(
		                    		lhsAst.getConstVal()).toString()));
		                } else if (rhsAst.isConst()) {
		                    tOut.set(lhsAst.getCol(), generateData(lhsAst.getType(), GetSmallerValue(
		                    		rhsAst.getConstVal()).toString()));
		                } else {
		                    tOut.set(lhsAst.getCol(), generateData(lhsAst.getType(), "0"));
		                    tOut.set(rhsAst.getCol(), generateData(rhsAst.getType(), "1"));
		                }
					} else {
						check(false);
						return null;
					}
				} else
					return null;
			} 
		} else {
			if (pred instanceof EqualExpression) {
				if (lhsAst.getType() == rhsAst.getType()) {
					if (lhsAst.getType() == DataType.INTEGER) {
						if(isCLhs && !isCRhs) {
							cntr = alloc.neq(((Integer)valueLhs).intValue(), objectRhs);
							alloc.post(cntr);
						}
						else if(!isCLhs && isCRhs) {
							cntr = alloc.neq(objectLhs, ((Integer)valueRhs).intValue());
							alloc.post(cntr);
						}
						else {
							cntr = alloc.neq(lhsAst.getCoralast(), rhsAst.getCoralast());
							alloc.post(cntr);
						}
						
						retAst = new ConstraintResultCoral(cntr, DataType.BOOLEAN, CntrTruth.TRUE);
						return retAst;
					} else if (lhsAst.getType() == DataType.DOUBLE) {
						if(isCLhs && !isCRhs) {
							cntr = alloc.neq(((Double)valueLhs).doubleValue(), objectRhs);
							alloc.post(cntr);
						}
						else if(!isCLhs && isCRhs) {
							cntr = alloc.neq(objectLhs, ((Double)valueRhs).doubleValue());
							alloc.post(cntr);
						}
						else {
							cntr = alloc.neq(lhsAst.getCoralast(), rhsAst.getCoralast());
							alloc.post(cntr);
						}
						
						retAst = new ConstraintResultCoral(cntr, DataType.BOOLEAN, CntrTruth.TRUE);
						return retAst;
					} else if (lhsAst.getType() == DataType.CHARARRAY || lhsAst.getType() == DataType.BOOLEAN) {
						if (lhsAst.isConst()) {
		                    tOut.set(rhsAst.getCol(), generateData(rhsAst.getType(),
		                            GetUnequalValue(lhsAst.getConstVal()).toString()));
		                } else if (rhsAst.isConst()) {
		                    tOut.set(lhsAst.getCol(), generateData(lhsAst.getType(), GetUnequalValue(
		                    		rhsAst.getConstVal()).toString()));
		                } else {
		                    tOut.set(lhsAst.getCol(), generateData(lhsAst.getType(), "0"));
		                    tOut.set(rhsAst.getCol(), generateData(rhsAst.getType(), "1"));
		                }
					}
				} else
					return null;
			} else if (pred instanceof NotEqualExpression) {
				if (lhsAst.getType() == rhsAst.getType()) {
					if (lhsAst.getType() == DataType.INTEGER) {
						if(isCLhs && !isCRhs) {
							cntr = alloc.eq(((Integer)valueLhs).intValue(), objectRhs);
							alloc.post(cntr);
						}
						else if(!isCLhs && isCRhs) {
							cntr = alloc.eq(objectLhs, ((Integer)valueRhs).intValue());
							alloc.post(cntr);
						}
						else {
							cntr = alloc.eq(lhsAst.getCoralast(), rhsAst.getCoralast());
							alloc.post(cntr);
						}
						
						retAst = new ConstraintResultCoral(cntr, DataType.BOOLEAN, CntrTruth.TRUE);
						return retAst;
					} else if (lhsAst.getType() == DataType.DOUBLE) {
						if(isCLhs && !isCRhs) {
							cntr = alloc.eq(((Double)valueLhs).doubleValue(), objectRhs);
							alloc.post(cntr);
						}
						else if(!isCLhs && isCRhs) {
							cntr = alloc.eq(objectLhs, ((Double)valueRhs).doubleValue());
							alloc.post(cntr);
						}
						else {
							cntr = alloc.eq(lhsAst.getCoralast(), rhsAst.getCoralast());
							alloc.post(cntr);
						}
						
						retAst = new ConstraintResultCoral(cntr, DataType.BOOLEAN, CntrTruth.TRUE);
						return retAst;
					} else if (lhsAst.getType() == DataType.CHARARRAY || lhsAst.getType() == DataType.BOOLEAN) {
						if (lhsAst.isConst()) {
		                    tOut.set(rhsAst.getCol(), generateData(rhsAst.getType(), lhsAst.getConstVal()
		                            .toString()));
		                } else if (rhsAst.isConst()) {
		                    tOut.set(lhsAst.getCol(), generateData(lhsAst.getType(), rhsAst.getConstVal()
		                            .toString()));
		                } else {
		                    tOut.set(lhsAst.getCol(), generateData(lhsAst.getType(), "0"));
		                    tOut.set(rhsAst.getCol(), generateData(rhsAst.getType(), "0"));
		                }
					}
				} else
					return null;
			} else if (pred instanceof GreaterThanExpression) {
				if (lhsAst.getType() == rhsAst.getType()) {
					if (lhsAst.getType() == DataType.INTEGER) {
						if(isCLhs && !isCRhs) {
							cntr = alloc.leq(((Integer)valueLhs).intValue(), objectRhs);
							alloc.post(cntr);
						}
						else if(!isCLhs && isCRhs) {
							cntr = alloc.leq(objectLhs, ((Integer)valueRhs).intValue());
							alloc.post(cntr);
						}
						else {
							cntr = alloc.leq(lhsAst.getCoralast(), rhsAst.getCoralast());
							alloc.post(cntr);
						}
						
						retAst = new ConstraintResultCoral(cntr, DataType.BOOLEAN, CntrTruth.TRUE);
						return retAst;
					} else if (lhsAst.getType() == DataType.DOUBLE) {
						if(isCLhs && !isCRhs) {
							cntr = alloc.leq(((Double)valueLhs).doubleValue(), objectRhs);
							alloc.post(cntr);
						}
						else if(!isCLhs && isCRhs) {
							cntr = alloc.leq(objectLhs, ((Double)valueRhs).doubleValue());
							alloc.post(cntr);
						}
						else {
							cntr = alloc.leq(lhsAst.getCoralast(), rhsAst.getCoralast());
							alloc.post(cntr);
						}
						
						retAst = new ConstraintResultCoral(cntr, DataType.BOOLEAN, CntrTruth.TRUE);
						return retAst;
					} 
					else if (lhsAst.getType() == DataType.CHARARRAY || lhsAst.getType() == DataType.BOOLEAN) {
						if (lhsAst.isConst()) {
		                    tOut.set(rhsAst.getCol(), generateData(rhsAst.getType(), GetLargerValue(
		                    		lhsAst.getConstVal()).toString()));
		                } else if (rhsAst.isConst()) {
		                    tOut.set(lhsAst.getCol(), generateData(lhsAst.getType(), GetSmallerValue(
		                    		rhsAst.getConstVal()).toString()));
		                } else {
		                    tOut.set(lhsAst.getCol(), generateData(lhsAst.getType(), "0"));
		                    tOut.set(rhsAst.getCol(), generateData(rhsAst.getType(), "1"));
		                }
					} else {
						check(false);
						return null;
					}
				} else
					return null;
			} else if (pred instanceof GreaterThanEqualExpression) {
				if (lhsAst.getType() == rhsAst.getType()) {
					if (lhsAst.getType() == DataType.INTEGER) {
						if(isCLhs && !isCRhs) {
							cntr = alloc.lt(((Integer)valueLhs).intValue(), objectRhs);
							alloc.post(cntr);
						}
						else if(!isCLhs && isCRhs) {
							cntr = alloc.lt(objectLhs, ((Integer)valueRhs).intValue());
							alloc.post(cntr);
						}
						else {
							cntr = alloc.lt(lhsAst.getCoralast(), rhsAst.getCoralast());
							alloc.post(cntr);
						}
						
						retAst = new ConstraintResultCoral(cntr, DataType.BOOLEAN, CntrTruth.TRUE);
						return retAst;
					} else if (lhsAst.getType() == DataType.DOUBLE) {
						if(isCLhs && !isCRhs) {
							cntr = alloc.lt(((Double)valueLhs).doubleValue(), objectRhs);
							alloc.post(cntr);
						}
						else if(!isCLhs && isCRhs) {
							cntr = alloc.lt(objectLhs, ((Double)valueRhs).doubleValue());
							alloc.post(cntr);
						}
						else {
							cntr = alloc.lt(lhsAst.getCoralast(), rhsAst.getCoralast());
							alloc.post(cntr);
						}
						
						retAst = new ConstraintResultCoral(cntr, DataType.BOOLEAN, CntrTruth.TRUE);
						return retAst;
					} 
					else if (lhsAst.getType() == DataType.CHARARRAY || lhsAst.getType() == DataType.BOOLEAN) {
						if (lhsAst.isConst()) {
		                    tOut.set(rhsAst.getCol(), generateData(rhsAst.getType(), GetLargerValue(
		                    		lhsAst.getConstVal()).toString()));
		                } else if (rhsAst.isConst()) {
		                    tOut.set(lhsAst.getCol(), generateData(lhsAst.getType(), GetSmallerValue(
		                    		rhsAst.getConstVal()).toString()));
		                } else {
		                    tOut.set(lhsAst.getCol(), generateData(lhsAst.getType(), "0"));
		                    tOut.set(rhsAst.getCol(), generateData(rhsAst.getType(), "1"));
		                }
					} else {
						check(false);
						return null;
					}
				} else
					return null;
			} else if (pred instanceof LessThanExpression) {
				if (lhsAst.getType() == rhsAst.getType()) {
					if (lhsAst.getType() == DataType.INTEGER) {
						if(isCLhs && !isCRhs) {
							cntr = alloc.geq(((Integer)valueLhs).intValue(), objectRhs);
							alloc.post(cntr);
						}
						else if(!isCLhs && isCRhs) {
							cntr = alloc.geq(objectLhs, ((Integer)valueRhs).intValue());
							alloc.post(cntr);
						}
						else {
							cntr = alloc.geq(lhsAst.getCoralast(), rhsAst.getCoralast());
							alloc.post(cntr);
						}
						
						retAst = new ConstraintResultCoral(cntr, DataType.BOOLEAN, CntrTruth.TRUE);
						return retAst;
					} else if (lhsAst.getType() == DataType.DOUBLE) {
						if(isCLhs && !isCRhs) {
							cntr = alloc.geq(((Double)valueLhs).doubleValue(), objectRhs);
							alloc.post(cntr);
						}
						else if(!isCLhs && isCRhs) {
							cntr = alloc.geq(objectLhs, ((Double)valueRhs).doubleValue());
							alloc.post(cntr);
						}
						else {
							cntr = alloc.geq(lhsAst.getCoralast(), rhsAst.getCoralast());
							alloc.post(cntr);
						}
						
						retAst = new ConstraintResultCoral(cntr, DataType.BOOLEAN, CntrTruth.TRUE);
						return retAst;
					} 
					else if (lhsAst.getType() == DataType.CHARARRAY || lhsAst.getType() == DataType.BOOLEAN) {
						if (lhsAst.isConst()) {
		                    tOut.set(rhsAst.getCol(), generateData(rhsAst.getType(),
		                            GetSmallerValue(lhsAst.getConstVal()).toString()));
		                } else if (rhsAst.isConst()) {
		                    tOut.set(lhsAst.getCol(), generateData(lhsAst.getType(), GetLargerValue(
		                    		rhsAst.getConstVal()).toString()));
		                } else {
		                    tOut.set(lhsAst.getCol(), generateData(lhsAst.getType(), "1"));
		                    tOut.set(rhsAst.getCol(), generateData(rhsAst.getType(), "0"));
		                }
					} else {
						check(false);
						return null;
					}
				} else
					return null;
			} else if (pred instanceof LessThanEqualExpression) {
				if (lhsAst.getType() == rhsAst.getType()) {
					if (lhsAst.getType() == DataType.INTEGER) {
						if(isCLhs && !isCRhs) {
							cntr = alloc.gt(((Integer)valueLhs).intValue(), objectRhs);
							alloc.post(cntr);
						}
						else if(!isCLhs && isCRhs) {
							cntr = alloc.gt(objectLhs, ((Integer)valueRhs).intValue());
							alloc.post(cntr);
						}
						else {
							cntr = alloc.gt(lhsAst.getCoralast(), rhsAst.getCoralast());
							alloc.post(cntr);
						}
						
						retAst = new ConstraintResultCoral(cntr, DataType.BOOLEAN, CntrTruth.TRUE);
						return retAst;
					} else if (lhsAst.getType() == DataType.DOUBLE) {
						if(isCLhs && !isCRhs) {
							cntr = alloc.gt(((Double)valueLhs).doubleValue(), objectRhs);
							alloc.post(cntr);
						}
						else if(!isCLhs && isCRhs) {
							cntr = alloc.gt(objectLhs, ((Double)valueRhs).doubleValue());
							alloc.post(cntr);
						}
						else {
							cntr = alloc.gt(lhsAst.getCoralast(), rhsAst.getCoralast());
							alloc.post(cntr);
						}
						
						retAst = new ConstraintResultCoral(cntr, DataType.BOOLEAN, CntrTruth.TRUE);
						return retAst;
					} 
					else if (lhsAst.getType() == DataType.CHARARRAY || lhsAst.getType() == DataType.BOOLEAN) {
						if (lhsAst.isConst()) {
		                    tOut.set(rhsAst.getCol(), generateData(rhsAst.getType(),
		                            GetSmallerValue(lhsAst.getConstVal()).toString()));
		                } else if (rhsAst.isConst()) {
		                    tOut.set(lhsAst.getCol(), generateData(lhsAst.getType(), GetLargerValue(
		                    		rhsAst.getConstVal()).toString()));
		                } else {
		                    tOut.set(lhsAst.getCol(), generateData(lhsAst.getType(), "1"));
		                    tOut.set(rhsAst.getCol(), generateData(rhsAst.getType(), "0"));
		                }
					} else {
						check(false);
						return null;
					}
				} else
					return null;
			}
		}
		return retAst;

	}

	
	
	
	
	protected ConstraintResultCoral mallocInCoral(LogicalSchema.LogicalFieldSchema pe) {
		//final Z3Context z3 = Z3Context.get();
		String s;
		ConstraintResultCoral ret = null;
		Object z = null;
		byte type;
		
		s = pe.alias;
		type = pe.type;
		switch (type) {
		case DataType.INTEGER:
			z = cccoral.checkCreateInt(s);
			break;
		case DataType.DOUBLE:
			z = cccoral.checkCreateReal(s);
			break;
		default:
			return null;
		}
		
		ret = new ConstraintResultCoral(z, type, CntrTruth.UNKNOWN);
		return ret;
	}
	
//	private String analyzeName(String n) {
//		for(String name : nameDeclared) {
//			if(name.equals(n))
//				return n;
//			else if(name.indexOf("::" + n) > 0)
//				return name;
//		}
//		nameDeclared.add(n);
//		return n;
//	}

	protected ConstraintResultCoral mallocInCoral(ProjectExpression pe, byte typeCast) {
		byte type;
		String alias, alias2;
		int lcol;
		try {
			alias = pe.getFieldSchema().alias;

			if(nameDeclared != null) {
				String aName = sync.analyzeName(alias, nameDeclared);
				if(alias.equals(aName)) {
					// add declared name to an array to make sure every one use the same name
					// for the same variable.
					nameDeclared.add(alias);
				} 
				alias2 = aName;
			} else {
				alias2 = alias;
			}
			
			if(typeCast == -1)
				type = pe.getType();
			else
				type = typeCast;
			lcol = extr.extractPosition(schema, alias);
			return mallocInCoral(alias2, type, lcol);//, fl
		} catch (FrontendException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	
	public ConstraintResultCoral mallocInCoral(String alias, byte type, int lcol) {//, FakeLimit f
		String s;
		Object z = null;
		ConstraintResultCoral ret = null;
		s = alias;
		//FakeLimit f = new FakeLimit();
		
		switch (type) {
		case DataType.INTEGER:
			z = cccoral.checkCreateInt(s);
			break;
		case DataType.DOUBLE:
			z = cccoral.checkCreateReal(s);
			break;
		default:
			return null;
		}
		
         
		ret = new ConstraintResultCoral(z, type, CntrTruth.UNKNOWN, lcol);
		return ret;
	}
	
	

	protected ConstraintResultCoral mallocInCoral(ConstantExpression ce, byte typeCast) {
		Object value = ce.getValue();
		byte type;
		if(typeCast == -1)
			type = DataType.findType(value);
		else
			type = typeCast;
		
		return new ConstraintResultCoral(value, type, true);
	}
	
	

	

	
	protected byte getType(LogicalExpression le) {
		try {
			if (le instanceof ProjectExpression) {
				return ((ProjectExpression) le).getType();
			} else if (le instanceof ConstantExpression) {
				return DataType.findType(((ConstantExpression) le).getValue());
			}
		} catch (FrontendException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return DataType.ERROR;
	}

	ConstraintResultCoral GenerateMatchingColumnExpression(LogicalExpression op)
			throws FrontendException, ExecException {
		ConstraintResultCoral ret = null;
		byte typeCast = -1;
		if (op instanceof ConstantExpression) {
			ret = mallocInCoral((ConstantExpression) op, typeCast);
			
		} else {
			if (op instanceof CastExpression) {
				CastExpression castOp = (CastExpression) op;
				op = castOp.getExpression();
				typeCast = castOp.getFieldSchema().type;
			}
			// if (!(pred.getLhsOperand() instanceof ProjectExpression &&
			// ((ProjectExpression)
			// pred
			// .getLhsOperand()).getProjection().size() == 1))
			// return; // too hard
			if (op instanceof ProjectExpression) {
				ret = mallocInCoral((ProjectExpression) op, typeCast);
			}
			else if (op instanceof ConstantExpression) {
				ret = mallocInCoral((ConstantExpression) op, typeCast);
			} else if (op instanceof AddExpression) {
				ret = GenerateMatchingTupleHelper((AddExpression) op, typeCast);
			} else if (op instanceof SubtractExpression) {
				ret = GenerateMatchingTupleHelper((SubtractExpression) op, typeCast);
			} else if (op instanceof MultiplyExpression) {
				ret = GenerateMatchingTupleHelper((MultiplyExpression) op, typeCast);
			} else if (op instanceof DivideExpression) {
				ret = GenerateMatchingTupleHelper((DivideExpression) op, typeCast);
			} else if (op instanceof ModExpression) {
				ret = GenerateMatchingTupleHelper((ModExpression) op, typeCast);
			} else if (op instanceof UserFuncExpression) {
				ret = GenerateMatchingTupleHelper((UserFuncExpression) op, typeCast);
			} else
				return null;
//			if(ret == null && op.getType() == DataType.CHARARRAY) {
//				
//			}
		}
		return ret;
	}
	
	ConstraintResultCoral GenerateMatchingColumnExpression(LogicalExpression op, byte typeCast)
			throws FrontendException, ExecException {
		ConstraintResultCoral ret = null;
		
		if (op instanceof ConstantExpression) {
			ret = mallocInCoral((ConstantExpression) op, typeCast);
			
		} else {
			if (op instanceof CastExpression) {
				CastExpression castOp = (CastExpression) op;
				op = castOp.getExpression();
				typeCast = castOp.getFieldSchema().type;
			}
			
			if (op instanceof ProjectExpression) {
				ret = mallocInCoral((ProjectExpression) op, typeCast);
			}
			else if (op instanceof ConstantExpression) {
				ret = mallocInCoral((ConstantExpression) op, typeCast);
			} else if (op instanceof AddExpression) {
				ret = GenerateMatchingTupleHelper((AddExpression) op, typeCast);
			} else if (op instanceof SubtractExpression) {
				ret = GenerateMatchingTupleHelper((SubtractExpression) op, typeCast);
			} else if (op instanceof MultiplyExpression) {
				ret = GenerateMatchingTupleHelper((MultiplyExpression) op, typeCast);
			} else if (op instanceof DivideExpression) {
				ret = GenerateMatchingTupleHelper((DivideExpression) op, typeCast);
			} else if (op instanceof ModExpression) {
				ret = GenerateMatchingTupleHelper((ModExpression) op, typeCast);
			} else if (op instanceof UserFuncExpression) {
				ret = GenerateMatchingTupleHelper((UserFuncExpression) op, typeCast);
			} else
				return null;
//			if(ret == null && op.getType() == DataType.CHARARRAY) {
//				
//			}
		}
		return ret;
	}
	
	ConstraintResultCoral ConnectLhsRhsAnd(ConstraintResultCoral lr, ConstraintResultCoral rr, byte b) {
		// do nothing; if you continue to call post, constraints will be anded together.
		return null;
	}
	
	ConstraintResultCoral ConnectLhsRhsOr(ConstraintResultCoral lr, ConstraintResultCoral rr, byte b) {
		throw new RuntimeException("Don't support OR in a first-logic order expression in Coral.");
	}

	/**
	 * Original implementation is bad. When they generate data for a column,
	 * they just inspect a unit constraint in a whole bunch of constriant. For
	 * example, x>5 && x<5.5 and x is a double, they would generate x=6 for the
	 * left-hand constraint , and set t.x =6 where t is the tuple in the
	 * parameter. Then for the right-hand constraint, they would generate x=4.5,
	 * and set t.x=4.5. But t.x=4.5 is in conflict with the first constraint.
	 */
	ConstraintResultCoral GenerateMatchingTupleHelper(AndExpression op, boolean invert)
			throws FrontendException, ExecException {
		Operator left, right;
		ConstraintResultCoral lr, rr;
		ConstraintResultCoral ret;
		byte b = CntrTruth.toCntrTruth(invert);
		
		if(!invert) {
			left = op.getLhs();
			lr = GenerateMatchingTupleHelper(left, invert);
			right = op.getRhs();
			rr = GenerateMatchingTupleHelper(right, invert);
			ret = ConnectLhsRhsAnd(lr, rr, b);
			
			return ret;
		} else {
			left = op.getLhs();
			lr = GenerateMatchingTupleHelper(left, invert);
			right = op.getRhs();
			rr = GenerateMatchingTupleHelper(right, invert);
			ret = ConnectLhsRhsOr(lr, rr, b);
			return ret;
		}
	}

	ConstraintResultCoral GenerateMatchingTupleHelper(OrExpression op, boolean invert)
			throws FrontendException, ExecException {
		Operator left, right;
		ConstraintResultCoral lr, rr;
		
		ConstraintResultCoral ret;
		byte b = CntrTruth.toCntrTruth(invert);
		
		if(invert) {
			left = op.getLhs();
			lr = GenerateMatchingTupleHelper(left, invert);
			right = op.getRhs();
			rr = GenerateMatchingTupleHelper(right, invert);
			ret = ConnectLhsRhsAnd(lr, rr, b);
			return ret;
		} else {
			left = op.getLhs();
			lr = GenerateMatchingTupleHelper(left, invert);
			right = op.getRhs();
			rr = GenerateMatchingTupleHelper(right, invert);
			ret = ConnectLhsRhsOr(lr, rr, b);
			return ret;
		}
		

	}

	ConstraintResultCoral GenerateMatchingTupleHelper(AddExpression op, byte typecast)
			throws FrontendException, ExecException {
		ConstraintResultCoral left = GenerateMatchingColumnExpression(op.getLhs(), typecast);
		ConstraintResultCoral right = GenerateMatchingColumnExpression(op.getRhs(), typecast);
		if (left == null || right == null) {
			System.err
					.println("Cannot support complex arithmetic function now.");
			return null;
		} else {
			byte typeLhs = left.getType();//getType(op.getLhs());
			byte typeRhs = right.getType();//getType(op.getRhs());
			boolean isCLhs = left.isConst();
			boolean isCRhs = right.isConst();
			Object valueLhs = left.getConstVal();
			Object astLhs = left.getCoralast();
			Object valueRhs = right.getConstVal();
			Object astRhs = right.getCoralast();
			Object c = null;
			if ((typeLhs == DataType.INTEGER && typeRhs == DataType.INTEGER)) {
				//|| (typeLhs == DataType.LONG && typeRhs == DataType.LONG)
				if(isCLhs && !isCRhs) {
					c = alloc.plus(((Integer)valueLhs).intValue(), astRhs);
//					alloc.post(c);
				}
				else if(!isCLhs && isCRhs) {
					c = alloc.plus(astLhs, ((Integer)valueRhs).intValue());
//					alloc.post(c);
				}
				else {
					c = alloc.plus(astLhs, astRhs);
//					alloc.post(c);
				}
			} else if ((typeLhs == DataType.DOUBLE && typeRhs == DataType.DOUBLE)) {
				//(typeLhs == DataType.FLOAT && typeRhs == DataType.FLOAT)
				if(isCLhs && !isCRhs) {
					c = alloc.plus(((Double)valueLhs).doubleValue(), astRhs);
//					alloc.post(c);
				}
				else if(!isCLhs && isCRhs) {
					c = alloc.plus(astLhs, ((Double)valueRhs).doubleValue());
//					alloc.post(c);
				}
				else {
					c = alloc.plus(astLhs, astRhs);
//					alloc.post(c);
				}
			} else {
				check(false); // TODO
				return null;
			}
			ConstraintResultCoral ret = new ConstraintResultCoral(c, typeLhs, CntrTruth.UNKNOWN);
			return ret;
		}

	}

	ConstraintResultCoral GenerateMatchingTupleHelper(SubtractExpression op, byte typecast)
			throws FrontendException, ExecException {
		ConstraintResultCoral left = GenerateMatchingColumnExpression(op.getLhs(), typecast);
		ConstraintResultCoral right = GenerateMatchingColumnExpression(op.getRhs(), typecast);
		if (left == null || right == null) {
			System.err
					.println("Cannot support complex arithmetic function now.");
			return null;
		} else {
			byte typeLhs = left.getType();//getType(op.getLhs());
			byte typeRhs = right.getType();//getType(op.getRhs());
			boolean isCLhs = left.isConst();
			boolean isCRhs = right.isConst();
			Object valueLhs = left.getConstVal();
			Object astLhs = left.getCoralast();
			Object valueRhs = right.getConstVal();
			Object astRhs = right.getCoralast();
			Object c = null;
			if ((typeLhs == DataType.INTEGER && typeRhs == DataType.INTEGER)) {
				//|| (typeLhs == DataType.LONG && typeRhs == DataType.LONG)
				if(isCLhs && !isCRhs) {
					c = alloc.minus(((Integer)valueLhs).intValue(), astRhs);
//					alloc.post(c);
				}
				else if(!isCLhs && isCRhs) {
					c = alloc.minus(astLhs, ((Integer)valueRhs).intValue());
//					alloc.post(c);
				}
				else {
					c = alloc.minus(astLhs, astRhs);
//					alloc.post(c);
				}
			} else if ((typeLhs == DataType.DOUBLE && typeRhs == DataType.DOUBLE)) {
				//(typeLhs == DataType.FLOAT && typeRhs == DataType.FLOAT)
				if(isCLhs && !isCRhs) {
					c = alloc.minus(((Double)valueLhs).doubleValue(), astRhs);
//					alloc.post(c);
				}
				else if(!isCLhs && isCRhs) {
					c = alloc.minus(astLhs, ((Double)valueRhs).doubleValue());
//					alloc.post(c);
				}
				else {
					c = alloc.minus(astLhs, astRhs);
//					alloc.post(c);
				}
			} else {
				check(false); // TODO
				return null;
			}
			ConstraintResultCoral ret = new ConstraintResultCoral(c, typeLhs, CntrTruth.UNKNOWN);
			return ret;
		}

	}

	ConstraintResultCoral GenerateMatchingTupleHelper(MultiplyExpression op, byte typecast)
			throws FrontendException, ExecException {
		ConstraintResultCoral left = GenerateMatchingColumnExpression(op.getLhs(), typecast);
		ConstraintResultCoral right = GenerateMatchingColumnExpression(op.getRhs(), typecast);
		if (left == null || right == null) {
			System.err
					.println("Cannot support complex arithmetic function now.");
			return null;
		} else {
			byte typeLhs = left.getType();//getType(op.getLhs());
			byte typeRhs = right.getType();//getType(op.getRhs());
			boolean isCLhs = left.isConst();
			boolean isCRhs = right.isConst();
			Object valueLhs = left.getConstVal();
			Object astLhs = left.getCoralast();
			Object valueRhs = right.getConstVal();
			Object astRhs = right.getCoralast();
			Object c = null;
			if ((typeLhs == DataType.INTEGER && typeRhs == DataType.INTEGER)) {
				//|| (typeLhs == DataType.LONG && typeRhs == DataType.LONG)
				if(isCLhs && !isCRhs) {
					c = alloc.mult(((Integer)valueLhs).intValue(), astRhs);
//					alloc.post(c);
				}
				else if(!isCLhs && isCRhs) {
					c = alloc.mult(astLhs, ((Integer)valueRhs).intValue());
//					alloc.post(c);
				}
				else {
					c = alloc.mult(astLhs, astRhs);
//					alloc.post(c);
				}
			} else if ((typeLhs == DataType.DOUBLE && typeRhs == DataType.DOUBLE)) {
				//(typeLhs == DataType.FLOAT && typeRhs == DataType.FLOAT)
				if(isCLhs && !isCRhs) {
					c = alloc.mult(((Double)valueLhs).doubleValue(), astRhs);
//					alloc.post(c);
				}
				else if(!isCLhs && isCRhs) {
					c = alloc.mult(astLhs, ((Double)valueRhs).doubleValue());
//					alloc.post(c);
				}
				else {
					c = alloc.mult(astLhs, astRhs);
//					alloc.post(c);
				}
			} else {
				check(false); // TODO
				return null;
			}
			ConstraintResultCoral ret = new ConstraintResultCoral(c, typeLhs, CntrTruth.UNKNOWN);
			return ret;
		}

	}

	ConstraintResultCoral GenerateMatchingTupleHelper(DivideExpression op, byte typecast)
			throws FrontendException, ExecException {
		ConstraintResultCoral left = GenerateMatchingColumnExpression(op.getLhs(), typecast);
		ConstraintResultCoral right = GenerateMatchingColumnExpression(op.getRhs(), typecast);
		if (left == null || right == null) {
			System.err
					.println("Cannot support complex arithmetic function now.");
			return null;
		} else {
			byte typeLhs = left.getType();//getType(op.getLhs());
			byte typeRhs = right.getType();//getType(op.getRhs());
			boolean isCLhs = left.isConst();
			boolean isCRhs = right.isConst();
			Object valueLhs = left.getConstVal();
			Object astLhs = left.getCoralast();
			Object valueRhs = right.getConstVal();
			Object astRhs = right.getCoralast();
			Object c = null;
			if ((typeLhs == DataType.INTEGER && typeRhs == DataType.INTEGER)) {
				//|| (typeLhs == DataType.LONG && typeRhs == DataType.LONG)
				if(isCLhs && !isCRhs) {
					c = alloc.div(((Integer)valueLhs).intValue(), astRhs);
//					alloc.post(c);
				}
				else if(!isCLhs && isCRhs) {
					c = alloc.div(astLhs, ((Integer)valueRhs).intValue());
//					alloc.post(c);
				}
				else {
					c = alloc.div(astLhs, astRhs);
//					alloc.post(c);
				}
			} else if ((typeLhs == DataType.DOUBLE && typeRhs == DataType.DOUBLE)) {
				//(typeLhs == DataType.FLOAT && typeRhs == DataType.FLOAT)
				if(isCLhs && !isCRhs) {
					c = alloc.div(((Double)valueLhs).doubleValue(), astRhs);
//					alloc.post(c);
				}
				else if(!isCLhs && isCRhs) {
					c = alloc.div(astLhs, ((Double)valueRhs).doubleValue());
//					alloc.post(c);
				}
				else {
					c = alloc.div(astLhs, astRhs);
//					alloc.post(c);
				}
			} else {
				check(false); // TODO
				return null;
			}
			ConstraintResultCoral ret = new ConstraintResultCoral(c, typeLhs, CntrTruth.UNKNOWN);
			return ret;
		}

	}

	// not supported
	ConstraintResultCoral GenerateMatchingTupleHelper(ModExpression op, byte typeCast)
			throws FrontendException, ExecException {
		return null;
	}
	
	ConstraintResultCoral GenerateMatchingTupleHelper(AddExpression op)
			throws FrontendException, ExecException {
		ConstraintResultCoral left = GenerateMatchingColumnExpression(op.getLhs());
		ConstraintResultCoral right = GenerateMatchingColumnExpression(op.getRhs());
		if (left == null || right == null) {
			System.err
					.println("Cannot support complex arithmetic function now.");
			return null;
		} else {
			byte typeLhs = left.getType();//getType(op.getLhs());
			byte typeRhs = right.getType();//getType(op.getRhs());
			boolean isCLhs = left.isConst();
			boolean isCRhs = right.isConst();
			Object valueLhs = left.getConstVal();
			Object astLhs = left.getCoralast();
			Object valueRhs = right.getConstVal();
			Object astRhs = right.getCoralast();
			Object c = null;
			if ((typeLhs == DataType.INTEGER && typeRhs == DataType.INTEGER)) {
				//|| (typeLhs == DataType.LONG && typeRhs == DataType.LONG)
				if(isCLhs && !isCRhs) {
					c = alloc.plus(((Integer)valueLhs).intValue(), astRhs);
//					alloc.post(c);
				}
				else if(!isCLhs && isCRhs) {
					c = alloc.plus(astLhs, ((Integer)valueRhs).intValue());
//					alloc.post(c);
				}
				else {
					c = alloc.plus(astLhs, astRhs);
//					alloc.post(c);
				}
			} else if ((typeLhs == DataType.DOUBLE && typeRhs == DataType.DOUBLE)) {
				//(typeLhs == DataType.FLOAT && typeRhs == DataType.FLOAT)
				if(isCLhs && !isCRhs) {
					c = alloc.plus(((Double)valueLhs).doubleValue(), astRhs);
//					alloc.post(c);
				}
				else if(!isCLhs && isCRhs) {
					c = alloc.plus(astLhs, ((Double)valueRhs).doubleValue());
//					alloc.post(c);
				}
				else {
					c = alloc.plus(astLhs, astRhs);
//					alloc.post(c);
				}
			} else {
				check(false); // TODO
				return null;
			}
			ConstraintResultCoral ret = new ConstraintResultCoral(c, typeLhs, CntrTruth.UNKNOWN);
			return ret;
		}
	}

	ConstraintResultCoral GenerateMatchingTupleHelper(SubtractExpression op)
			throws FrontendException, ExecException {
		ConstraintResultCoral left = GenerateMatchingColumnExpression(op.getLhs());
		ConstraintResultCoral right = GenerateMatchingColumnExpression(op.getRhs());
		if (left == null || right == null) {
			System.err
					.println("Cannot support complex arithmetic function now.");
			return null;
		} else {
			byte typeLhs = left.getType();//getType(op.getLhs());
			byte typeRhs = right.getType();//getType(op.getRhs());
			boolean isCLhs = left.isConst();
			boolean isCRhs = right.isConst();
			Object valueLhs = left.getConstVal();
			Object astLhs = left.getCoralast();
			Object valueRhs = right.getConstVal();
			Object astRhs = right.getCoralast();
			Object c = null;
			if ((typeLhs == DataType.INTEGER && typeRhs == DataType.INTEGER)) {
				//|| (typeLhs == DataType.LONG && typeRhs == DataType.LONG)
				if(isCLhs && !isCRhs) {
					c = alloc.minus(((Integer)valueLhs).intValue(), astRhs);
//					alloc.post(c);
				}
				else if(!isCLhs && isCRhs) {
					c = alloc.minus(astLhs, ((Integer)valueRhs).intValue());
//					alloc.post(c);
				}
				else {
					c = alloc.minus(astLhs, astRhs);
//					alloc.post(c);
				}
			} else if ((typeLhs == DataType.DOUBLE && typeRhs == DataType.DOUBLE)) {
				//(typeLhs == DataType.FLOAT && typeRhs == DataType.FLOAT)
				if(isCLhs && !isCRhs) {
					c = alloc.minus(((Double)valueLhs).doubleValue(), astRhs);
//					alloc.post(c);
				}
				else if(!isCLhs && isCRhs) {
					c = alloc.minus(astLhs, ((Double)valueRhs).doubleValue());
//					alloc.post(c);
				}
				else {
					c = alloc.minus(astLhs, astRhs);
//					alloc.post(c);
				}
			} else {
				check(false); // TODO
				return null;
			}
			ConstraintResultCoral ret = new ConstraintResultCoral(c, typeLhs, CntrTruth.UNKNOWN);
			return ret;
		}

	}

	ConstraintResultCoral GenerateMatchingTupleHelper(MultiplyExpression op)
			throws FrontendException, ExecException {
		ConstraintResultCoral left = GenerateMatchingColumnExpression(op.getLhs());
		ConstraintResultCoral right = GenerateMatchingColumnExpression(op.getRhs());
		if (left == null || right == null) {
			System.err
					.println("Cannot support complex arithmetic function now.");
			return null;
		} else {
			byte typeLhs = left.getType();//getType(op.getLhs());
			byte typeRhs = right.getType();//getType(op.getRhs());
			boolean isCLhs = left.isConst();
			boolean isCRhs = right.isConst();
			Object valueLhs = left.getConstVal();
			Object astLhs = left.getCoralast();
			Object valueRhs = right.getConstVal();
			Object astRhs = right.getCoralast();
			Object c = null;
			if ((typeLhs == DataType.INTEGER && typeRhs == DataType.INTEGER)) {
				//|| (typeLhs == DataType.LONG && typeRhs == DataType.LONG)
				if(isCLhs && !isCRhs) {
					c = alloc.mult(((Integer)valueLhs).intValue(), astRhs);
//					alloc.post(c);
				}
				else if(!isCLhs && isCRhs) {
					c = alloc.mult(astLhs, ((Integer)valueRhs).intValue());
//					alloc.post(c);
				}
				else {
					c = alloc.mult(astLhs, astRhs);
//					alloc.post(c);
				}
			} else if ((typeLhs == DataType.DOUBLE && typeRhs == DataType.DOUBLE)) {
				//(typeLhs == DataType.FLOAT && typeRhs == DataType.FLOAT)
				if(isCLhs && !isCRhs) {
					c = alloc.mult(((Double)valueLhs).doubleValue(), astRhs);
//					alloc.post(c);
				}
				else if(!isCLhs && isCRhs) {
					c = alloc.mult(astLhs, ((Double)valueRhs).doubleValue());
//					alloc.post(c);
				}
				else {
					c = alloc.mult(astLhs, astRhs);
//					alloc.post(c);
				}
			} else {
				check(false); // TODO
				return null;
			}
			ConstraintResultCoral ret = new ConstraintResultCoral(c, typeLhs, CntrTruth.UNKNOWN);
			return ret;
		}

	}

	ConstraintResultCoral GenerateMatchingTupleHelper(DivideExpression op)
			throws FrontendException, ExecException {
		ConstraintResultCoral left = GenerateMatchingColumnExpression(op.getLhs());
		ConstraintResultCoral right = GenerateMatchingColumnExpression(op.getRhs());
		if (left == null || right == null) {
			System.err
					.println("Cannot support complex arithmetic function now.");
			return null;
		} else {
			byte typeLhs = left.getType();//getType(op.getLhs());
			byte typeRhs = right.getType();//getType(op.getRhs());
			boolean isCLhs = left.isConst();
			boolean isCRhs = right.isConst();
			Object valueLhs = left.getConstVal();
			Object astLhs = left.getCoralast();
			Object valueRhs = right.getConstVal();
			Object astRhs = right.getCoralast();
			Object c = null;
			if ((typeLhs == DataType.INTEGER && typeRhs == DataType.INTEGER)) {
				//|| (typeLhs == DataType.LONG && typeRhs == DataType.LONG)
				if(isCLhs && !isCRhs) {
					c = alloc.div(((Integer)valueLhs).intValue(), astRhs);
//					alloc.post(c);
				}
				else if(!isCLhs && isCRhs) {
					c = alloc.div(astLhs, ((Integer)valueRhs).intValue());
//					alloc.post(c);
				}
				else {
					c = alloc.div(astLhs, astRhs);
//					alloc.post(c);
				}
			} else if ((typeLhs == DataType.DOUBLE && typeRhs == DataType.DOUBLE)) {
				//(typeLhs == DataType.FLOAT && typeRhs == DataType.FLOAT)
				if(isCLhs && !isCRhs) {
					c = alloc.div(((Double)valueLhs).doubleValue(), astRhs);
//					alloc.post(c);
				}
				else if(!isCLhs && isCRhs) {
					c = alloc.div(astLhs, ((Double)valueRhs).doubleValue());
//					alloc.post(c);
				}
				else {
					c = alloc.div(astLhs, astRhs);
//					alloc.post(c);
				}
			} else {
				check(false); // TODO
				return null;
			}
			ConstraintResultCoral ret = new ConstraintResultCoral(c, typeLhs, CntrTruth.UNKNOWN);
			return ret;
		}

	}

	//not supported
	ConstraintResultCoral GenerateMatchingTupleHelper(ModExpression op)
			throws FrontendException, ExecException {
		return null;

	}

	ConstraintResultCoral GenerateMatchingTupleHelper(RegexExpression pred,
			boolean invert) throws FrontendException, ExecException {
		// now we are sure that the expression operators are the roots of the
		// plan

		Object rightConst = null;
		int leftCol = -1;
		
		ConstraintResultCoral ret = new ConstraintResultCoral();

		LogicalExpression lhs = pred.getLhs();
		if (lhs instanceof CastExpression)
			lhs = ((CastExpression) lhs).getExpression();
		// if (!(pred.getLhsOperand() instanceof ProjectExpression &&
		// ((ProjectExpression)
		// pred
		// .getLhsOperand()).getProjection().size() == 1))
		// return; // too hard
		if (!(lhs instanceof ProjectExpression))
			return ret;
		//leftCol = ((ProjectExpression) lhs).getColNum();
//		LogicalFieldSchema lfs = schema.getFieldSubNameMatch(((ProjectExpression)lhs).getFieldSchema().alias);
//		if ( lfs != null) {
//			leftCol = schema.getFields().indexOf(lfs);
//  		} else
//  			return ret;
		
		leftCol = extr.extractPosition(schema, ((ProjectExpression)lhs).getFieldSchema().alias);
		if(leftCol == -1)
			return ret;
		
		if (!(pred.getRhs() instanceof ConstantExpression))
			return ret;

		rightConst = ((ConstantExpression) (pred.getRhs())).getValue();

		
		Xeger generator = new Xeger((String) rightConst);
		String text = generator.generate();
		// now we try to change some nulls to constants
		if (invert)
			tOut.set(leftCol, text + "edu.umass.cs.pig");
		else
			tOut.set(leftCol, text);
		ret = new ConstraintResultCoral(CntrTruth.toCntrTruth(invert));
		return ret;
	}

	ConstraintResultCoral GenerateMatchingTupleHelper(NotExpression op, boolean invert)
			throws FrontendException, ExecException {
		LogicalExpression input = op.getExpression();
		ConstraintResultCoral ret = GenerateMatchingTupleHelper(input, !invert);
		return ret;
	}

	/*
	 * op.getExpression(): x:(Name: Project Type: chararray Uid: 5 Input: 0 Column: 0)
	 * op.getExpression().fieldSchema: x#5:chararray
	 */
	void GenerateMatchingTupleHelper(IsNullExpression op,
			boolean invert) throws FrontendException, ExecException {
		int lcol = extr.extractPosition(schema, op.getExpression().getFieldSchema().alias);
		if (!invert)
			nullPos.add(lcol);
//			tOut.set(lcol, null);
//		else {
//			byte type = op.getExpression().getType();
//			tOut.set(lcol, generateData(type, "0"));
//		}
	}
	
	Object generateData(byte type, String data) {
        switch (type) {
        case DataType.BOOLEAN:
            if (data.equalsIgnoreCase("true")) {
                return Boolean.TRUE;
            } else if (data.equalsIgnoreCase("false")) {
                return Boolean.FALSE;
            } else {
                return null;
            }
        case DataType.BYTEARRAY:
            return new DataByteArray(data.getBytes());
        case DataType.DOUBLE:
            return Double.valueOf(data);
        case DataType.FLOAT:
            return Float.valueOf(data);
        case DataType.INTEGER:
            return Integer.valueOf(data);
        case DataType.LONG:
            return Long.valueOf(data);
        case DataType.CHARARRAY:
            return data;
        default:
            return null;
        }
    }

	
	ConstraintResultCoral GenerateMatchingTupleHelper(UserFuncExpression op, boolean invert)
			throws FrontendException, ExecException {
		Object udf = null;
		FuncSpec userFunc = op.getFuncSpec();
		List<LogicalExpression> args = op.getArguments();
		List<ConstraintResultCoral> crargs = new ArrayList<ConstraintResultCoral>(); 
		for(LogicalExpression arg: args) {
			if(arg.getFieldSchema().type != DataType.DOUBLE) {
				if(arg instanceof ProjectExpression)
					crargs.add(mallocInCoral((ProjectExpression)arg, (byte)-1));
			}
			else
				throw new RuntimeException("not support non-double arguments inside udf");
		}
		if(userFunc.getClassName().toLowerCase().endsWith("pow")) {
			ConstraintResultCoral first = crargs.get(0);
			ConstraintResultCoral second = crargs.get(1);
			boolean isCLhs = first.isConst();
			boolean isCRhs = second.isConst();
			Double valueLhs = (Double)first.getConstVal();
			Object astLhs = first.getCoralast();
			Double valueRhs = (Double)second.getConstVal();
			Object astRhs = second.getCoralast();
			if(isCLhs && !isCRhs) {
				udf = alloc.power(valueLhs.doubleValue(), astRhs);
				alloc.post(udf);
			}
			else if(!isCLhs && isCRhs) {
				udf = alloc.power(astLhs, valueRhs.doubleValue());
				alloc.post(udf);
			}
			else if(!isCLhs && !isCRhs) {
				udf = alloc.power(astLhs, astRhs);
				alloc.post(udf);
			}
		}
		
		byte retT = op.getFieldSchema().type;
		
		ConstraintResultCoral ret = new ConstraintResultCoral(udf, retT, CntrTruth.UNKNOWN);
		
		return ret;
	}
	
	ConstraintResultCoral GenerateMatchingTupleHelper(UserFuncExpression op, byte typecast)
			throws FrontendException, ExecException {
		Object udf = null;
		FuncSpec userFunc = op.getFuncSpec();
		List<LogicalExpression> args = op.getArguments();
		List<ConstraintResultCoral> crargs = new ArrayList<ConstraintResultCoral>(); 
		for(LogicalExpression arg: args) {
			if(arg.getFieldSchema().type == DataType.DOUBLE) {
				if(arg instanceof ProjectExpression)
					crargs.add(mallocInCoral((ProjectExpression)arg, (byte)-1));
				else if(arg instanceof ConstantExpression)
					crargs.add(mallocInCoral((ConstantExpression)arg, (byte)-1));
			}
			else
				throw new RuntimeException("not support non-double arguments inside udf");
		}
		if(userFunc.getClassName().toLowerCase().endsWith("pow")) {
			ConstraintResultCoral first = crargs.get(0);
			ConstraintResultCoral second = crargs.get(1);
			boolean isCLhs = first.isConst();
			boolean isCRhs = second.isConst();
			Double valueLhs = (Double)first.getConstVal();
			Object astLhs = first.getCoralast();
			Double valueRhs = (Double)second.getConstVal();
			Object astRhs = second.getCoralast();
			if(isCLhs && !isCRhs) {
				udf = alloc.power(valueLhs.doubleValue(), astRhs);
//				alloc.post(udf);
			}
			else if(!isCLhs && isCRhs) {
				udf = alloc.power(astLhs, valueRhs.doubleValue());
//				alloc.post(udf);
			}
			else if(!isCLhs && !isCRhs) {
				udf = alloc.power(astLhs, astRhs);
//				alloc.post(udf);
			}
		}
		
		byte retT = op.getFieldSchema().type;
		
		ConstraintResultCoral ret = new ConstraintResultCoral(udf, retT, CntrTruth.UNKNOWN);
		
		return ret;
	}
	
	Object GetUnequalValue(Object v) {
        byte type = DataType.findType(v);

        if (type == DataType.BAG || type == DataType.TUPLE
                || type == DataType.MAP)
            return null;

        Object zero = generateData(type, "0");

        if (v.equals(zero))
            return generateData(type, "1");

        return zero;
    }

     Object GetSmallerValue(Object v) {
        byte type = DataType.findType(v);

        if (type == DataType.BAG || type == DataType.TUPLE
                || type == DataType.MAP)
            return null;

        switch (type) {
        case DataType.CHARARRAY:
            String str = (String) v;
            if (str.length() > 0)
                return str.substring(0, str.length() - 1);
            else
                return null;
        case DataType.BYTEARRAY:
            DataByteArray data = (DataByteArray) v;
            if (data.size() > 0)
                return new DataByteArray(data.get(), 0, data.size() - 1);
            else
                return null;
        case DataType.INTEGER:
            return Integer.valueOf((Integer) v - 1);
        case DataType.LONG:
            return Long.valueOf((Long) v - 1);
        case DataType.FLOAT:
            return Float.valueOf((Float) v - 1);
        case DataType.DOUBLE:
            return Double.valueOf((Double) v - 1);
        default:
            return null;
        }

    }

     Object GetLargerValue(Object v) {
        byte type = DataType.findType(v);

        if (type == DataType.BAG || type == DataType.TUPLE
                || type == DataType.MAP)
            return null;

        switch (type) {
        case DataType.CHARARRAY:
            return (String) v + "0";
        case DataType.BYTEARRAY:
            String str = ((DataByteArray) v).toString();
            str = str + "0";
            return new DataByteArray(str);
        case DataType.INTEGER:
            return Integer.valueOf((Integer) v + 1);
        case DataType.LONG:
            return Long.valueOf((Long) v + 1);
        case DataType.FLOAT:
            return Float.valueOf((Float) v + 1);
        case DataType.DOUBLE:
            return Double.valueOf((Double) v + 1);
        default:
            return null;
        }
    }
     
     /**
      * not supported now.
      */
     ConstraintResult GenerateRlTuple(PltRelCntr ctr) throws FrontendException, ExecException {
    	 return null;
     }

	public List<Integer> getNullPos() {
		return nullPos;
	}

	public void setNullPos(List<Integer> nullPos) {
		this.nullPos = nullPos;
	}

	public CAndCCoral getCccoral() {
		return cccoral;
	}
	
	
}
