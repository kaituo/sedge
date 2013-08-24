package edu.umass.cs.userfunc;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.pig.FuncSpec;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BinSedesTuple;
import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.logical.expression.ConstantExpression;
import org.apache.pig.newplan.logical.expression.LogicalExpression;
import org.apache.pig.newplan.logical.expression.ProjectExpression;
import org.apache.pig.newplan.logical.expression.UserFuncExpression;
import org.apache.pig.newplan.logical.relational.LogicalSchema.LogicalFieldSchema;


import edu.umass.cs.pig.sort.BitVector32Sort;
import edu.umass.cs.pig.sort.BitVector64Sort;
import edu.umass.cs.pig.sort.Fp32Sort;
import edu.umass.cs.pig.sort.Fp64Sort;
import edu.umass.cs.pig.sort.Z3BoolSort;
import edu.umass.cs.pig.util.Allocator4Constant;
import edu.umass.cs.pig.z3.Z3Context;
import edu.umass.cs.z3.AstArray;
import edu.umass.cs.z3.SWIGTYPE_p__Z3_ast;
import edu.umass.cs.z3.SWIGTYPE_p__Z3_func_decl;
import edu.umass.cs.z3.SWIGTYPE_p__Z3_sort;

/**
 * TODO:
 * Support for String:
 * 1) use uninterpreted function:
 * http://stackoverflow.com/questions/7126839/can-z3-be-used-to-reason-about-substrings
 * 
 * Q:

	I am trying to use Z3 to reason about substrings, and have come across some non-intuitive behavior. Z3 returns 'sat' when asked to determine if 'xy' appears within 'xy', but it returns 'unknown' when asked if 'x' is in 'x', or 'x' is in 'xy'.
	
	I've commented the following code to illustrate this behavior:
	
	(set-logic AUFLIA)
	(declare-sort Char 0)
	
	;characters to build strings are _x_ and _y_
	(declare-fun _x_ () Char)
	(declare-fun _y_ () Char)
	(assert (distinct _x_ _y_))
	
	;string literals
	(declare-fun findMeX () (Array Int Char))  
	(declare-fun findMeXY () (Array Int Char))  
	(declare-fun x () (Array Int Char))
	(declare-fun xy () (Array Int Char))
	(declare-fun length ( (Array Int Char) ) Int )
	
	;set findMeX = 'x'
	(assert (= (select findMeX 0) _x_))
	(assert (= (length findMeX) 1))
	
	;set findMeXY = 'xy'
	(assert (= (select findMeXY 0) _x_))
	(assert (= (select findMeXY 1) _y_))
	(assert (= (length findMeXY) 2))
	
	;set x = 'x'
	(assert (= (select x 0) _x_))
	(assert (= (length x) 1))
	
	;set xy = 'xy'
	(assert (= (select xy 0) _x_))
	(assert (= (select xy 1) _y_))
	(assert (= (length xy) 2))
	
	Now that the problem is set up, we try to find the substrings:
	
	;search for findMeX='x' in x='x' 
	
	(push 1)
	(assert 
	  (exists 
	    ((offset Int)) 
	    (and 
	      (<= offset (- (length x) (length findMeX))) 
	      (>= offset 0) 
	      (forall 
	        ((index Int)) 
	        (=> 
	          (and 
	            (< index (length findMeX)) 
	            (>= index 0)) 
	          (= 
	            (select x (+ index offset)) 
	            (select findMeX index)))))))
	
	(check-sat) ;'sat' expected, 'unknown' returned
	(pop 1)
	
	If we instead search for findMeXY in xy, the solver returns 'sat', as expected. It would seem that since the solver can handle this query for 'xy', it should be able to handle it for 'x'. Further, if searching for findMeX='x' in 'xy='xy', it returns 'unknown'.
	
	Can anyone suggest an explanation, or perhaps an alternate model for expressing this problem within a SMT solver?
	
 * 	A:
 * The short answer for the observed behavior is: Z3 returns ‘unknown’ because your assertions contain quantifiers.
	
	Z3 contains many procedures and heuristics for handling quantifiers. Z3 uses a technique called Model-Based Quantifier Instantiation (MBQI) for building models for satisfying queries like yours. The first step is this procedure consists of creating a candidate interpretation based on an interpretation that satisfies the quantifier free assertions. Unfortunately, in the current Z3, this step does not interact smoothly with the array theory. The basic problem is that the interpretation built by the array theory cannot be modified by this module.
	
	A fair question is: why does it work when we remove the push/pop commands? It works because Z3 uses more aggressive simplification (preprocessing) steps when incremental solving commands (such as push and pop commands) are not used.
	
	I see two possible workarounds for your problem.
	
	    You can avoid quantifiers, and keep using array theory. This is feasible in your example, since you know the length of all “strings”. Thus, you can expand the quantifier. Although, this approach may seem awkward, it is used in practice and in many verification and testing tools.
	
	    You can avoid array theory. You declare string as an uninterpreted sort, like you did for Char. Then, you declare a function char-of that is supposed to return the i-th character of a string. You can axiomatize this operation. For example, you may say that two strings are equal if they have the same length and contain the same characters:
	
		
		(declare-sort Char 0)
		(declare-sort String 0)
		
		;characters to build strings are _x_ and _y_
		(declare-fun _x_ () Char)
		(declare-fun _y_ () Char)
		(assert (distinct _x_ _y_))
		
		;string literals
		(declare-fun findMeX () String)
		(declare-fun findMeXY () String)
		(declare-fun x () String)
		(declare-fun xy () String)
		(declare-fun char-of (String Int) Char)
		(declare-fun length (String) Int)
		
		;extensionality
		(assert (forall ((s1 String) (s2 String))
		                (=> (and 
		                     (= (length s1) (length s2))
		                     (forall ((i Int))
		                             (=> (and (<= 0 i) (< i (length s1)))
		                                 (= (char-of s1 i) (char-of s2 i)))))
		                    (= s1 s2))))
		                    
		;set findMeX = 'x'
		(assert (= (char-of findMeX 0) _x_))
		(assert (= (length findMeX) 1))
		
		;set findMeXY = 'xy'
		(assert (= (char-of findMeXY 0) _x_))
		(assert (= (char-of findMeXY 1) _y_))
		(assert (= (length findMeXY) 2))
		
		;set x = 'x'
		(assert (= (char-of x 0) _x_))
		(assert (= (length x) 1))
		
		;set xy = 'xy'
		(assert (= (char-of xy 0) _x_))
		(assert (= (char-of xy 1) _y_))
		(assert (= (length xy) 2))
		
		(push 1)
		(assert 
		  (exists 
		    ((offset Int)) 
		    (and 
		      (<= offset (- (length x) (length findMeX))) 
		      (>= offset 0) 
		      (forall 
		        ((index Int)) 
		        (=> 
		          (and 
		            (< index (length findMeX)) 
		            (>= index 0)) 
		          (= 
		            (char-of x (+ index offset)) 
		            (char-of findMeX index)))))))
		
		(check-sat) 
		(pop 1)
	
	The following link contains your script encoded using the second approach: http://rise4fun.com/Z3/yD3
	
	The second approach is more attractive, and will allow you to prove more complicated properties about strings. However, it is very easy to write satisfiable quantified formulas that Z3 will not be able to build a model. The Z3 Guide describes the main capabilities and limitations of the MBQI module. It contains may decidable fragments that can be handled by Z3. BTW, note that dropping array theory will not be a big problem if you have quantifiers. The guide shows how to encode arrays using quantifiers and functions.
	
	You can find more information about how MBQI works in the following papers:
	
	    Complete instantiation for quantified SMT formulas
	
	    Efficiently Solving Quantified Bit-Vector Formula

 * 2) write my own or use other constraint solver like hampi.
 * http://people.csail.mit.edu/akiezun/hampi/
 * 
 * Status: right now, i only support string constraints that doesn't have any relationshp with other string or non-string constraints;
 * that is, my string constraints are independent.
 * @author kaituo
 *
 */
public class UninterpretedMaker {
	protected final Z3Context z3; 
	protected List<InputOutput> concreteValues;
	protected FuncSpec userFunc;
	protected byte retType;
	protected Allocator4Constant alloc;
	protected UserFuncExpression udfOp;
	protected static char x = 'a';
	
	public UninterpretedMaker(List<InputOutput> concreteVals, UserFuncExpression op,
			 byte retT) {
		z3 = Z3Context.get();
		concreteValues = concreteVals;
		userFunc = op.getFuncSpec();
		retType = retT;
		alloc = new Allocator4Constant();
		udfOp = op;
	}
	
	private List<String> toArgs(Object inp) {
		List<String> args = new ArrayList<String>();
		if(inp instanceof BinSedesTuple) {
			BinSedesTuple inputs = (BinSedesTuple)inp;
			int s = inputs.size();
			for(int i = 0; i < s; i++) {
				try {
					String arg = inputs.get(i).toString();
					args.add(arg);
				} catch (ExecException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
			}
		}
		if(args.isEmpty())
			return null;
		else
			return args;
	}
	
	private List<Object> toObjectArgs(Object inp) {
		List<Object> args = new ArrayList<Object>();
		if(inp instanceof BinSedesTuple) {
			BinSedesTuple inputs = (BinSedesTuple)inp;
			int s = inputs.size();
			for(int i = 0; i < s; i++) {
				try {
					Object arg = inputs.get(i);
					args.add(arg);
				} catch (ExecException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
			}
		}
		if(args.isEmpty())
			return null;
		else
			return args;
	}
	
	
	private Map<List<String>, String> toArgsRet() {
		Map<List<String>, String> args2ret = new HashMap<List<String>, String>();
		for(InputOutput iout: concreteValues) {
			Object inputs = iout.input.result;
			Object output = iout.output.result;
			List<String> args = toArgs(inputs);
			String ret = output.toString();
			if(args != null || ret != null)
				args2ret.put(args, ret);
		}
		return args2ret;
		
	}
	
	private Map<List<Object>, Object> toObjectArgsRet() {
		Map<List<Object>, Object> args2ret = new HashMap<List<Object>, Object>();
		for(InputOutput iout: concreteValues) {
			Object inputs = iout.input.result;
			Object output = iout.output.result;
			List<Object> args = toObjectArgs(inputs);
//			String ret = output.toString();
			if(args != null || output != null)
				args2ret.put(args, output);
		}
		return args2ret;
		
	}
	
	public String toValueString() {
		Map<List<String>, String> arg2RetMap = toArgsRet();
		String ret = "";
		for (Entry<List<String>, String> entry : arg2RetMap.entrySet()) {
			List<String> inputAlias = entry.getKey();
			String t = entry.getValue();
			for (String inp : inputAlias) {
				ret = ret + " " + inp;
			}
			ret = ret + "\n";
			ret = ret + t + "\n";
		}
		return ret;
	}
	
	private SWIGTYPE_p__Z3_sort[] mk_arg_sort() {
		Schema s = userFunc.getInputArgsSchema();
//		if(s != null) {
		List<FieldSchema> fsFields = s.getFields();
		int size = fsFields.size();
		SWIGTYPE_p__Z3_sort[] arg_sorts = new SWIGTYPE_p__Z3_sort[size];
		for(int i=0;i<size;i++){
			FieldSchema sFS = fsFields.get(i);
            arg_sorts[i] = mk_Sort(sFS.type);
		}
		return arg_sorts;
//		} else {
//			try {
//				LogicalFieldSchema argSchema = udfOp.getFieldSchema();
//				SWIGTYPE_p__Z3_sort[] arg_sorts = new SWIGTYPE_p__Z3_sort[1];
//				
//		        arg_sorts[0] = mk_Sort(argSchema.type);
//				
//				return arg_sorts;
//			} catch (FrontendException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			} 
//		}
//		return null;

	}
	
	private byte[] mk_arg_types() {
		Schema s = userFunc.getInputArgsSchema();
		List<FieldSchema> fsFields = s.getFields();
		int size = fsFields.size();
		byte[] arg_types = new byte[size];
		for(int i=0;i<size;i++){
			FieldSchema sFS = fsFields.get(i);
            arg_types[i] = sFS.type;
		}
		return arg_types;
	}
	
	private String[] mk_arg_alias() {
		Schema s = userFunc.getInputArgsSchema();
		List<FieldSchema> fsFields = s.getFields();
		int size = fsFields.size();
		String[] arg_alias = new String[size];
		for(int i=0; i < size; i++){
			FieldSchema sFS = fsFields.get(i);
            arg_alias[i] = sFS.alias;
		}
		return arg_alias;
	}
	
	
	private SWIGTYPE_p__Z3_sort mk_Sort(byte type) {
		switch (type) {
		case DataType.INTEGER:
			SWIGTYPE_p__Z3_sort intT = BitVector32Sort.getInstance().getZ3Sort();
			return intT;
		case DataType.LONG:
			SWIGTYPE_p__Z3_sort int64T = BitVector64Sort.getInstance().getZ3Sort();
			return int64T;
		case DataType.FLOAT:
			SWIGTYPE_p__Z3_sort floatT = Fp32Sort.getInstance().getZ3Sort();
			return floatT;
		case DataType.DOUBLE:
			SWIGTYPE_p__Z3_sort doubleT = Fp64Sort.getInstance().getZ3Sort();
			return doubleT;
		case DataType.CHARARRAY:
			// doesn't support string arithmetic now
			return null;
		case DataType.BOOLEAN:
			SWIGTYPE_p__Z3_sort boolT = Z3BoolSort.getInstance().getZ3Sort();
			return boolT;
		default:
			return null;
		}
	}
	
	private String[] getNextConstantAliases(int len) {
		char y = (char)(x++); 
		String[] ret = new String[len];
		for(int i=0; i<len; i++)
			ret[i] = y + "!" + i;
		return ret;
	}
	
	private String getNextConstantAlias() {
		char y = (char)(x++); 
		String ret = y + "!" + 1;
		return ret;
	}
	
	private String[] getNextAliases(int len) throws FrontendException {
		OperatorPlan pl = udfOp.getPlan();
		List<Operator> inputs = pl.getSuccessors(udfOp);
		List<String> ret = new ArrayList<String>();
        if( inputs != null ) {
            for( Operator op : inputs ) {
                LogicalExpression input = (LogicalExpression)op;
                if(input instanceof ProjectExpression) {
                	ProjectExpression prj = (ProjectExpression)input;
                	String alias = prj.getFieldSchema().alias;
                	ret.add(alias);
                } else if(input instanceof ConstantExpression) {
                	String alias = getNextConstantAlias();
                	ret.add(alias);
                }
                	
            }
        }
		
		return ret.toArray(new String[ret.size()]);
	}
	
	/**
	 * TODO: need to deal with different input types like z3.mk_real
	 * @return
	 * @throws FrontendException 
	 */
	public SWIGTYPE_p__Z3_ast toUninterpFunc() throws FrontendException {
		SWIGTYPE_p__Z3_sort[] domain = mk_arg_sort();
		SWIGTYPE_p__Z3_sort range = mk_Sort(retType);
		
        SWIGTYPE_p__Z3_func_decl FPolicy = z3.mk_func_decl2(z3.mk_string_symbol(userFunc.getClassName()), range, domain);  
        
        
 //       Map<List<String>, String> arg2RetMap = toArgsRet();
        Map<List<Object>, Object> arg2RetMap = toObjectArgsRet();
		int sizeD = domain.length;
		int sizeC = arg2RetMap.size();
		SWIGTYPE_p__Z3_ast[][] args = new SWIGTYPE_p__Z3_ast[sizeC][sizeD];
        SWIGTYPE_p__Z3_ast[] fS = new SWIGTYPE_p__Z3_ast[sizeC]; 
        SWIGTYPE_p__Z3_ast f;
        byte[] domainDataType = mk_arg_types();
		int j = 0;
		for (Entry<List<Object>, Object> entry : arg2RetMap.entrySet()) {
			
			List<Object> inputAlias = entry.getKey();
			Object t = entry.getValue();
			int i = 0;
			for (Object inp : inputAlias) {
				args[j][i] = alloc.mallocInZ3(inp, domainDataType[i]).getZ3ast();//z3.mk_numeral(inp, domain[i]);
				i++;
			}
			fS[j] =  alloc.mallocInZ3(t, retType).getZ3ast();//z3.mk_numeral(t, range);
			j++;
		}
//		for (Entry<List<String>, String> entry : arg2RetMap.entrySet()) {
//			
//			List<String> inputAlias = entry.getKey();
//			String t = entry.getValue();
//			int i = 0;
//			for (String inp : inputAlias) {
//				args[j][i] = alloc.mallocInZ3(inp, domainDataType[i]).getZ3ast();//z3.mk_numeral(inp, domain[i]);
//				i++;
//			}
//			fS[j] =  alloc.mallocInZ3(t, retType).getZ3ast();//z3.mk_numeral(t, range);
//			j++;
//		}
        f = alloc.mallocUnknownInZ3(retType).getZ3ast();//z3.mk_numeral("2147483640", range);
		
		String[] alias = getNextAliases(sizeD); //mk_arg_alias();
		
		if(alias.length != sizeD)
			throw new FrontendException("Cannot get the whole alias of udf arguments!");
		
		SWIGTYPE_p__Z3_ast[] func = new SWIGTYPE_p__Z3_ast[sizeD];
		for(int k = 0; k < sizeD; k++) {
			func[k] = z3.mk_var(alias[k], domain[k]);
		}
        
        SWIGTYPE_p__Z3_ast fun = z3.mk_app(FPolicy, func);
        SWIGTYPE_p__Z3_ast[][] conditionS = new SWIGTYPE_p__Z3_ast[sizeC][sizeD];
        SWIGTYPE_p__Z3_ast[] condition = new SWIGTYPE_p__Z3_ast[sizeC];
        SWIGTYPE_p__Z3_ast[] ite = new SWIGTYPE_p__Z3_ast[sizeC];
        
        for(int ii = 0; ii < sizeC; ii++) {
        	for(int jj = 0; jj < sizeD; jj++) {
        		conditionS[ii][jj] = z3.mk_eq(func[jj], args[ii][jj]);
        	}
        }
        
        AstArray andArgs[] = new AstArray[sizeC];
        
        for(int ii2 = 0; ii2 < sizeC; ii2++) {
        	andArgs[ii2] = new AstArray(sizeD);
        	for(int jj2 = 0; jj2 < sizeD; jj2++) {
        		andArgs[ii2].setitem(jj2, conditionS[ii2][jj2]);
        	}
        }
        		
        for(int ii3 = 0; ii3 < sizeC; ii3++) {
        	condition[ii3] = z3.mk_and(sizeD, andArgs[ii3].cast());
        }
        
        int indexLast = sizeC-1;
        ite[indexLast] = z3.mk_ite(condition[indexLast], z3.mk_eq(fun, fS[indexLast]), z3.mk_eq(fun, f));
        for(int ii4 = sizeC-2; ii4 >=0; ii4--) {
        	ite[ii4] = z3.mk_ite(condition[ii4], z3.mk_eq(fun, fS[ii4]), ite[ii4+1]);
        }
        
//        System.out.println("term : " + z3.ast_to_string(ite[0]));
        
        z3.assert_cnstr(ite[0]);
        // retType is not of type bool or chararray
        if(f != null) {
        	SWIGTYPE_p__Z3_ast cntr2 = z3.mk_not(z3.mk_eq(fun, f));
        	z3.assert_cnstr(cntr2);
        }
        return fun;
	}

}
