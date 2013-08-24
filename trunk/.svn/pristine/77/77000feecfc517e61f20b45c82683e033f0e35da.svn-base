package edu.umass.cs.pig.z3.test;

import edu.umass.cs.z3.SWIGTYPE_p__Z3_ast;
import edu.umass.cs.z3.SWIGTYPE_p__Z3_sort;
import edu.umass.cs.z3.SortArray;
import edu.umass.cs.z3.SortArrayTheirs;

/**
 * http://stackoverflow.com/questions/8734976/about-the-representation-of-the-uninterpreted-function-in-z3
 * Q: I have a quick question. I wrote a simple program (using Z3 NET API) and got the output as follows.

	Program (Partial):

        Sort[] domain = new Sort[3];
        domain[0] = intT;  
        domain[1] = intT;          
        domain[2] = intT;  
        FPolicy = z3.MkFuncDecl("FPolicy", domain, boolT);      

        Term[] args = new Term[3];
        args[0] = z3.MkNumeral(0, intT);
        args[1] = z3.MkNumeral(1, intT);
        args[2] = z3.MkNumeral(30, intT);
        z3.AssertCnstr(z3.MkApp(FPolicy, args));

        args[1] = z3.MkNumeral(2, intT);
        args[2] = z3.MkNumeral(20, intT);
        z3.AssertCnstr(z3.MkApp(FPolicy, args));

		Output:
		
		FPolicy -> {
		  0 1 30 -> true
		  0 2 20 -> true     
		  else -> true
		}
		
		I am wondering could I make the "else -> true" as false (i.e., "else -> false"). 
 
	 * mhs's Answer:
	   How about the following (RiSE4fun link)?
		
		(set-option :MBQI true)
		
		(declare-fun FPolicy (Int Int Int) Bool)
		
		(assert (forall ((x1 Int) (x2 Int) (x3 Int)) (!
		  (implies
		    (not
		      (or
		        (and (= x1 0) (= x2 1) (= x3 30))
		        (and (= x1 0) (= x2 2) (= x3 20))))
		      (= (FPolicy x1 x2 x3) false))
		  :pattern (FPolicy x1 x2 x3))))
		
		(assert (FPolicy 0 1 30))
		(assert (FPolicy 0 2 20))
		
		(check-sat)
		(get-model)
		
		The advantage I can see is that you can change it such that FPolicy(0 1 30) == false without touching the forall-constraint. The obvious disadvantage is that you basically have to mention all argument tuples twice, and that the created model is rather convoluted.
		
		I am looking forward to seeing better solutions :-)
		
		
	  *	Leonardo de Moura's answer:
	    For quantifier free problems, Z3 (3.2) will select for the else the value that occurs more often in the range. By range here, I mean the finite set of values that Z3 assigned to a particular finite set of input values. In our example, only true occurs in the range. Thus, true is selected as the else value.

		For quantifier free (and array free) problems, if the option :model-compact true is not used, then the value of the else doesn’t matter. That is, if the formula F is satisfiable, Z3 will produce a model M. Then, if we change the value of any else in M, the resultant model M’ is still a model for F. Thus, you can ignore the else, or assume it is whatever you want, IF the input formula F is quantifier free, F does not use array theory, and :model-compact true is not used. This property is based on the algorithms currently implemented in Z3, and this may change in the future. In contrast, the solution provided by mhs is not affected by changes in the implementation of Z3. In his encoding, any SMT solver (that succeeds in producing a model) will have to use false as the value of the function in every point not specified in the antecedent of the quantifier.
		
		Another option is to use the default operator, and encode your problem using arrays. When, the default operator is used, we should view arrays as pairs: (Actual Array, Default value). This Default Value is used to provide the else value during model construction. Z3 also has several builtin axioms to propagate default values over: store and map operators. Here is your problem encoded using this approach:
		
		(set-option :produce-models true)
		(declare-const FPolicy (Array Int Int Int Bool))
		
		(assert (select FPolicy 0 1 30))
		(assert (select FPolicy 0 2 20))
		(assert (not (default FPolicy)))
		
		(check-sat)
		(get-model)
		
		
	*  In my case, I want to have the following program:
		(set-option :produce-models true)
		(declare-const FPolicy (Array Int Int Int Int))
		
		(assert ( = (select FPolicy 0 1 30) 45))
		(assert ( = (select FPolicy 0 2 20) 12))
		(assert (= (default FPolicy) 0))
		
		(check-sat)
		(get-model) 
		
	    Z3 provides the following model:
	    sat (model (define-fun FPolicy () (Array Int Int Int Int) (_ as-array k!0)) (define-fun k!0 ((x!1 Int) (x!2 Int) (x!3 Int)) Int (ite (and (= x!1 0) (= x!2 2) (= x!3 20)) 12 (ite (and (= x!1 0) (= x!2 1) (= x!3 30)) 45 0))) )

	*  Don't know how to create multidimensional array.   Maybe create array of arrays is good choice.
	    (define-sort FPolicy1 ()(Array Int Bool))
		(define-sort FPolicy2 ()(Array Int FPolicy1))
		(declare-const FPolicy (Array Int FPolicy2))
		
		(assert (select ( select (select FPolicy 0) 1) 30))
		(assert (select (select (select FPolicy 0) 2) 20))
		(assert(not(default(default (default FPolicy)))))
		
		(check-sat)
		(get-model) 
 * 
 * 
 * @author kaituo
 *
 */
public class ArrayAccessTest extends Z3PlainTest {
	public void testArray() {
		/* create node3 ::= node(20, node2, node1); */
//		Z3_ast args3[3] = { mk_int(ctx, 20), node2, node1 };
//		Z3_ast node3    = Z3_mk_app(ctx, node_decl, 3, args3);
	}
		

}
