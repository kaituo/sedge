package edu.umass.cs.pig.z3.test;

import static edu.umass.cs.pig.util.Log.logln;
import junit.framework.TestCase;
import edu.umass.cs.pig.MainConfig;
import edu.umass.cs.pig.z3.DisplayModel;
import edu.umass.cs.pig.z3.Z3Context;
import edu.umass.cs.z3.SWIGTYPE_p__Z3_app;
import edu.umass.cs.z3.SWIGTYPE_p__Z3_ast;
import edu.umass.cs.z3.SWIGTYPE_p__Z3_func_decl;
import edu.umass.cs.z3.SWIGTYPE_p__Z3_model;
import edu.umass.cs.z3.SWIGTYPE_p__Z3_sort;
import edu.umass.cs.z3.SWIGTYPE_p__Z3_symbol;

/**
 * @author csallner@uta.edu (Christoph Csallner)
 */
public abstract class Z3Test extends TestCase {
  
  protected final Z3Context z3 = Z3Context.get();
  
  protected Z3Test() {
    MainConfig.get().LOG_MODEL_Z3 = true;
    MainConfig.get().LOG_MODEL_DSC = true;
    MainConfig.get().LOG_ALL_CONSTRAINTS = true;
    
  }
  
  @Override
  protected void setUp() throws Exception {
  	super.setUp();
    System.out.println("\n");
  }
  
  @Override
  protected void tearDown() throws Exception {
  	super.tearDown();
  }
  
  
  /**
   * Simplified version of the prove function of the Z3 C examples.<br><br>
   * 
   * Checks if (in the current Z3 context)
   * there is a solution for the negation of the given formula.<br><br>
   * 
   * If we find such solution, we have disproved the formula.
   * If we cannot find such a solution, there may not be one, which would
   * indicate that the formula is true.
   */
  protected boolean always(SWIGTYPE_p__Z3_ast formula) {
    boolean res = true;
    
    z3.push();
    
    z3.assert_cnstr (not (formula));
    
    SWIGTYPE_p__Z3_model model = z3.check_and_get_model_simple();
    if (model==null)
      if (MainConfig.get().LOG_MODEL_Z3)
        logln("Cannot find a counter-example, " +
        		"chances are that the formula is always true.");
    if (model!=null) {
      res = false;
      if (MainConfig.get().LOG_MODEL_Z3) {
        logln(z3.model_to_string(model));
        logln("Just printed a counter-example for the formula.");
      }
    }
    
    z3.pop(1);
    return res;
  }

  
  protected boolean canBeTrue(SWIGTYPE_p__Z3_ast formula) {
    boolean res = false;    
    z3.push();
    
    z3.assert_cnstr(formula);    
    SWIGTYPE_p__Z3_model model = z3.check_and_get_model_simple();
    if (model==null) {
      if (MainConfig.get().LOG_MODEL_Z3)
        logln("Cannot find a single satisfying solution, " +
            "chances are the formula is always false.");
    }
    else {
      res = true;
      if (MainConfig.get().LOG_MODEL_Z3) {
        logln(z3.model_to_string(model));
        logln("Just printed satisfying solution for the formula.");
      }
      DisplayModel m = new DisplayModel();
		m.display_model(model);
		System.out.println();
		System.out.println(z3.model_to_string(model));
    }
    
    z3.pop(1);
    return res;
  }
  
  protected boolean never(SWIGTYPE_p__Z3_ast formula) {
    return !canBeTrue(formula);
  }
  
  public void display_function_interpretations(SWIGTYPE_p__Z3_model m) 
	{
	   long num_functions, i;
	
	   System.out.print("function interpretations:\n");
	
	   num_functions = z3.get_model_num_funcs(m);
	   for (i = 0; i < num_functions; i++) {
		   SWIGTYPE_p__Z3_func_decl fdecl;
		   SWIGTYPE_p__Z3_symbol name;
		   SWIGTYPE_p__Z3_ast func_else;
	       long num_entries, j;
	       
	       fdecl = z3.get_model_func_decl(m, i);
	       name = z3.get_decl_name(fdecl);
	       display_symbol(name);
	       System.out.print(" = {");
	       num_entries = z3.get_model_func_num_entries(m, i);
	       for (j = 0; j < num_entries; j++) {
	           long num_args, k;
	           if (j > 0) {
	               System.out.print(", ");
	           }
	           num_args = z3.get_model_func_entry_num_args(m, i, j);
	           System.out.print("(");
	           for (k = 0; k < num_args; k++) {
	               if (k > 0) {
	                   System.out.print(", ");
	               }
	               System.out.print(z3.get_model_func_entry_arg(m, i, j, k));
	           }
	           System.out.print("|->");
	           display_ast(z3.get_model_func_entry_value(m, i, j));
	           System.out.print(")");
	       }
	       if (num_entries > 0) {
	    	   System.out.print(", ");
	       }
	       System.out.print("(else|->");
	       func_else = z3.get_model_func_else(m, i);
	       System.out.print(func_else);
	       System.out.print(")}\n");
	   }
	}
  
  void display_symbol(SWIGTYPE_p__Z3_symbol s) 
  {
      switch (z3.get_symbol_kind(s)) {
      case Z3_INT_SYMBOL:
          System.out.print(z3.get_symbol_int(s));
          break;
      case Z3_STRING_SYMBOL:
    	  System.out.print(z3.get_symbol_string(s));
          break;
      default:
          unreachable();
      }
  }
  
  void display_ast(SWIGTYPE_p__Z3_ast v) 
  {
      switch (z3.get_ast_kind(v)) {
      case Z3_NUMERAL_AST: {
    	  SWIGTYPE_p__Z3_sort t;
          System.out.print(z3.get_numeral_string(v));
          t = z3.get_sort(v);
          System.out.print(":");
          display_sort(t);
          break;
      }
      case Z3_APP_AST: {
          long i;
          SWIGTYPE_p__Z3_app app = z3.Z3_to_app(v);
          long num_fields = z3.get_app_num_args(app);
          SWIGTYPE_p__Z3_func_decl d = z3.get_app_decl(app);
          System.out.print(z3.func_decl_to_string(d));
          if (num_fields > 0) {
        	  System.out.print("[");
              for (i = 0; i < num_fields; i++) {
                  if (i > 0) {
                	  System.out.print(", ");
                  }
                  display_ast(z3.get_app_arg(app, i));
              }
              System.out.print("]");
          }
          break;
      }
      case Z3_QUANTIFIER_AST: {
    	  System.out.print("quantifier");
          ;	
      }
      default:
    	  System.out.print("#unknown");
      }
  }
  
  void display_sort(SWIGTYPE_p__Z3_sort ty) 
  {
      switch (z3.get_sort_kind(ty)) {
      case Z3_UNINTERPRETED_SORT:
          display_symbol(z3.get_sort_name(ty));
          break;
      case Z3_BOOL_SORT:
    	  System.out.print("bool");
          break;
      case Z3_INT_SORT:
    	  System.out.print("int");
          break;
      case Z3_REAL_SORT:
    	  System.out.print("real");
          break;
      case Z3_BV_SORT:
    	  System.out.print("bv" + z3.get_bv_sort_size(ty));
          break;
      case Z3_ARRAY_SORT: 
    	  System.out.print("[");
          display_sort(z3.get_array_sort_domain(ty));
          System.out.print( "->");
          display_sort(z3.get_array_sort_range(ty));
          System.out.print("]");
          break;
      case Z3_DATATYPE_SORT:
  		if (z3.get_datatype_sort_num_constructors(ty) != 1) 
  		{
  			System.out.print(z3.sort_to_string(ty));
  			break;
  		}
  		{
          long num_fields = z3.get_tuple_sort_num_fields(ty);
          long i;
          System.out.print("(");
          for (i = 0; i < num_fields; i++) {
        	  SWIGTYPE_p__Z3_func_decl field = z3.get_tuple_sort_field_decl(ty, i);
              if (i > 0) {
            	  System.out.print(", ");
              }
              display_sort(z3.get_range(field));
          }
          System.out.print(")");
          break;
      }
      default:
    	  System.out.print("unknown[");
          display_symbol(z3.get_sort_name(ty));
          System.out.print("]");
          break;
      }
  }
  
  void unreachable() 
  {
      System.err.println("unreachable code was reached");
  }
  
  
  /*
   * TODO: Everything below is copy-and-paste from Axiom.
   * @see edu.uta.cse.dsc.rules.base.Axiom
   */
  

  /**
   * <pre>
   *   ref type --> z3bool
   * </pre> 
   * 
   * The ref_staticType relation can be sound and complete.
   */
//  protected SWIGTYPE_p__Z3_ast ref_staticType(
//      SWIGTYPE_p__Z3_ast ref,
//      SWIGTYPE_p__Z3_ast type) {
//    return z3.mk_app(Z3ConnectionState.get().ref_staticType, ref, type); 
//  }
  
  
//  /**
//   * <pre>
//   *   ref --> type
//   * </pre>
//   * 
//   * Warning: The ref_dynamicType returns an invalid type for the
//   * ref parameter of null.
//   */
//  protected SWIGTYPE_p__Z3_ast ref_dynamicType(SWIGTYPE_p__Z3_ast ref) {
//    return z3.mk_app(Z3ContextDscState.get().ref_dynamicType, ref); 
//  }
//
//  
//  /**
//   * <pre>
//   *   type --> bool
//   * </pre>
//   * 
//   * The type_interface can be sound and complete.
//   */
//  protected SWIGTYPE_p__Z3_ast type_interface(SWIGTYPE_p__Z3_ast type) {
//    return z3.mk_app(Z3ContextDscState.get().type_interface, type); 
//  }
  
  
  /**
   * <pre>
   *   type --> class
   * </pre>
   * 
   * Because the Superclass axiom restricts the result to non-interface
   * types.<br>
   * <br>
   * Warning: The i_sub_superclass function returns an arbitrary class if the 
   * type parameter is an interface. You should not use the result of 
   * i_sub_superclass when the type parameter is an interface. This has
   * to do with i_sub_superclass being a total function, although it
   * should be partial -- only defined on class types.
   */
//  protected SWIGTYPE_p__Z3_ast i_sub_superclass(SWIGTYPE_p__Z3_ast type) {
//    return z3.mk_app(Z3ConnectionState.get().i_sub_superclass, type); 
//  }

  
  /**
   * <pre>
   *   type class     --> false
   *   type interface --> bool
   * </pre>
   * 
   * Because the Interfaces axiom sets the result of (type class)
   * to false.<br>
   * <br>
   * The i_sub_interface relation can be sound and complete.
   */
//  protected SWIGTYPE_p__Z3_ast i_sub_interface(
//      SWIGTYPE_p__Z3_ast subT,
//      SWIGTYPE_p__Z3_ast superT) 
//  {
//    return z3.mk_app(Z3ConnectionState.get().i_sub_interface, subT, superT); 
//  }
  
  
  /**
   * <pre>
   *   type type --> bool
   * </pre>
   * 
   * The sub_supertype relation can be sound and complete.
   */
//  protected SWIGTYPE_p__Z3_ast sub_supertype(
//      SWIGTYPE_p__Z3_ast subT,
//      SWIGTYPE_p__Z3_ast superT) 
//  {
//    return z3.mk_app(Z3ConnectionState.get().sub_supertype, subT, superT); 
//  }

  
  
  protected SWIGTYPE_p__Z3_ast not(
      SWIGTYPE_p__Z3_ast a) 
  {
    return z3.mk_not(a); 
  }

  protected SWIGTYPE_p__Z3_ast eq(
      SWIGTYPE_p__Z3_ast t1, 
      SWIGTYPE_p__Z3_ast t2) 
  {
    return z3.mk_eq(t1, t2); 
  }  
  
  protected SWIGTYPE_p__Z3_ast neq(
      SWIGTYPE_p__Z3_ast t1, 
      SWIGTYPE_p__Z3_ast t2) 
  {
    return not (eq (t1, t2)); 
  } 
  
  protected SWIGTYPE_p__Z3_ast implies(
      SWIGTYPE_p__Z3_ast t1, 
      SWIGTYPE_p__Z3_ast t2) 
  {
    return z3.mk_implies(t1, t2); 
  }
  
  protected SWIGTYPE_p__Z3_ast and(
      SWIGTYPE_p__Z3_ast t1, 
      SWIGTYPE_p__Z3_ast t2) 
  {
    return z3.mk_and(t1, t2); 
  }  
  
  protected SWIGTYPE_p__Z3_ast or(
      SWIGTYPE_p__Z3_ast t1, 
      SWIGTYPE_p__Z3_ast t2) 
  {
    return z3.mk_or(t1, t2); 
  }
}

