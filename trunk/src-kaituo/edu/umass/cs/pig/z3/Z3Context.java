package edu.umass.cs.pig.z3;

import static edu.umass.cs.pig.util.Assertions.check;
import static edu.umass.cs.pig.util.Assertions.notNull;

import java.lang.reflect.Field;

import org.apache.commons.math3.fraction.BigFraction;


import edu.umass.cs.pig.MainConfig;
import edu.umass.cs.pig.OSValidator;
import edu.umass.cs.pig.ast.Z3Ast;
import edu.umass.cs.z3.AstArray;
import edu.umass.cs.z3.AstArrayTheirs;
import edu.umass.cs.z3.SWIGTYPE_p__Z3_app;
import edu.umass.cs.z3.SWIGTYPE_p__Z3_ast;
import edu.umass.cs.z3.SWIGTYPE_p__Z3_config;
import edu.umass.cs.z3.SWIGTYPE_p__Z3_context;
import edu.umass.cs.z3.SWIGTYPE_p__Z3_func_decl;
import edu.umass.cs.z3.SWIGTYPE_p__Z3_model;
import edu.umass.cs.z3.SWIGTYPE_p__Z3_pattern;
import edu.umass.cs.z3.SWIGTYPE_p__Z3_sort;
import edu.umass.cs.z3.SWIGTYPE_p__Z3_symbol;
import edu.umass.cs.z3.SWIGTYPE_p_f_enum_Z3_error_code__void;
import edu.umass.cs.z3.SWIGTYPE_p_p__Z3_ast;
import edu.umass.cs.z3.SWIGTYPE_p_p__Z3_pattern;
import edu.umass.cs.z3.SWIGTYPE_p_p__Z3_sort;
import edu.umass.cs.z3.SWIGTYPE_p_p__Z3_symbol;
import edu.umass.cs.z3.SWIGTYPE_p_unsigned_int;
import edu.umass.cs.z3.SortArray;
import edu.umass.cs.z3.SortArrayTheirs;
import edu.umass.cs.z3.Z3;
import edu.umass.cs.z3.Z3_ast_kind;
import edu.umass.cs.z3.Z3_error_code;
import edu.umass.cs.z3.Z3_lbool;
import edu.umass.cs.z3.Z3_sort_kind;
import edu.umass.cs.z3.Z3_symbol_kind;
import edu.umass.cs.z3.MyInt;
import edu.umass.cs.z3.MyInt64;


/**
 * Manages the connection to Z3: Creates the Z3 interaction state and
 * makes all native calls (via Z3Wrap.dll).
 * 
 * Each method here is a convenience wrapper around a corresponding Z3
 * function in Z3Wrap.dll. Our convenience wrapper does the following.<ul><li>
 *   Make call relative to the current context. No need to pass the
 *      current context into every call.</li><li>
 *   Prints the time spent in individual Z3 calls, etc.</li></ul>
 * 
 * No other class should call Z3 directly.
 * If it does, it should pass our current Z3 context object ctx. 
 * 
 * TODO: JNA may be easier, see:
 *        https://jna.dev.java.net/
 *        https://jna.dev.java.net/javadoc/com/sun/jna/ptr/ByReference.html
 * 
 * @see JNI chapter of Java performance book:
 * http://java.sun.com/docs/books/performance/1st_edition/html/JPNativeCode.fm.html#18062
 * 
 * @author csallner@uta.edu (Christoph Csallner)
 */
public class Z3Context 
{    
  private final SWIGTYPE_p__Z3_context contextZ3;
  
  /**
   * Height of Z3 context stack.
   */
  private int contextStack = 0;  
  
  protected static final Z3Context _theInstance = new Z3Context();
  
  /**
   * @return singleton instance
   */
  public static Z3Context get() {
    return _theInstance;
  }
  /**
   * Constructor
   */
  protected Z3Context() 
  {
    MainConfig conf = MainConfig.get();
  	if (conf.DUMPER_MODE)
  	{
  		contextZ3 = null;
  		return;
  	}
  	
    try {
    	if(OSValidator.isWindows()) {
    		/* First try loading it from user-specified path */
    	      System.load(MainConfig.get().Z3_WRAP_PATH + MainConfig.get().z3WrapDll);
    	} else if(OSValidator.isUnix()) {
    		//set LD_LIBRARY_PATH=.
    		System.loadLibrary(MainConfig.get().getz3Wrap());
    	} else {
    		throw new UnsatisfiedLinkError();
    	}
      
    } 
    catch (UnsatisfiedLinkError e1) {
      try {
        /* If that did not work, try the same directory  */
        System.loadLibrary(MainConfig.get().z3Wrap);  
      }
      catch (UnsatisfiedLinkError e2) {
        System.err.println("Native code library failed to load. " +
            "See the chapter on Dynamic Linking Problems in the SWIG " +
            "Java documentation for help.\n" + e2);
        System.err.println(MainConfig.get().Z3_WRAP_PATH + MainConfig.get().z3WrapDll);
        // FIXME(csallner): instead of System.exit, throw a runtime exception 
        // and handle it in Dsc, RoopsDsc, etc.
        System.exit(1);
      }
    }
    
    SWIGTYPE_p__Z3_config configZ3 = Z3.Z3_mk_config();
    configureZ3(configZ3);
    this.contextZ3 = Z3.Z3_mk_context(configZ3);
    Z3.Z3_del_config(configZ3);
    
    if (conf.LOG_Z3_TRACE_TO_STD_ERR)
      Z3.Z3_trace_to_stderr(contextZ3);
  }  

  
  private void configureZ3(SWIGTYPE_p__Z3_config configZ3) 
  {
    MainConfig conf = MainConfig.get();
    for (Field field: conf.getClass().getFields()) {
      String fieldName = field.getName();
      if (! fieldName.startsWith("Z3PARAM_"))
        continue;
      String paramName = fieldName.substring(8);
      try {
        Z3.Z3_set_param_value(configZ3, paramName, field.get(conf).toString());
      }
      catch(IllegalAccessException iae) {
        iae.printStackTrace();
      }
    }
  }  
  
//  private void set_param_value(SWIGTYPE_p__Z3_config configZ3) 
//  {
//    MainConfig conf = MainConfig.get();
//    for (Field field: conf.getClass().getFields()) {
//      String fieldName = field.getName();
//      if (! fieldName.startsWith("Z3PARAM_"))
//        continue;
//      String paramName = fieldName.substring(8);
//      try {
//        Z3.Z3_set_param_value(configZ3, paramName, field.get(conf).toString());
//      }
//      catch(IllegalAccessException iae) {
//        iae.printStackTrace();
//      }
//    }
//  }  


  /* push and pop contexts onto the context stack */
  
  public int getContextStackHeight() {
    return contextStack;
  }
  
  public void push() 
  {
    MainConfig conf = MainConfig.get();
    if (conf.DUMPER_MODE)
    	return;
    
    long begin = getTime();
    Z3.Z3_push(contextZ3);
    contextStack += 1;
    edu.umass.cs.pig.util.Log.loglnIf(conf.LOG_EXPLORATION, conf.LOG_SECTION_PREFIX, "push 1 context --> "+contextStack);
    log(begin);
  }

  public void pop(long long_scopes) 
  {
    MainConfig conf = MainConfig.get();
    if (conf.DUMPER_MODE)
    	return;
    
    int scopes = (int) long_scopes;
    check(scopes <= contextStack, "Cannot pop more contexts than pushed.");
    
    long begin = getTime();
    Z3.Z3_pop(contextZ3, scopes);
    contextStack -= scopes;
    log(begin);    
  }
  

  public void persist_ast(Z3Ast ast, int num_scopes) 
  {
    MainConfig conf = MainConfig.get();
    if (conf.DUMPER_MODE)
    	return;
    
  	notNull(ast.getZ3Ast());
  	check(num_scopes > 0);
  	check(num_scopes <= getContextStackHeight());
  	
  	final int level = getContextStackHeight() - num_scopes;
  	
    if (MainConfig.get().LOG_MALLOC_PERSIST_DISCARD)
    	edu.umass.cs.pig.util.Log.logln("persist in context level "+level+": "+ast);
  	
    Z3.Z3_persist_ast(contextZ3, ast.getZ3Ast(), num_scopes);
  }
  
  /* constraints */
  
  /**
   * Issue generic constraint to our Z3 constraint solver.
   * 
   * <p>This method logs the Dsc-level constraint representation,
   * which is often more compact than the more low-level Z3-level
   * representation.
   */
//  protected void assertConstraint(
//	  LogicalExpressionPlan constraint, 
//      boolean logToScreen,
//      boolean logToFile) 
//  {
//    assert_cnstr(constraint.getZ3Ast());
//    
//    if (! (logToScreen || logToFile) )
//      return;
//    
//    String s = constraint.toString();
//    s = s.substring(1, s.length()-1);    
//    
//    if (logToScreen) 
//      edu.uta.cse.dsc.util.Log.logln(s);
//    
//    if (logToFile) 
//    {
//      if (contextStack == 0)
//      	SymbolicState.getInstance().dscBaseNode.loglnPerNode(s);
//      // FIXME: use fresh meth root node foreach exploration.
//      else if (contextStack == 1)
//      	SymbolicState.getInstance().methRootNode.loglnPerNode(s);
//      else if (contextStack > 1)
//      	QueryManager.z3ContextNode.loglnPerNode(s);
//      else
//      	check(false);
//    }    
//  }
  
  
  /**
   * Issue generic constraint to constraint solver.
   */
//  public void assertConstraint(
//	  LogicalExpressionPlan constraint, 
//      boolean logToScreen) 
//  {
//    MainConfig conf = MainConfig.get();
//    if (conf.DUMPER_MODE)
//    	return;
//    
//    assertConstraint(
//        constraint, 
//        conf.LOG_ALL_CONSTRAINTS || logToScreen, 
//        conf.LOG_ALL_CONSTRAINTS_TO_FILE);
//  }
  
  
//  /**
//   * Issue branch condition to constraint solver.
//   */
//  public void assertBranchCondition(Constraint constraint) 
//  {
//    MainConfig conf = MainConfig.get();
//    if (conf.DUMPER_MODE)
//    	return;
//    
//    assertConstraint(
//        constraint, 
//        conf.LOG_ALL_CONSTRAINTS || conf.LOG_BRANCH_CONDITIONS, 
//        conf.LOG_ALL_CONSTRAINTS_TO_FILE);
//  }
  
    
  public void assert_cnstr(SWIGTYPE_p__Z3_ast a) 
  {
    long begin = getTime();
    Z3.Z3_assert_cnstr(contextZ3, a);    
    log(begin);
  }  
  
  
  
  /* wrapper around Z3 api */  
  
  public SWIGTYPE_p__Z3_symbol mk_int_symbol(int i) {
    long begin = getTime();
    SWIGTYPE_p__Z3_symbol res = Z3.Z3_mk_int_symbol(contextZ3, i);    
    log(begin);
    return res;
  }

  public SWIGTYPE_p__Z3_symbol mk_string_symbol(String s) {
    long begin = getTime();
    SWIGTYPE_p__Z3_symbol res = Z3.Z3_mk_string_symbol(contextZ3, s);    
    log(begin);
    return res;
  }

  public SWIGTYPE_p__Z3_sort mk_uninterpreted_sort(SWIGTYPE_p__Z3_symbol s) {
    long begin = getTime();
    SWIGTYPE_p__Z3_sort res = Z3.Z3_mk_uninterpreted_sort(contextZ3, s);    
    log(begin);
    return res;
  }

  public SWIGTYPE_p__Z3_sort mk_bool_sort() {
    long begin = getTime();
    SWIGTYPE_p__Z3_sort res = Z3.Z3_mk_bool_sort(contextZ3);    
    log(begin);
    return res;
  }

  public SWIGTYPE_p__Z3_sort mk_int_sort() {
    long begin = getTime();
    SWIGTYPE_p__Z3_sort res = Z3.Z3_mk_int_sort(contextZ3);    
    log(begin);
    return res;
  }

  public SWIGTYPE_p__Z3_sort mk_real_sort() {
    long begin = getTime();
    SWIGTYPE_p__Z3_sort res = Z3.Z3_mk_real_sort(contextZ3);    
    log(begin);
    return res;
  }

  public SWIGTYPE_p__Z3_sort mk_bv_sort(long sz) {
    long begin = getTime();
    SWIGTYPE_p__Z3_sort res = Z3.Z3_mk_bv_sort(contextZ3, sz);    
    log(begin);
    return res;
  }

  public SWIGTYPE_p__Z3_sort mk_array_sort(SWIGTYPE_p__Z3_sort domain, SWIGTYPE_p__Z3_sort range) {
    long begin = getTime();
    SWIGTYPE_p__Z3_sort res = Z3.Z3_mk_array_sort(contextZ3, domain, range);    
    log(begin);
    return res;
  }
  

//  public SWIGTYPE_p__Z3_sort mk_tuple_sort(SWIGTYPE_p__Z3_symbol mk_tuple_name, long num_fields, SWIGTYPE_p_p__Z3_symbol field_names, SWIGTYPE_p_p__Z3_sort field_types, SWIGTYPE_p_p__Z3_const_decl_ast mk_tuple_decl, SWIGTYPE_p_p__Z3_const_decl_ast proj_decl) {
//    long begin = getTime();
//    SWIGTYPE_p__Z3_sort res = Z3.Z3_mk_tuple_sort(contextZ3, mk_tuple_name, num_fields, field_names, field_types, mk_tuple_decl, proj_decl);    
//    log(begin);
//    return res;
//  }

  
  
  /**
   * Convenience function
   */
  public SWIGTYPE_p__Z3_func_decl mk_injective_function(
      SWIGTYPE_p__Z3_symbol s,
      SWIGTYPE_p__Z3_sort range,
      SWIGTYPE_p__Z3_sort... domain)
  {
    SortArray typeAstArray = new SortArrayTheirs(domain.length);
    for (int i=0; i<domain.length; i++)
      typeAstArray.setitem(i, domain[i]);
    return mk_injective_function(s, domain.length, typeAstArray.cast(), range);
  }  
  
  
  
  public SWIGTYPE_p__Z3_func_decl mk_injective_function(
      SWIGTYPE_p__Z3_symbol s, 
      long domain_size, 
      SWIGTYPE_p_p__Z3_sort domain, 
      SWIGTYPE_p__Z3_sort range) 
  {
    long begin = getTime();
    SWIGTYPE_p__Z3_func_decl res = Z3.Z3_mk_injective_function(contextZ3, s, domain_size, domain, range);    
    log(begin);
    return res;
  }  
  
  
  
  /**
   * Convenience function
   */
  public SWIGTYPE_p__Z3_func_decl mk_func_decl(
      SWIGTYPE_p__Z3_symbol s,
      SWIGTYPE_p__Z3_sort range,
      SWIGTYPE_p__Z3_sort... domain)
  {
    SortArray typeAstArray = new SortArrayTheirs(domain.length);
    for (int i=0; i<domain.length; i++)
      typeAstArray.setitem(i, domain[i]);
    return mk_func_decl(s, domain.length, typeAstArray.cast(), range);
  }
  
  
  public SWIGTYPE_p__Z3_func_decl mk_func_decl(
      SWIGTYPE_p__Z3_symbol s, 
      long domain_size, 
      SWIGTYPE_p_p__Z3_sort domain, 
      SWIGTYPE_p__Z3_sort range) 
  {
    long begin = getTime();
    SWIGTYPE_p__Z3_func_decl res = Z3.Z3_mk_func_decl(contextZ3, s, domain_size, domain, range);    
    log(begin);
    return res;
  }
  
  public SWIGTYPE_p__Z3_func_decl mk_func_decl2(
	      SWIGTYPE_p__Z3_symbol s,
	      SWIGTYPE_p__Z3_sort range,
	      SWIGTYPE_p__Z3_sort... domain)
	  {
	    SortArray typeAstArray = new SortArrayTheirs(domain.length);
	    for (int i=0; i<domain.length; i++)
	      typeAstArray.setitem(i, domain[i]);
	    return mk_func_decl(s, domain.length, typeAstArray.cast(), range);
	  }

  
  /**
   * Convenience function
   */
  public SWIGTYPE_p__Z3_ast mk_app(
      SWIGTYPE_p__Z3_func_decl d, 
      SWIGTYPE_p__Z3_ast... args) 
  {
    AstArray astArray = new AstArrayTheirs(args.length);
    for (int i=0; i<args.length; i++)
      astArray.setitem(i, args[i]);
    return mk_app(d, args.length, astArray.cast());
  }
  
  public SWIGTYPE_p__Z3_ast mk_app(SWIGTYPE_p__Z3_func_decl d, long num_args, SWIGTYPE_p_p__Z3_ast args) {
    long begin = getTime();
    SWIGTYPE_p__Z3_ast res = Z3.Z3_mk_app(contextZ3, d, num_args, args);    
    log(res, begin);
    return res;
  }

  public SWIGTYPE_p__Z3_ast mk_const(SWIGTYPE_p__Z3_symbol s, SWIGTYPE_p__Z3_sort ty) {
    long begin = getTime();
    SWIGTYPE_p__Z3_ast res = Z3.Z3_mk_const(contextZ3, s, ty);    
    log(res, begin);
    return res;
  }

  public SWIGTYPE_p__Z3_ast mk_label(SWIGTYPE_p__Z3_symbol s, int is_pos, SWIGTYPE_p__Z3_ast f) {
    long begin = getTime();
    SWIGTYPE_p__Z3_ast res = Z3.Z3_mk_label(contextZ3, s, is_pos, f);    
    log(begin);
    return res;
  }

  public SWIGTYPE_p__Z3_func_decl mk_fresh_func_decl(String prefix, long domain_size, SWIGTYPE_p_p__Z3_sort domain, SWIGTYPE_p__Z3_sort range) {
    long begin = getTime();
    SWIGTYPE_p__Z3_func_decl res = Z3.Z3_mk_fresh_func_decl(contextZ3, prefix, domain_size, domain, range);    
    log(begin);
    return res;
  }

  public SWIGTYPE_p__Z3_ast mk_fresh_const(String prefix, SWIGTYPE_p__Z3_sort ty) {
    long begin = getTime();
    SWIGTYPE_p__Z3_ast res = Z3.Z3_mk_fresh_const(contextZ3, prefix, ty);    
    log(res, begin);
    return res;
  }

  public SWIGTYPE_p__Z3_ast mk_true() {
    long begin = getTime();
    SWIGTYPE_p__Z3_ast res = Z3.Z3_mk_true(contextZ3);    
    log(res, begin);
    return res;
  }

  public SWIGTYPE_p__Z3_ast mk_false() {
    long begin = getTime();
    SWIGTYPE_p__Z3_ast res = Z3.Z3_mk_false(contextZ3);    
    log(res, begin);
    return res;
  }

  public SWIGTYPE_p__Z3_ast mk_eq(SWIGTYPE_p__Z3_ast l, SWIGTYPE_p__Z3_ast r) {
    long begin = getTime();
    SWIGTYPE_p__Z3_ast res = Z3.Z3_mk_eq(contextZ3, l, r);    
    log(res, begin);
    return res;
  }

  
  /**
   * Convenience function
   */
  public SWIGTYPE_p__Z3_ast mk_distinct(SWIGTYPE_p__Z3_ast... args) {
    AstArray astarray = new AstArrayTheirs(args.length);
    for (int i=0; i<args.length; i++)
      astarray.setitem(i, args[i]);    
    return mk_distinct(args.length, astarray.cast());
  }
  
  public SWIGTYPE_p__Z3_ast mk_distinct(long num_args, SWIGTYPE_p_p__Z3_ast args) {
    long begin = getTime();
    SWIGTYPE_p__Z3_ast res = Z3.Z3_mk_distinct(contextZ3, num_args, args);    
    log(res, begin);
    return res;
  }

  public SWIGTYPE_p__Z3_ast mk_not(SWIGTYPE_p__Z3_ast a) {
    long begin = getTime();
    SWIGTYPE_p__Z3_ast res = Z3.Z3_mk_not(contextZ3, a);    
    log(res, begin);
    return res;
  }

  public SWIGTYPE_p__Z3_ast mk_ite(SWIGTYPE_p__Z3_ast t1, SWIGTYPE_p__Z3_ast t2, SWIGTYPE_p__Z3_ast t3) {
    long begin = getTime();
    SWIGTYPE_p__Z3_ast res = Z3.Z3_mk_ite(contextZ3, t1, t2, t3);    
    log(res, begin);
    return res;
  }

  public SWIGTYPE_p__Z3_ast mk_iff(SWIGTYPE_p__Z3_ast t1, SWIGTYPE_p__Z3_ast t2) {
    long begin = getTime();
    SWIGTYPE_p__Z3_ast res = Z3.Z3_mk_iff(contextZ3, t1, t2);    
    log(res, begin);
    return res;
  }

  public SWIGTYPE_p__Z3_ast mk_implies(SWIGTYPE_p__Z3_ast t1, SWIGTYPE_p__Z3_ast t2) {
    long begin = getTime();
    SWIGTYPE_p__Z3_ast res = Z3.Z3_mk_implies(contextZ3, t1, t2);    
    log(res, begin);
    return res;
  }

  public SWIGTYPE_p__Z3_ast mk_xor(SWIGTYPE_p__Z3_ast t1, SWIGTYPE_p__Z3_ast t2) {
    long begin = getTime();
    SWIGTYPE_p__Z3_ast res = Z3.Z3_mk_xor(contextZ3, t1, t2);    
    log(res, begin);
    return res;
  }

  
  /**
   * Convenience method for the common, binary and:
   *   x and y
   */
  public SWIGTYPE_p__Z3_ast mk_and(SWIGTYPE_p__Z3_ast x, SWIGTYPE_p__Z3_ast y) {
    AstArray andArgs = new AstArrayTheirs(2);
    andArgs.setitem(0, x);
    andArgs.setitem(1, y);
    return mk_and(2, andArgs.cast()); 
  }
  
  public SWIGTYPE_p__Z3_ast mk_and(long num_args, SWIGTYPE_p_p__Z3_ast args) {
    long begin = getTime();
    SWIGTYPE_p__Z3_ast res = Z3.Z3_mk_and(contextZ3, num_args, args);    
    log(res, begin);
    return res;
  }

  
  /**
   * Convenience method for the common, binary and:
   *   x and y
   */
  public SWIGTYPE_p__Z3_ast mk_or(SWIGTYPE_p__Z3_ast x, SWIGTYPE_p__Z3_ast y) {
    AstArray andArgs = new AstArrayTheirs(2);
    andArgs.setitem(0, x);
    andArgs.setitem(1, y);
    return mk_or(2, andArgs.cast()); 
  }
  
  public SWIGTYPE_p__Z3_ast mk_or(long num_args, SWIGTYPE_p_p__Z3_ast args) {
    long begin = getTime();
    SWIGTYPE_p__Z3_ast res = Z3.Z3_mk_or(contextZ3, num_args, args);    
    log(res, begin);
    return res;
  }
  
  
  /* Perfect integers */
  
  public SWIGTYPE_p__Z3_ast mk_lt(SWIGTYPE_p__Z3_ast t1, SWIGTYPE_p__Z3_ast t2) {
    long begin = getTime();
    SWIGTYPE_p__Z3_ast res = Z3.Z3_mk_lt(contextZ3, t1, t2);    
    log(res, begin);
    return res;
  }

  public SWIGTYPE_p__Z3_ast mk_le(SWIGTYPE_p__Z3_ast t1, SWIGTYPE_p__Z3_ast t2) {
    long begin = getTime();
    SWIGTYPE_p__Z3_ast res = Z3.Z3_mk_le(contextZ3, t1, t2);    
    log(res, begin);
    return res;
  }

  public SWIGTYPE_p__Z3_ast mk_gt(SWIGTYPE_p__Z3_ast t1, SWIGTYPE_p__Z3_ast t2) {
    long begin = getTime();
    SWIGTYPE_p__Z3_ast res = Z3.Z3_mk_gt(contextZ3, t1, t2);    
    log(res, begin);
    return res;
  }

  public SWIGTYPE_p__Z3_ast mk_ge(SWIGTYPE_p__Z3_ast t1, SWIGTYPE_p__Z3_ast t2) {
    long begin = getTime();
    SWIGTYPE_p__Z3_ast res = Z3.Z3_mk_ge(contextZ3, t1, t2);    
    log(res, begin);
    return res;
  }
  
  
  public String get_symbol_string(SWIGTYPE_p__Z3_symbol s) {
    long begin = getTime();
    String res = Z3.Z3_get_symbol_string(contextZ3, s);    
    log(begin);
    return res;
  }

  public SWIGTYPE_p__Z3_symbol get_decl_name(SWIGTYPE_p__Z3_func_decl d) {
    long begin = getTime();
    SWIGTYPE_p__Z3_symbol res = Z3.Z3_get_decl_name(contextZ3, d);    
    log(begin);
    return res;
  }
  
  
  public long get_model_num_constants(SWIGTYPE_p__Z3_model m) {
    long begin = getTime();
    long res = Z3.Z3_get_model_num_constants(contextZ3, m);    
    log(begin);
    return res;
  }

  public SWIGTYPE_p__Z3_func_decl get_model_constant(SWIGTYPE_p__Z3_model m, long i) {
    long begin = getTime();
    SWIGTYPE_p__Z3_func_decl res = Z3.Z3_get_model_constant(contextZ3, m, i);    
    log(begin);
    return res;
  }

  public Z3_ast_kind get_ast_kind(SWIGTYPE_p__Z3_ast a) 
  {
    long begin = getTime();
    Z3_ast_kind res = Z3.Z3_get_ast_kind(contextZ3, a);    
    log(begin);
    return res; 
  }
  
  public String get_numeral_string(SWIGTYPE_p__Z3_ast v) 
  {
    /* pre-condition:
     * http://research.microsoft.com/en-us/um/redmond/projects/z3/group__capi.html#ga94617ef18fa7157e1a3f85db625d2f4b */
    final Z3_ast_kind kind = get_ast_kind(v);
    check(kind == Z3_ast_kind.Z3_NUMERAL_AST, "expected: Z3_NUMERAL_AST, got: "+kind);
    
    long begin = getTime();
    String res = Z3.Z3_get_numeral_string(contextZ3, v);    
    log(begin);
    return res;
  }

  
  public Z3_lbool get_bool_value(SWIGTYPE_p__Z3_ast a) {
    return Z3.Z3_get_bool_value(contextZ3, a);
  }
  
  /* Arrays */

  public SWIGTYPE_p__Z3_ast mk_const_array(
      SWIGTYPE_p__Z3_sort domain, 
      SWIGTYPE_p__Z3_ast v) 
  {
    return Z3.Z3_mk_const_array(contextZ3, domain, v);
  }
  

  public SWIGTYPE_p__Z3_ast mk_array_default(SWIGTYPE_p__Z3_ast array) 
  {
    return Z3.Z3_mk_array_default(contextZ3, array);
  }
  
  /**
   * @return 
   *  not an array ==> -1, 
   *  else ==> the number of entries mapping to non-default values of the array
   */
  public long is_array_value(SWIGTYPE_p__Z3_ast v, SWIGTYPE_p__Z3_model m)
  {
    return Z3.Z3_is_array_value_return_out_param(contextZ3, m, v);
  }
  
  
  public SWIGTYPE_p__Z3_ast get_array_value(
      SWIGTYPE_p__Z3_ast v, 
      SWIGTYPE_p__Z3_model m,
      long num_entries, 
      SWIGTYPE_p_p__Z3_ast indices, 
      SWIGTYPE_p_p__Z3_ast values) 
  {
    return Z3.Z3_get_array_value_return_out_param(
        contextZ3, m, v, num_entries, indices, values);  
  }    
  
  
  /* Functions */
  
  public long get_model_num_funcs(SWIGTYPE_p__Z3_model m) {
    long begin = getTime();
    long res = Z3.Z3_get_model_num_funcs(contextZ3, m);    
    log(begin);
    return res;
  }

  public SWIGTYPE_p__Z3_func_decl get_model_func_decl(SWIGTYPE_p__Z3_model m, long i) {
    long begin = getTime();
    SWIGTYPE_p__Z3_func_decl res = Z3.Z3_get_model_func_decl(contextZ3, m, i);    
    log(begin);
    return res;
  }

  public SWIGTYPE_p__Z3_ast get_model_func_else(SWIGTYPE_p__Z3_model m, long i) {
    long begin = getTime();
    SWIGTYPE_p__Z3_ast res = Z3.Z3_get_model_func_else(contextZ3, m, i);    
    log(begin);
    return res;
  }

  public long get_model_func_num_entries(SWIGTYPE_p__Z3_model m, long i) {
    long begin = getTime();
    long res = Z3.Z3_get_model_func_num_entries(contextZ3, m, i);    
    log(begin);
    return res;
  }

  public long get_model_func_entry_num_args(SWIGTYPE_p__Z3_model m, long i, long j) {
    long begin = getTime();
    long res = Z3.Z3_get_model_func_entry_num_args(contextZ3, m, i, j);    
    log(begin);
    return res;
  }

  public SWIGTYPE_p__Z3_ast get_model_func_entry_arg(SWIGTYPE_p__Z3_model m, long i, long j, long k) {
    long begin = getTime();
    SWIGTYPE_p__Z3_ast res = Z3.Z3_get_model_func_entry_arg(contextZ3, m, i, j, k);    
    log(begin);
    return res;
  }

  public SWIGTYPE_p__Z3_ast get_model_func_entry_value(SWIGTYPE_p__Z3_model m, long i, long j) {
    long begin = getTime();
    SWIGTYPE_p__Z3_ast res = Z3.Z3_get_model_func_entry_value(contextZ3, m, i, j);    
    log(begin);
    return res;
  }

  
  /* FIXME: Make __out parameter v the return value.. */
//  public int eval(SWIGTYPE_p__Z3_model m, SWIGTYPE_p__Z3_ast t, SWIGTYPE_p_p__Z3_value v) {
//    long begin = getTime();
//    int res = Z3.Z3_get_model_func_decl(ctx, m, begin);    
//    log(begin);
//    return res;
//  }


  
  /* BitVec */
  
  public SWIGTYPE_p__Z3_ast mk_bvnot(SWIGTYPE_p__Z3_ast t1) {
    long begin = getTime();
    SWIGTYPE_p__Z3_ast res = Z3.Z3_mk_bvnot(contextZ3, t1);    
    log(res, begin);
    return res;
  }

  public SWIGTYPE_p__Z3_ast mk_bvand(SWIGTYPE_p__Z3_ast t1, SWIGTYPE_p__Z3_ast t2) {
    long begin = getTime();
    SWIGTYPE_p__Z3_ast res = Z3.Z3_mk_bvadd(contextZ3, t1, t2);    
    log(res, begin);
    return res;
  }

  public SWIGTYPE_p__Z3_ast mk_bvor(SWIGTYPE_p__Z3_ast t1, SWIGTYPE_p__Z3_ast t2) {
    long begin = getTime();
    SWIGTYPE_p__Z3_ast res = Z3.Z3_mk_bvor(contextZ3, t1, t2);    
    log(res, begin);
    return res;
  }

  public SWIGTYPE_p__Z3_ast mk_bvxor(SWIGTYPE_p__Z3_ast t1, SWIGTYPE_p__Z3_ast t2) {
    long begin = getTime();
    SWIGTYPE_p__Z3_ast res = Z3.Z3_mk_bvxor(contextZ3, t1, t2);    
    log(res, begin);
    return res;
  }

  public SWIGTYPE_p__Z3_ast mk_bvnand(SWIGTYPE_p__Z3_ast t1, SWIGTYPE_p__Z3_ast t2) {
    long begin = getTime();
    SWIGTYPE_p__Z3_ast res = Z3.Z3_mk_bvnand(contextZ3, t1, t2);    
    log(res, begin);
    return res;
  }

  public SWIGTYPE_p__Z3_ast mk_bvnor(SWIGTYPE_p__Z3_ast t1, SWIGTYPE_p__Z3_ast t2) {
    long begin = getTime();
    SWIGTYPE_p__Z3_ast res = Z3.Z3_mk_bvnor(contextZ3, t1, t2);    
    log(res, begin);
    return res;
  }

  public SWIGTYPE_p__Z3_ast mk_bvxnor(SWIGTYPE_p__Z3_ast t1, SWIGTYPE_p__Z3_ast t2) {
    long begin = getTime();
    SWIGTYPE_p__Z3_ast res = Z3.Z3_mk_bvxnor(contextZ3, t1, t2);    
    log(res, begin);
    return res;
  }

  public SWIGTYPE_p__Z3_ast mk_bvneg(SWIGTYPE_p__Z3_ast t1) {
    long begin = getTime();
    SWIGTYPE_p__Z3_ast res = Z3.Z3_mk_bvneg(contextZ3, t1);    
    log(res, begin);
    return res;
  }
  
  public SWIGTYPE_p__Z3_ast mk_bvadd(SWIGTYPE_p__Z3_ast t1, SWIGTYPE_p__Z3_ast t2) {
    long begin = getTime();
    SWIGTYPE_p__Z3_ast res = Z3.Z3_mk_bvadd(contextZ3, t1, t2);    
    log(res, begin);
    return res;
  }

  public SWIGTYPE_p__Z3_ast mk_bvsub(SWIGTYPE_p__Z3_ast t1, SWIGTYPE_p__Z3_ast t2) {
    long begin = getTime();
    SWIGTYPE_p__Z3_ast res = Z3.Z3_mk_bvsub(contextZ3, t1, t2);    
    log(res, begin);
    return res;
  }

  public SWIGTYPE_p__Z3_ast mk_bvmul(SWIGTYPE_p__Z3_ast t1, SWIGTYPE_p__Z3_ast t2) {
    long begin = getTime();
    SWIGTYPE_p__Z3_ast res = Z3.Z3_mk_bvmul(contextZ3, t1, t2);    
    log(res, begin);
    return res;
  }

  public SWIGTYPE_p__Z3_ast mk_bvudiv(SWIGTYPE_p__Z3_ast t1, SWIGTYPE_p__Z3_ast t2) {
    long begin = getTime();
    SWIGTYPE_p__Z3_ast res = Z3.Z3_mk_bvudiv(contextZ3, t1, t2);    
    log(res, begin);
    return res;
  }

  public SWIGTYPE_p__Z3_ast mk_bvsdiv(SWIGTYPE_p__Z3_ast t1, SWIGTYPE_p__Z3_ast t2) {
    long begin = getTime();
    SWIGTYPE_p__Z3_ast res = Z3.Z3_mk_bvsdiv(contextZ3, t1, t2);    
    log(res, begin);
    return res;
  }

  public SWIGTYPE_p__Z3_ast mk_bvurem(SWIGTYPE_p__Z3_ast t1, SWIGTYPE_p__Z3_ast t2) {
    long begin = getTime();
    SWIGTYPE_p__Z3_ast res = Z3.Z3_mk_bvurem(contextZ3, t1, t2);    
    log(res, begin);
    return res;
  }

  public SWIGTYPE_p__Z3_ast mk_bvsrem(SWIGTYPE_p__Z3_ast t1, SWIGTYPE_p__Z3_ast t2) {
    long begin = getTime();
    SWIGTYPE_p__Z3_ast res = Z3.Z3_mk_bvsrem(contextZ3, t1, t2);    
    log(res, begin);
    return res;
  }

  public SWIGTYPE_p__Z3_ast mk_bvsmod(SWIGTYPE_p__Z3_ast t1, SWIGTYPE_p__Z3_ast t2) {
    long begin = getTime();
    SWIGTYPE_p__Z3_ast res = Z3.Z3_mk_bvsmod(contextZ3, t1, t2);    
    log(res, begin);
    return res;
  }

  public SWIGTYPE_p__Z3_ast mk_bvult(SWIGTYPE_p__Z3_ast t1, SWIGTYPE_p__Z3_ast t2) {
    long begin = getTime();
    SWIGTYPE_p__Z3_ast res = Z3.Z3_mk_bvult(contextZ3, t1, t2);    
    log(res, begin);
    return res;
  }

  public SWIGTYPE_p__Z3_ast mk_bvslt(SWIGTYPE_p__Z3_ast t1, SWIGTYPE_p__Z3_ast t2) {
    long begin = getTime();
    SWIGTYPE_p__Z3_ast res = Z3.Z3_mk_bvslt(contextZ3, t1, t2);    
    log(res, begin);
    return res;
  }

  public SWIGTYPE_p__Z3_ast mk_bvule(SWIGTYPE_p__Z3_ast t1, SWIGTYPE_p__Z3_ast t2) {
    long begin = getTime();
    SWIGTYPE_p__Z3_ast res = Z3.Z3_mk_bvule(contextZ3, t1, t2);    
    log(res, begin);
    return res;
  }

  public SWIGTYPE_p__Z3_ast mk_bvsle(SWIGTYPE_p__Z3_ast t1, SWIGTYPE_p__Z3_ast t2) {
    long begin = getTime();
    SWIGTYPE_p__Z3_ast res = Z3.Z3_mk_bvsle(contextZ3, t1, t2);    
    log(res, begin);
    return res;
  }

  public SWIGTYPE_p__Z3_ast mk_bvuge(SWIGTYPE_p__Z3_ast t1, SWIGTYPE_p__Z3_ast t2) {
    long begin = getTime();
    SWIGTYPE_p__Z3_ast res = Z3.Z3_mk_bvuge(contextZ3, t1, t2);    
    log(res, begin);
    return res;
  }

  public SWIGTYPE_p__Z3_ast mk_bvsge(SWIGTYPE_p__Z3_ast t1, SWIGTYPE_p__Z3_ast t2) {
    long begin = getTime();
    SWIGTYPE_p__Z3_ast res = Z3.Z3_mk_bvsge(contextZ3, t1, t2);    
    log(res, begin);
    return res;
  }

  public SWIGTYPE_p__Z3_ast mk_bvugt(SWIGTYPE_p__Z3_ast t1, SWIGTYPE_p__Z3_ast t2) {
    long begin = getTime();
    SWIGTYPE_p__Z3_ast res = Z3.Z3_mk_bvugt(contextZ3, t1, t2);    
    log(res, begin);
    return res;
  }

  public SWIGTYPE_p__Z3_ast mk_bvsgt(SWIGTYPE_p__Z3_ast t1, SWIGTYPE_p__Z3_ast t2) {
    long begin = getTime();
    SWIGTYPE_p__Z3_ast res = Z3.Z3_mk_bvsgt(contextZ3, t1, t2);    
    log(res, begin);
    return res;
  }
  
  
  /* Select, store */
  
  public SWIGTYPE_p__Z3_ast mk_select(SWIGTYPE_p__Z3_ast a, SWIGTYPE_p__Z3_ast i) {
    long begin = getTime();
    SWIGTYPE_p__Z3_ast res = Z3.Z3_mk_select(contextZ3, a, i);    
    log(res, begin);
    return res;
  }

  public SWIGTYPE_p__Z3_ast mk_store(SWIGTYPE_p__Z3_ast a, SWIGTYPE_p__Z3_ast i, SWIGTYPE_p__Z3_ast v) {
    long begin = getTime();
    SWIGTYPE_p__Z3_ast res = Z3.Z3_mk_store(contextZ3, a, i, v);    
    log(res, begin);
    return res;
  }

  public SWIGTYPE_p__Z3_ast mk_numeral(String numeral, SWIGTYPE_p__Z3_sort ty) {
    long begin = getTime();
    SWIGTYPE_p__Z3_ast res = Z3.Z3_mk_numeral(contextZ3, numeral, ty);    
    log(res, begin);
    return res;
  }

  public SWIGTYPE_p__Z3_ast mk_int(int v, SWIGTYPE_p__Z3_sort ty) {
    long begin = getTime();
    SWIGTYPE_p__Z3_ast res = Z3.Z3_mk_int(contextZ3, v, ty);    
    log(res, begin);
    return res;
  }

  public SWIGTYPE_p__Z3_ast mk_unsigned_int(long v, SWIGTYPE_p__Z3_sort ty) {
    long begin = getTime();
    SWIGTYPE_p__Z3_ast res = Z3.Z3_mk_unsigned_int(contextZ3, v, ty);    
    log(res, begin);
    return res;
  }

  public SWIGTYPE_p__Z3_ast mk_int64(long v, SWIGTYPE_p__Z3_sort ty) {
    long begin = getTime();
    SWIGTYPE_p__Z3_ast res = Z3.Z3_mk_int64(contextZ3, v, ty);    
    log(res, begin);
    return res;
  }

  public SWIGTYPE_p__Z3_ast mk_unsigned_int64(java.math.BigInteger v, SWIGTYPE_p__Z3_sort ty) {
    long begin = getTime();
    SWIGTYPE_p__Z3_ast res = Z3.Z3_mk_unsigned_int64(contextZ3, v, ty);    
    log(res, begin);
    return res;
  }

  public SWIGTYPE_p__Z3_pattern mk_pattern(long num_patterns, SWIGTYPE_p_p__Z3_ast terms) {
    long begin = getTime();
    SWIGTYPE_p__Z3_pattern res = Z3.Z3_mk_pattern(contextZ3, num_patterns, terms);    
    log(begin);
    return res;
  }
  
  public String context_to_string() {
	    long begin = getTime();
	    String res = Z3.Z3_context_to_string(contextZ3);   
	    log(begin);
	    return res;
  }

  
  /**
   * Careful!
   * 
   * Read the documentation, i.e., on <b>De Bruijn indices</b>
   * 
   * @see http://en.wikipedia.org/wiki/De_Bruijn_index
   */
  public SWIGTYPE_p__Z3_ast mk_bound(long index, SWIGTYPE_p__Z3_sort ty) {
    long begin = getTime();
    SWIGTYPE_p__Z3_ast res = Z3.Z3_mk_bound(contextZ3, index, ty);    
    log(res, begin);
    return res;
  }

  public SWIGTYPE_p__Z3_ast mk_forall(long weight, long num_patterns, SWIGTYPE_p_p__Z3_pattern patterns, long num_decls, SWIGTYPE_p_p__Z3_sort types, SWIGTYPE_p_p__Z3_symbol decl_names, SWIGTYPE_p__Z3_ast body) {
    long begin = getTime();
    SWIGTYPE_p__Z3_ast res = Z3.Z3_mk_forall(contextZ3, weight, num_patterns, patterns, num_decls, types, decl_names, body);    
    log(res, begin);
    return res;
  }

  public SWIGTYPE_p__Z3_ast mk_exists(long weight, long num_patterns, SWIGTYPE_p_p__Z3_pattern patterns, long num_decls, SWIGTYPE_p_p__Z3_sort types, SWIGTYPE_p_p__Z3_symbol decl_names, SWIGTYPE_p__Z3_ast body) {
    long begin = getTime();
    SWIGTYPE_p__Z3_ast res = Z3.Z3_mk_exists(contextZ3, weight, num_patterns, patterns, num_decls, types, decl_names, body);    
    log(res, begin);
    return res;
  }

  public SWIGTYPE_p__Z3_ast mk_quantifier(
      int is_forall, 
      long weight, 
      long num_patterns, 
      SWIGTYPE_p_p__Z3_pattern patterns, 
      long num_decls, 
      SWIGTYPE_p_p__Z3_sort types, 
      SWIGTYPE_p_p__Z3_symbol decl_names, 
      SWIGTYPE_p__Z3_ast body) 
  {
    long begin = getTime();
    SWIGTYPE_p__Z3_ast res = Z3.Z3_mk_quantifier(
        contextZ3, 
        is_forall, 
        weight, 
        num_patterns, 
        patterns, 
        num_decls, 
        types, 
        decl_names, 
        body);    
    log(res, begin);
    return res;
  }

  
  public SWIGTYPE_p__Z3_sort get_sort(SWIGTYPE_p__Z3_ast t) 
  {
    return Z3.Z3_get_sort(contextZ3, t);
  }  
  
  
  public Z3_sort_kind get_sort_kind(SWIGTYPE_p__Z3_sort t) 
  {
    return Z3.Z3_get_sort_kind(contextZ3, t);
  }  
  


  
  public String ast_to_string(SWIGTYPE_p__Z3_ast a) {
    long begin = getTime();
    String res = Z3.Z3_ast_to_string(contextZ3, a);    
    log(begin);
    return res;
  }

  public String model_to_string(SWIGTYPE_p__Z3_model m) {
    long begin = getTime();
    String res = Z3.Z3_model_to_string(contextZ3, m);    
    log(begin);
    return res;
  }

  public String value_to_string(SWIGTYPE_p__Z3_ast v) {
    long begin = getTime();
    String res = Z3.Z3_ast_to_string(contextZ3, v);
    log(begin);
    return res;
  }

//  public String context_to_string() {
//    long begin = getTime();
//    String res = Z3.Z3_context_to_string(contextZ3);    
//    log(begin);
//    return res;
//  }

//  public void parse_smtlib_string(String str, long num_types, SWIGTYPE_p_p__Z3_symbol type_names, SWIGTYPE_p_p__Z3_sort types, long num_decls, SWIGTYPE_p_p__Z3_symbol decl_names, SWIGTYPE_p_p__Z3_const_decl_ast decls) {
//    long begin = getTime();
//    Z3.Z3_parse_smtlib_string(contextZ3, str, num_types, type_names, types, num_decls, decl_names, decls);    
//    log(begin);
//  }
//
//  public void parse_smtlib_file(String file_name, long num_types, SWIGTYPE_p_p__Z3_symbol type_names, SWIGTYPE_p_p__Z3_sort types, long num_decls, SWIGTYPE_p_p__Z3_symbol decl_names, SWIGTYPE_p_p__Z3_const_decl_ast decls) {
//    long begin = getTime();
//    Z3.Z3_parse_smtlib_file(contextZ3, file_name, num_types, type_names, types, num_decls, decl_names, decls);    
//    log(begin);
//  }

  public long get_smtlib_num_formulas() {
    long begin = getTime();
    long res = Z3.Z3_get_smtlib_num_formulas(contextZ3);    
    log(begin);
    return res;
  }

  public SWIGTYPE_p__Z3_ast get_smtlib_formula(long i) {
    long begin = getTime();
    SWIGTYPE_p__Z3_ast res = Z3.Z3_get_smtlib_formula(contextZ3, i);    
    log(res, begin);
    return res;
  }

  public long get_smtlib_num_assumptions() {
    long begin = getTime();
    long res = Z3.Z3_get_smtlib_num_assumptions(contextZ3);    
    log(begin);
    return res;
  }

  public SWIGTYPE_p__Z3_ast get_smtlib_assumption(long i) {
    long begin = getTime();
    SWIGTYPE_p__Z3_ast res = Z3.Z3_get_smtlib_assumption(contextZ3, i);    
    log(res, begin);
    return res;
  }

  public long get_smtlib_num_decls() {
    long begin = getTime();
    long res = Z3.Z3_get_smtlib_num_decls(contextZ3);
    log(begin);
    return res;
  }

  public SWIGTYPE_p__Z3_func_decl get_smtlib_decl(long i) {
    long begin = getTime();
    SWIGTYPE_p__Z3_func_decl res = Z3.Z3_get_smtlib_decl(contextZ3, i);
    log(begin);
    return res;
  }

  public Z3_error_code get_error_code() {
    long begin = getTime();
    Z3_error_code res = Z3.Z3_get_error_code(contextZ3);    
    log(begin);
    return res;
  }

  public void set_error_handler(SWIGTYPE_p_f_enum_Z3_error_code__void h) {
    long begin = getTime();
    Z3.Z3_set_error_handler(contextZ3, h);    
    log(begin);
  }

  public String get_error_msg(Z3_error_code err) {
    long begin = getTime();
    String res = Z3.Z3_get_error_msg(err);    
    log(begin);
    return res;
  }

  public void get_version(SWIGTYPE_p_unsigned_int major, SWIGTYPE_p_unsigned_int minor, SWIGTYPE_p_unsigned_int build_number, SWIGTYPE_p_unsigned_int revision_number) {
    long begin = getTime();
    Z3.Z3_get_version(major, minor, build_number, revision_number);    
    log(begin);
  }

//  public int type_check(SWIGTYPE_p__Z3_ast t) {
//    long begin = getTime();
//    int res = Z3.Z3_type_check(contextZ3, t);    
//    log(begin);
//    return res;
//  }
//
//  public long get_allocation_size() {
//    long begin = getTime();
//    long res = Z3.Z3_get_allocation_size();    
//    log(begin);
//    return res;
//  }

  public SWIGTYPE_p__Z3_model check_and_get_model_simple() {
    long begin = getTime();
    SWIGTYPE_p__Z3_model res = Z3.Z3_check_and_get_model_return_out_param(contextZ3);
    log(begin);
    return res;
  }
  
  
  public void del_model(SWIGTYPE_p__Z3_model m) {
    Z3.Z3_del_model(contextZ3, m);
  }


  public SWIGTYPE_p__Z3_ast eval_func_decl( 
      SWIGTYPE_p__Z3_model m, 
      SWIGTYPE_p__Z3_func_decl decl) 
  {
    return Z3.Z3_eval_func_decl_return_out_param(contextZ3, m, decl);
  }
  
	public Z3_symbol_kind get_symbol_kind(SWIGTYPE_p__Z3_symbol s) {
		return Z3.Z3_get_symbol_kind(contextZ3, s);
	}
	
	public int get_symbol_int(SWIGTYPE_p__Z3_symbol s) {
		return Z3.Z3_get_symbol_int(contextZ3, s);
	}
	
	public String func_decl_to_string(SWIGTYPE_p__Z3_func_decl s) {
		
		return Z3.Z3_func_decl_to_string(contextZ3, s);
	}
	
	public SWIGTYPE_p__Z3_app Z3_to_app(SWIGTYPE_p__Z3_ast s) {
		return Z3.Z3_to_app(contextZ3, s);
	}
	
	public long get_app_num_args(SWIGTYPE_p__Z3_app s) {
		return Z3.Z3_get_app_num_args(contextZ3, s);
	}
	
	public SWIGTYPE_p__Z3_func_decl get_app_decl(SWIGTYPE_p__Z3_app s) {
		return Z3.Z3_get_app_decl(contextZ3, s);
	}
	
	public SWIGTYPE_p__Z3_ast get_app_arg(SWIGTYPE_p__Z3_app s, long l) {
		return Z3.Z3_get_app_arg(contextZ3, s, l);
	}
	
	public SWIGTYPE_p__Z3_symbol get_sort_name(SWIGTYPE_p__Z3_sort s) {
		return Z3.Z3_get_sort_name(contextZ3, s);
	}
	
	public SWIGTYPE_p__Z3_sort get_array_sort_domain(SWIGTYPE_p__Z3_sort s) {
		return Z3.Z3_get_array_sort_domain(contextZ3, s);
	}
	
	public long get_bv_sort_size(SWIGTYPE_p__Z3_sort ty) {
		return Z3.Z3_get_bv_sort_size(contextZ3, ty);
	}
	
	public SWIGTYPE_p__Z3_sort get_array_sort_range(SWIGTYPE_p__Z3_sort s) {
		return Z3.Z3_get_array_sort_range(contextZ3, s);
	}
	
	public long get_tuple_sort_num_fields(SWIGTYPE_p__Z3_sort s) {
		return Z3.Z3_get_tuple_sort_num_fields(contextZ3, s);
	}
	
	public SWIGTYPE_p__Z3_sort get_range(SWIGTYPE_p__Z3_func_decl s) {
		return Z3.Z3_get_range(contextZ3, s);
	}
	
	public long get_datatype_sort_num_constructors(SWIGTYPE_p__Z3_sort s) {
		return Z3.Z3_get_datatype_sort_num_constructors(contextZ3, s);
	}
	
	public SWIGTYPE_p__Z3_func_decl get_tuple_sort_field_decl(SWIGTYPE_p__Z3_sort s, long t) {
		return Z3.Z3_get_tuple_sort_field_decl(contextZ3, s, t);
	}
	
	public String sort_to_string(SWIGTYPE_p__Z3_sort s) {
		return Z3.Z3_sort_to_string(contextZ3, s);
	}
	
  public SWIGTYPE_p__Z3_ast eval(
      SWIGTYPE_p__Z3_model m, 
      SWIGTYPE_p__Z3_ast t) 
  {
    return Z3.Z3_eval_return_out_param(contextZ3, m, t);
  }
  
  protected int trace_to_file(String trace_file) {
    long begin = getTime();
    int res = Z3.Z3_trace_to_file(contextZ3, trace_file);    
    log(begin);
    return res;
  }

  protected void trace_to_stderr() {
    long begin = getTime();
    Z3.Z3_trace_to_stderr(contextZ3);    
    log(begin);
  }

  public void trace_to_stdout() {
    long begin = getTime();
    Z3.Z3_trace_to_stdout(contextZ3);    
    log(begin);
  }

  public void trace_off() {
    long begin = getTime();
    Z3.Z3_trace_off(contextZ3);    
    log(begin);
  }  

  public void del_context() 
  {
    MainConfig conf = MainConfig.get();
    if (conf.DUMPER_MODE)
    	return;
    
    Z3.Z3_del_context(contextZ3);
  }
  
  
  /* logging */
  
  public long getTime() {
    long res = 0;
    
    if (MainConfig.get().LOG_Z3_TIMES)
      res = System.currentTimeMillis();
    
    return res;
  }
  
  
  public void log(long begin) {
    if (MainConfig.get().LOG_Z3_TIMES) {    
      long duration = getTime() - begin;
      edu.umass.cs.pig.util.Log.logln(String.format("%4d ms Z3", Long.valueOf(duration)));
      if (duration >= 10) {
        /* Put a breakpoint here to find most expensive calls */
    	  edu.umass.cs.pig.util.Log.logln("^^^^^^^ Expensive Z3 call");
      }
    }
  }
  
  
  public void log(SWIGTYPE_p__Z3_ast ast, long begin) {
    if (MainConfig.get().LOG_AST_Z3)
    	edu.umass.cs.pig.util.Log.logln(Z3.Z3_ast_to_string(contextZ3, ast));
    
    if (MainConfig.get().LOG_Z3_TIMES) {    
      long duration = getTime() - begin;
      edu.umass.cs.pig.util.Log.logln(String.format("%4d ms Z3", Long.valueOf(duration)));
      if (duration >= 10) {
        /* Put a breakpoint here to find most expensive calls */
    	  edu.umass.cs.pig.util.Log.logln("^^^^^^^ Expensive Z3 call");
      }
    }
  }
  
  public void log(int ast, long begin) {
	    if (MainConfig.get().LOG_AST_Z3)
	    	edu.umass.cs.pig.util.Log.logln(Integer.toString(ast));
	    
	    if (MainConfig.get().LOG_Z3_TIMES) {    
	      long duration = getTime() - begin;
	      edu.umass.cs.pig.util.Log.logln(String.format("%4d ms Z3", Long.valueOf(duration)));
	      if (duration >= 10) {
	        /* Put a breakpoint here to find most expensive calls */
	    	  edu.umass.cs.pig.util.Log.logln("^^^^^^^ Expensive Z3 call");
	      }
	    }
	  }
  
  public void log(long ast, long begin) {
	    if (MainConfig.get().LOG_AST_Z3)
	    	edu.umass.cs.pig.util.Log.logln(Long.toString(ast));
	    
	    if (MainConfig.get().LOG_Z3_TIMES) {    
	      long duration = getTime() - begin;
	      edu.umass.cs.pig.util.Log.logln(String.format("%4d ms Z3", Long.valueOf(duration)));
	      if (duration >= 10) {
	        /* Put a breakpoint here to find most expensive calls */
	    	  edu.umass.cs.pig.util.Log.logln("^^^^^^^ Expensive Z3 call");
	      }
	    }
	  }
  
	public void log(long[] ast, long begin) {
		int length = ast.length;
		for (int i = 0; i < length; i++) {
			if (MainConfig.get().LOG_AST_Z3)
				edu.umass.cs.pig.util.Log.logln(Long.toString(ast[i]));
		}

		if (MainConfig.get().LOG_Z3_TIMES) {
			long duration = getTime() - begin;
			edu.umass.cs.pig.util.Log.logln(String.format("%4d ms Z3",
					Long.valueOf(duration)));
			if (duration >= 10) {
				/* Put a breakpoint here to find most expensive calls */
				edu.umass.cs.pig.util.Log.logln("^^^^^^^ Expensive Z3 call");
			}
		}
	}
  
	public SWIGTYPE_p__Z3_ast mk_real_var(String name) {
		long begin = getTime();
		SWIGTYPE_p__Z3_sort ty = Z3.Z3_mk_real_sort(contextZ3);
		SWIGTYPE_p__Z3_ast res = mk_var(name, ty);
		log(res, begin);
		return res;
	}

	public SWIGTYPE_p__Z3_ast mk_real(String r) {
		long begin = getTime();
		SWIGTYPE_p__Z3_sort ty = Z3.Z3_mk_real_sort(contextZ3);
		SWIGTYPE_p__Z3_ast res = mk_numeral(r, ty);
		log(begin);
		return res;
	}

	public SWIGTYPE_p__Z3_ast mk_add(long num_args, SWIGTYPE_p__Z3_ast[] args) {
		long begin = getTime();
		AstArray astarray = new AstArrayTheirs(args.length);
		for (int i = 0; i < args.length; i++)
			astarray.setitem(i, args[i]);
		SWIGTYPE_p__Z3_ast res = Z3.Z3_mk_add(contextZ3, num_args,
				astarray.cast());
		log(begin);
		return res;
	}
	
	public SWIGTYPE_p__Z3_ast mk_sub(long num_args, SWIGTYPE_p__Z3_ast[] args) {
		long begin = getTime();
		AstArray astarray = new AstArrayTheirs(args.length);
		for (int i = 0; i < args.length; i++)
			astarray.setitem(i, args[i]);
		SWIGTYPE_p__Z3_ast res = Z3.Z3_mk_sub(contextZ3, num_args,
				astarray.cast());
		log(begin);
		return res;
	}
	
	public SWIGTYPE_p__Z3_ast mk_mul(long num_args, SWIGTYPE_p__Z3_ast[] args) {
		long begin = getTime();
		AstArray astarray = new AstArrayTheirs(args.length);
		for (int i = 0; i < args.length; i++)
			astarray.setitem(i, args[i]);
		SWIGTYPE_p__Z3_ast res = Z3.Z3_mk_mul(contextZ3, num_args,
				astarray.cast());
		log(begin);
		return res;
	}
	
	public SWIGTYPE_p__Z3_ast mk_div(SWIGTYPE_p__Z3_ast arg1, SWIGTYPE_p__Z3_ast arg2) {
		long begin = getTime();
		
		SWIGTYPE_p__Z3_ast res = Z3.Z3_mk_div(contextZ3, arg1,
				arg2);
		log(begin);
		return res;
	}
	
	public SWIGTYPE_p__Z3_ast mk_mod(SWIGTYPE_p__Z3_ast arg1, SWIGTYPE_p__Z3_ast arg2) {
		long begin = getTime();
		
		SWIGTYPE_p__Z3_ast res = Z3.Z3_mk_mod(contextZ3, arg1,
				arg2);
		log(begin);
		return res;
	}
	
	public SWIGTYPE_p__Z3_ast mk_var(String name, SWIGTYPE_p__Z3_sort ty) {
	    long begin = getTime();
	    SWIGTYPE_p__Z3_symbol s = Z3.Z3_mk_string_symbol(contextZ3, name);
	    SWIGTYPE_p__Z3_ast res = Z3.Z3_mk_const(contextZ3, s, ty);    
	    log(res, begin);
	    return res;
	}
	
	public SWIGTYPE_p__Z3_ast mk_bv2int(SWIGTYPE_p__Z3_ast bv) {
	    long begin = getTime();
	    SWIGTYPE_p__Z3_ast res = Z3.Z3_mk_bv2int(contextZ3, bv, 1);
	    log(res, begin);
	    return res;
	}
	
	public int get_numeral_int(SWIGTYPE_p__Z3_ast numeral) {
	    long begin = getTime();
	    MyInt m= new MyInt();
	    Z3.Z3_get_numeral_int_return_out_param(contextZ3, numeral, m);
	    int res = m.getValue();
	    log(res, begin);
	    return res;
	}
	
	public long get_numeral_int64(SWIGTYPE_p__Z3_ast numeral) {
	    long begin = getTime();
	    MyInt64 m = new MyInt64();
	    Z3.Z3_get_numeral_int64_return_out_param(contextZ3, numeral, m);
	    long res = m.getValue();
	    log(res, begin);
	    return res;
	}
	
	public BigFraction get_numeral_rational_int64(SWIGTYPE_p__Z3_ast numeral) {
	    long begin = getTime();
	    MyInt64 m1 = new MyInt64();
	    MyInt64 m2 = new MyInt64();
	    Z3.Z3_get_numeral_rational_int64_return_out_param(contextZ3, numeral, m1, m2);
	    long res[] = {m1.getValue(), m2.getValue()};
	    BigFraction bf = new BigFraction(m1.getValue(), m2.getValue());
	    log(res, begin);
	    return bf;
	}
	
	public SWIGTYPE_p__Z3_ast mk_real(int num, int den) {
	    long begin = getTime();
	    SWIGTYPE_p__Z3_ast res = Z3.Z3_mk_real(contextZ3, num, den);
	    log(res, begin);
	    return res;
	}
	
	public SWIGTYPE_p__Z3_ast mk_bvmul_no_overflow(SWIGTYPE_p__Z3_ast arg1, SWIGTYPE_p__Z3_ast arg2) {
		long begin = getTime();
	    SWIGTYPE_p__Z3_ast res = Z3.Z3_mk_bvmul_no_overflow(contextZ3, arg1, arg2, 1);
	    log(res, begin);
	    return res;
	}
	
	public SWIGTYPE_p__Z3_ast mk_bvmul_no_underflow(SWIGTYPE_p__Z3_ast arg1, SWIGTYPE_p__Z3_ast arg2) {
		long begin = getTime();
	    SWIGTYPE_p__Z3_ast res = Z3.Z3_mk_bvmul_no_underflow(contextZ3, arg1, arg2);
	    log(res, begin);
	    return res;
	}
	
	public SWIGTYPE_p__Z3_ast mk_bvsub_no_overflow(SWIGTYPE_p__Z3_ast arg1, SWIGTYPE_p__Z3_ast arg2) {
		long begin = getTime();
	    SWIGTYPE_p__Z3_ast res = Z3.Z3_mk_bvsub_no_overflow(contextZ3, arg1, arg2);
	    log(res, begin);
	    return res;
	}
	
	public SWIGTYPE_p__Z3_ast mk_bvsdiv_no_overflow(SWIGTYPE_p__Z3_ast arg1, SWIGTYPE_p__Z3_ast arg2) {
		long begin = getTime();
	    SWIGTYPE_p__Z3_ast res = Z3.Z3_mk_bvsdiv_no_overflow (contextZ3, arg1, arg2);
	    log(res, begin);
	    return res;
	}
	
	public SWIGTYPE_p__Z3_ast mk_bvsub_no_underflow (SWIGTYPE_p__Z3_ast arg1, SWIGTYPE_p__Z3_ast arg2) {
		long begin = getTime();
	    SWIGTYPE_p__Z3_ast res = Z3.Z3_mk_bvsub_no_underflow(contextZ3, arg1, arg2, 1);
	    log(res, begin);
	    return res;
	}
	
	public SWIGTYPE_p__Z3_ast mk_bvadd_no_overflow(SWIGTYPE_p__Z3_ast arg1, SWIGTYPE_p__Z3_ast arg2) {
		long begin = getTime();
	    SWIGTYPE_p__Z3_ast res = Z3.Z3_mk_bvadd_no_overflow(contextZ3, arg1, arg2, 1);
	    log(res, begin);
	    return res;
	}
	
	public SWIGTYPE_p__Z3_ast mk_bvadd_no_underflow(SWIGTYPE_p__Z3_ast arg1, SWIGTYPE_p__Z3_ast arg2) {
		long begin = getTime();
	    SWIGTYPE_p__Z3_ast res = Z3.Z3_mk_bvadd_no_underflow(contextZ3, arg1, arg2);
	    log(res, begin);
	    return res;
	}
	
	
	
	
}
