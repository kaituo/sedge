package edu.umass.cs.pig.ast;

import static edu.umass.cs.pig.util.Assertions.check;
import static edu.umass.cs.pig.util.Assertions.notNull;
import static edu.umass.cs.pig.util.Log.logln;

import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.logical.expression.LogicalExpression;

import edu.umass.cs.pig.MainConfig;
import edu.umass.cs.pig.z3.Z3Context;
import edu.umass.cs.z3.SWIGTYPE_p__Z3_ast;

/**
 * @author csallner@uta.edu (Christoph Csallner)
 */
public abstract class AbstractExpression
implements Expression
{
  /**
   * Cached Z3 C pointer representing this constraint.
   * Set on demand, cached after that.
   */
  private SWIGTYPE_p__Z3_ast cachedZ3Pointer = null;

  /**
   * Z3 context stack level at which cachedZ3Pointer
   * is (or will be) persisted.
   */
  private final int persistLevel;

        /**
   * Pseudo-Java infix notation.
   */
  private String cachedSimpleRep = null;


  /**
   * By default, persist the corresponding Z3 ast
   * in the Dsc base context, can be reused in all future explorations.
   */
  protected AbstractExpression() {
        this(MainConfig.get().CONTEXT_HEIGHT_BASE_OF_ALL_EXPLORATIONS);
  }

  /**
   * @param persistLevel height of the Z3 context stack at which
   * the corresponding Z3 ast should be persisted.
   */
  protected AbstractExpression(int persistLevel) {
        this.persistLevel = persistLevel;
  }


  /**
   * @return Z3 ast that represents this expression.
   */
  @Override
  public SWIGTYPE_p__Z3_ast getZ3Ast() 
  {
    MainConfig conf = MainConfig.get();
    if (conf.DUMPER_MODE)
        return null;    
        
        if (cachedZ3Pointer==null) {
                if (conf.LOG_MALLOC_PERSIST_DISCARD)
                        logln("malloc "+this);
                Z3Context z3 = Z3Context.get();
                cachedZ3Pointer = notNull(mallocInZ3());

                /* Persist the Z3 pointer at the given Z3 context stack height
                 * (force Z3 to maintain pointer). */
      final int num_scopes = z3.getContextStackHeight() - persistLevel;
      if (num_scopes > 0)
        z3.persist_ast(this, num_scopes);
        }
        return cachedZ3Pointer;
  }


  @Override
  public final void discardZ3Pointer() {
        // FIXME: Un-comment below and manage memory better!

//      if (MainConfig.get().LOG_MALLOC_PERSIST_DISCARD)
//              logln("discard "+this);
//      cachedZ3Pointer = null;
  }
  
  /**
   * @author kaituo
   */
	protected SWIGTYPE_p__Z3_ast mallocInZ3() {
		check(false);
		return null;
	}

	/**
	 * @author kaituo
	 */
	protected String constructSimpleRep() {
		check(false);
		return null;
	}


  @Override
  public String toString() {
    // TODO: if we do not cache the string, the space for cachedSimpleRep
    // is wasted, as we never use it.
    if (MainConfig.get().DUMPER_MODE)
      return constructSimpleRep();
    
        if (cachedSimpleRep==null)
                cachedSimpleRep = constructSimpleRep();
    return cachedSimpleRep;
  }
}