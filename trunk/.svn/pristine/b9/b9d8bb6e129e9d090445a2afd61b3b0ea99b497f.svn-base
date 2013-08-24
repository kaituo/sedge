package edu.umass.cs.pig.dataflow;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;

import edu.umass.cs.pig.cntr.PltRelCntr;
import edu.umass.cs.pig.vm.PathList;
/**
 * Establish a map from x1 to <c,x2,y> where x==c for constraints like x1+x2=y
 * @author kaituo
 *
 */
public class PltAggrRelation extends Relation implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	  /** Map from parent vars to matching child vars. */
	  public Map<VarInfo,PltRelCntr> parent_to_child_map;

	  /** Map from child vars to matching parent vars. */
	  public Map<PltRelCntr,VarInfo> child_to_parent_map;
	  
	  public PltRelCntr aggregate;
	  
	  /**
	   * Create a relation between the specified parent and child.  The actual
	   * variable relations are filled in by the caller.  Note that this creates
	   * the connection between this relation and the parent/child.
	   */
	  public PltAggrRelation(PathList parent, PathList child) {
        super(parent, child);
//	    this.parent = parent;
//	    this.child = child;
	    parent_to_child_map = new LinkedHashMap<VarInfo,PltRelCntr>();
	    child_to_parent_map = new LinkedHashMap<PltRelCntr,VarInfo>();
	    
//	    connect();
	  }
	  
	  /**
	   * Adds this relation to its child's parent list and its parent's
	   * children list.
	   */
//	  private void connect() {
//	    assert !child.parents.contains(this);
//	    assert !parent.children.contains(this);
//	    child.parents.add(this);
//	    parent.children.add(this);
//	  }

	  /**
	   * Returns the child variable that corresponds to parentVar.  Returns
	   * null if there is no corresponding variable.
	   */
	  
	  public /*@Nullable*/ PltRelCntr childVar(VarInfo parentVar) {
		    return parent_to_child_map.get(parentVar);
	  }
	  
	  /**
	   * Returns the parent variable that corresponds to childVar.  Returns
	   * null if there is no corresponding variable.
	   */

	  public /*@Nullable*/ VarInfo parentVar(PltRelCntr childVar) {
	    return child_to_parent_map.get(childVar);
	  }

	public Map<VarInfo, PltRelCntr> getParent_to_child_map() {
		return parent_to_child_map;
	}

	public Map<PltRelCntr, VarInfo> getChild_to_parent_map() {
		return child_to_parent_map;
	}

}
