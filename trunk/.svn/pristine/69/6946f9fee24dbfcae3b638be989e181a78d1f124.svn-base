package edu.umass.cs.pig.dataflow;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;

import edu.umass.cs.pig.vm.PathList;

public class PltRelation extends Relation implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	
	  
	  /**
	   * Create a relation between the specified parent and child.  The actual
	   * variable relations are filled in by the caller.  Note that this creates
	   * the connection between this relation and the parent/child.
	   */
	  public PltRelation(PathList parent, PathList child) {
        super(parent, child);
//	    this.parent = parent;
//	    this.child = child;
        
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
	  
	  public /*@Nullable*/ VarInfo childVar(VarInfo parentVar) {
		    return parent_to_child_map.get(parentVar);
	  }
	  
	  /**
	   * Returns the parent variable that corresponds to childVar.  Returns
	   * null if there is no corresponding variable.
	   */

	  public /*@Nullable*/ VarInfo parentVar(VarInfo childVar) {
	    return child_to_parent_map.get(childVar);
	  }

	public Map<VarInfo, VarInfo> getParent_to_child_map() {
		return parent_to_child_map;
	}

	public void setParent_to_child_map(Map<VarInfo, VarInfo> parent_to_child_map) {
		this.parent_to_child_map = parent_to_child_map;
	}
}
