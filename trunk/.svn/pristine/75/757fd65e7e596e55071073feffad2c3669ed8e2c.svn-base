package edu.umass.cs.pig.vm;

import static edu.umass.cs.pig.util.Assertions.check;
import static edu.umass.cs.pig.util.Assertions.notNull;
import static edu.umass.cs.pig.util.Log.loglnIf;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.pig.newplan.logical.expression.LogicalExpressionPlan;

import edu.umass.cs.pig.MainConfig;
import edu.umass.cs.pig.util.Log;
import edu.umass.cs.pig.z3.Z3Context;
import gnu.trove.THashMap;

/**
 * Node in the tree of execution paths.
 *
 * <p>A node represents either a part of a feasible execution path or a
 * part of an infeasible execution path. If it represents a part of an
 * infeasible path, this node will not have any children. We add
 * children lazily, whenever a concrete (feasible) execution triggers
 * us to do so.<ul><li>
 *
 * (children.size() ==0) ==> node is either<ul><li>
 *                            the last basic-block of a feasible path, or</li><li>
 *                            the root node of an infeasible sub-tree.</li></ul><li>
 * (children.size() > 0) ==> node is feasible.</li><li>
 * (children.size() ==1) ==> node has an unexplored outgoing branch.</li><li>
 * (children.size() ==2) ==> node has no direct unexplored outgoing branches.</li></ul>
 *
 * We have organized the execution paths into a tree structure to share
 * space, i.e., common path prefixes are only stored once.
 *
 * TODO: Support multi-branch nodes, which may have more than two children.
 *
 * @author csallner@uta.edu (Christoph Csallner)
 */
public class PathTreeNode
{
  protected PathTreeNode parent;   	// parent in execution tree
  protected final PathBranch branchTaken; // branch taken to get to this node
  
  public final List<PathTreeNode> children =
    new LinkedList<PathTreeNode>();				// successors in execution tree

  final StringBuilder nodeLog = new StringBuilder();
  
  /**
   * Constructor
   */
  public PathTreeNode(PathTreeNode parent, PathBranch constraint) {
    this.parent = notNull(parent);
    this.branchTaken = notNull(constraint);
  }
  
  public PathTreeNode(PathBranch constraint) {
	  this.parent = null;
	  this.branchTaken = notNull(constraint);
  }
  
  /**
   * For root/base node only
   */
  public PathTreeNode() {
    this.parent = null;
    this.branchTaken = null;
  }

  public void logPerNode(String s) {
  	nodeLog.append(s);
  }

  public void loglnPerNode(String s) {
  	logPerNode(s);
  	logPerNode(Log.NL);
  }


  public PathTreeNode getParent() {
    return parent;
  }

  public void setParent(PathTreeNode parent) {
	this.parent = parent;
}

public PathBranch getBranchTaken() {
    return branchTaken;
  }


  /**
   * @return null if we do not have such a child
   */
  public PathTreeNode getChild(PathBranch cond) {
    for (PathTreeNode n: children)
      if (n.branchTaken == cond)
        return n;

    return null;
  }


  @Override
  public String toString() {
    return (nodeLog.length()>0) ? nodeLog.toString() : branchTaken.toString();
  }
}
