package edu.umass.cs.pig.dataflow;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.pig.backend.executionengine.ExecException;

import edu.umass.cs.pig.cntr.PltRelCntr;
import edu.umass.cs.pig.vm.PathList;

public class JoinPointConnector {
	public void connectJoinInput(PathList parent, List<VarInfo> groupCols0, PathList child, List<VarInfo> groupCols ) {
		assert (parent != null) && (child != null);

		PltRelation rel = new PltRelation(parent, child);

		Iterator<VarInfo> itpparent = groupCols0.iterator();
		Iterator<VarInfo> itpchild = groupCols.iterator();

		while (itpparent.hasNext() && itpchild.hasNext()) {
			VarInfo vp = itpparent.next();
			VarInfo vc = itpchild.next();
			rel.child_to_parent_map.put(vc, vp);
			rel.parent_to_child_map.put(vp, vc);
		}
	}
	
	public void connectPathListWithAggr(PathList parent, Map<VarInfo, PltRelCntr> relMap, PathList child) {
		assert (parent != null) && (child != null);

		PltAggrRelation rel = new PltAggrRelation(parent, child);
		
		 for (Map.Entry<VarInfo, PltRelCntr> entry : relMap.entrySet()) {
			 VarInfo vp = entry.getKey();
			 PltRelCntr vc = entry.getValue();
			 rel.child_to_parent_map.put(vc, vp);
			 rel.parent_to_child_map.put(vp, vc);
          }
	}

}
