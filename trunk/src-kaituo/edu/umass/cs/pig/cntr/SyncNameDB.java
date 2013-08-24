package edu.umass.cs.pig.cntr;

import java.util.ArrayList;

public class SyncNameDB {
	
	public String analyzeName(String n, ArrayList<String> nameDB) {
		for (String name : nameDB) {
			if (name.equals(n))
				return n;
			else if (name.indexOf("::" + n) > 0)
				return name;//
		}
		int startColon = n.indexOf("::");
		if(startColon > 0)
			return n.substring(startColon+2);
		else
			return n;
	}
}
