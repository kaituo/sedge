package edu.umass.cs.pig.dataflow;

import java.io.Serializable;

/**
 * remember the alias and type during processing of Join.
 * The type of a variable could be changed as upstream
 * processing goes.  For example, A = load A using PigStorage()
 * as (x:int).  B = load B using PigStorage() as (x:int). C=
 * Join A by x, B by x.  Both x are type of int in Join operator.
 * But in both A and B before using PigStorage, both x are of type
 * bytearray.
 * @author kaituo
 *
 */
public class VarInfo implements Serializable {
	
	private static final long serialVersionUID = 1L;
	protected String alias;
	protected byte type;
	protected long uid;
	
	public VarInfo(String alias, byte type) {
		this.alias = alias;
		this.type = type;
		this.uid = 0;
	}
	
	public VarInfo(long uid, byte type) {
		this.alias = null;
		this.type = type;
		this.uid = uid;
	}
	
	public long getUid() {
		return uid;
	}
	public void setUid(long uid) {
		this.uid = uid;
	}
	
	public String getAlias() {
		return alias;
	}
	
	public void setAlias(String alias) {
		this.alias = alias;
	}
	
	public byte getType() {
		return type;
	}
	
	public void setType(byte type) {
		this.type = type;
	}
}
