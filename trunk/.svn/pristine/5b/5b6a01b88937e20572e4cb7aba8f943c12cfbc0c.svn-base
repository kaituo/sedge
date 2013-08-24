package edu.umass.cs.pig.dataflow;

import java.io.Serializable;
import java.util.List;

import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.newplan.logical.relational.LogicalSchema;

import edu.umass.cs.pig.vm.QueryManager;

public class LoadArgs implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	Tuple exampleTuple;
	QueryManager q;
	LogicalSchema schema; 
	//LogicalSchema uidOnlySchema;
	DataBag newInputData;
	DataBag inputData;
	List<LogicalSchema> allSchema;
	
	public LoadArgs(Tuple exampleTuple, QueryManager q, LogicalSchema schema,
			DataBag newInputData, DataBag inputData) {
		super();
		this.exampleTuple = exampleTuple;
		this.q = q;
		this.schema = schema;
		this.newInputData = newInputData;
		this.inputData = inputData;
		//this.uidOnlySchema = uidOnlySchema;
		
	}
	
	public Tuple getExampleTuple() {
		return exampleTuple;
	}
	public void setExampleTuple(Tuple exampleTuple) {
		this.exampleTuple = exampleTuple;
	}
	public QueryManager getQ() {
		return q;
	}
	public void setQ(QueryManager q) {
		this.q = q;
	}
	public LogicalSchema getSchema() {
		return schema;
	}
	public void setSchema(LogicalSchema schema) {
		this.schema = schema;
	}
	public DataBag getNewInputData() {
		return newInputData;
	}
	public void setNewInputData(DataBag newInputData) {
		this.newInputData = newInputData;
	}
	public DataBag getInputData() {
		return inputData;
	}
	public void setInputData(DataBag inputData) {
		this.inputData = inputData;
	}

	public List<LogicalSchema> getAllSchema() {
		return allSchema;
	}

	public void setAllSchema(List<LogicalSchema> allSchema) {
		this.allSchema = allSchema;
	}

//	public LogicalSchema getUidOnlySchema() {
//		return uidOnlySchema;
//	}
//
//	public void setUidOnlySchema(LogicalSchema uidOnlySchema) {
//		this.uidOnlySchema = uidOnlySchema;
//	}

}
