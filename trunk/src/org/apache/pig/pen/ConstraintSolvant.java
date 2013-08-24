package org.apache.pig.pen;

import org.apache.pig.newplan.logical.expression.LogicalExpressionPlan;
import org.apache.pig.newplan.logical.relational.LogicalSchema;

public class ConstraintSolvant {
	LogicalSchema schema;
	LogicalExpressionPlan plan;
    boolean invert;
    
	public LogicalSchema getSchema() {
		return schema;
	}
	public void setSchema(LogicalSchema schema) {
		this.schema = schema;
	}
	public LogicalExpressionPlan getPlan() {
		return plan;
	}
	public void setPlan(LogicalExpressionPlan plan) {
		this.plan = plan;
	}
	public boolean isInvert() {
		return invert;
	}
	public void setInvert(boolean invert) {
		this.invert = invert;
	}

}
