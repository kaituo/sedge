package edu.umass.cs.pig.test.sigmod;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.pig.EvalFunc;
import org.apache.pig.FuncSpec;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

public class HASH extends EvalFunc<Integer> {
	public Integer exec(Tuple input) throws IOException {

		if (input == null || input.size() == 0)
			return null;
		try {
			Integer y = (Integer) input.get(0);
			int hash = 15;

			hash = hash * 35 + y;
			return hash;
		} catch (Exception e) {
			throw new IOException("Caught exception processing input row ", e);

		}
	}
	
	@Override
    public List<FuncSpec> getArgToFuncMapping() throws FrontendException {
        List<FuncSpec> funcList = new ArrayList<FuncSpec>();
        List<Schema.FieldSchema> fields = new ArrayList<Schema.FieldSchema>();
        fields.add(new Schema.FieldSchema(null, DataType.INTEGER));
        funcList.add(new FuncSpec(this.getClass().getName(), new Schema(fields)));

        return funcList;
    }

}
