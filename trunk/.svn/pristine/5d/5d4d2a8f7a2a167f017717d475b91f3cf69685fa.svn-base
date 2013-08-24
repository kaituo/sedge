package edu.umass.cs.pig.util;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.security.SecureRandom;
import java.math.BigInteger;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.newplan.logical.relational.LogicalSchema;
import org.apache.pig.newplan.logical.relational.LogicalSchema.LogicalFieldSchema;
import org.apache.pig.pen.util.ExampleTuple;

public class DefaultTuple {
	Random r = new Random();
	LogicalSchema schema;
	List<LogicalSchema> schemas;
	DataByteArrayConverter converter;
	
	public DefaultTuple(LogicalSchema schema, List<LogicalSchema> schemas) {
		r = new Random();
		this.schema = schema;
		this.schemas = schemas;
		converter = new DataByteArrayConverter();
	}

	/**
	 * Doesn't support mutate map and bag yet
	 * @param old
	 * @return
	 * @throws FrontendException
	 */
	public Tuple newDefaultTuple() throws FrontendException {
		Tuple inputT = TupleFactory.getInstance().newTuple(
				schema.size());
		Tuple nExampleTpl = new ExampleTuple(inputT);
		int i = 0;
		for(LogicalFieldSchema f: schema.getFields()) {
			try {
				
					String alias = f.alias;
					byte type = -1;
					for (LogicalSchema s : schemas) {
						LogicalFieldSchema fmatch = s.getFieldSubNameMatch(alias);
			            if(fmatch != null)
			            	type = fmatch.type;
			        }
					if(type == -1)
						throw new FrontendException("Cannot find matching field from all known schemas!");
		            switch (type) {
		                case DataType.INTEGER:
		                	// All data are encoded as DataByteArray by Pig
		                	int newVal = randomInt();
		                	DataByteArray da = converter.toDataByteArray(newVal);
			        		nExampleTpl.set(i, da);
		                    break;
		                case DataType.LONG:
		                	long newVal2 = randomLong();
		                	DataByteArray da2 = converter.toDataByteArray(newVal2);
			        		nExampleTpl.set(i, da2);
		                    break;
		                case DataType.FLOAT:
		                	float newVal4 = randomFloat();
		                	DataByteArray da4 = converter.toDataByteArray(newVal4);
			        		nExampleTpl.set(i, da4);
		                    break;
		                case DataType.DOUBLE:
		                	double newVal3 = randomDouble();
		                	DataByteArray da3 = converter.toDataByteArray(newVal3);
			        		nExampleTpl.set(i, da3);
		                    break;
		                case DataType.CHARARRAY:
		                	String newVal5 = randomString();
		                	DataByteArray da5 = converter.toDataByteArray(newVal5);
							nExampleTpl.set(i, da5);
		                	break;
		                case DataType.BYTEARRAY:
			        		nExampleTpl.set(i, new DataByteArray(randomString()));
		                	break;
		                case DataType.BOOLEAN:
		                	nExampleTpl.set(i, Boolean.FALSE);
		                	break;
		                case DataType.MAP:
		                	Map<String, Object> outputMap = new HashMap<String, Object>();
		                	int val = randomInt();
		                	String key = randomString();
		                	outputMap.put(key, val);
		                	nExampleTpl.set(i, outputMap);
		                	break;
		                case DataType.BAG:
		                	DataBag bag = BagFactory.getInstance().newDefaultBag();
		                	Tuple tp2 = TupleFactory.getInstance().newTuple(1);
		                	Map<String, Object> outputMap2 = new HashMap<String, Object>();
		                	int val2 = randomInt();
		                	String key2 = randomString();
		                	outputMap2.put(key2, val2);
		                	tp2.set(0, outputMap2);
		                	bag.add(tp2);
		                	nExampleTpl.set(i, bag);
		                	break;
		                default:
		                    throw new FrontendException("Unsupported number type");
		            }
		            i++;
				
			} catch (ExecException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
            
        }
		
//		for (int i = 0; i < old.size(); i++) {
//        	Object d = null;
//			try {
//				d = old.get(i);
//				if(d != null) {
//					if(d instanceof String) {
//						String ds = (String)d;
//						nExampleTpl.set(i, mutateString(ds));
//		        	} else if(d instanceof Integer) {
//		        		int newVal = ((Integer)d).intValue() + r.nextInt();
//		        		nExampleTpl.set(i, newVal);
//		        	} else if(d instanceof Long) {
//		        		long newVal2 = ((Integer)d).longValue() + r.nextLong();
//		        		nExampleTpl.set(i, newVal2);
//		        	} else if(d instanceof Double) {
//		        		double newVal3 = ((Double)d).doubleValue() + r.nextDouble();
//		        		nExampleTpl.set(i, newVal3);
//		        	} else if(d instanceof Float) {
//		        		float newVal4 = ((Float)d).floatValue() + r.nextFloat();
//		        		nExampleTpl.set(i, newVal4);
//		        	} else if(d instanceof DataByteArray) {
//		        		String str = ((DataByteArray) d).toString();
//		        		nExampleTpl.set(i, new DataByteArray(mutateString(str)));
//		        	} else 
//		        		throw new FrontendException("Unsupported number format!");
//				} else {
//					throw new FrontendException("Unsupported number format!");
//				}
//			} catch (ExecException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}

		return nExampleTpl;
	}
	
	private int randomInt() {
		int ret = r.nextInt(10);
		return ret;
	}
	
	private double randomDouble() {
		double ret = r.nextDouble();
		return ret;
	}
	
	private float randomFloat() {
		float ret = r.nextFloat();
		return ret;
	}
	
	private long randomLong() {
		long ret = nextLong(r, 10);
		return ret;
	}
	
	private String randomString() {
		SecureRandom random = new SecureRandom();
		return new BigInteger(130, random).toString(32);
	}
	

	private long nextLong(Random rng, long n) {
		// error checking and 2^x checking removed for simplicity.
		long bits, val;
		do {
			bits = (rng.nextLong() << 1) >>> 1;
			val = bits % n;
		} while (bits - val + (n - 1) < 0L);
		return val;
	}

}
