package roaringbitmap.test;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableLongObjectInspector;
import org.apache.hadoop.io.LongWritable;

public class GenericAddTest extends GenericUDF {

    PrimitiveObjectInspector i1;
    PrimitiveObjectInspector i2;


    @Override
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        this.i1=(WritableLongObjectInspector)objectInspectors[0];
        this.i2=(WritableLongObjectInspector)objectInspectors[1];
        return PrimitiveObjectInspectorFactory.writableLongObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
        if (deferredObjects[0]==null || deferredObjects[1]== null) {
            return null;
        }
        if (deferredObjects[0].get()==null || deferredObjects[1].get()== null) {
            return null;
        }

        long value1= PrimitiveObjectInspectorUtils.getLong(deferredObjects[0].get(),i1);
        long value2= PrimitiveObjectInspectorUtils.getLong(deferredObjects[1].get(),i2);
        return new LongWritable(value1+value2);
    }

    @Override
    public String getDisplayString(String[] strings) {
        return "";
    }
}
