package roaringbitmap.test;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredJavaObject;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.LongWritable;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class TestGenericAdd {

    @Test
    public void testGenericAddNormal() throws HiveException {
        GenericAddTest udf = new GenericAddTest();
        ObjectInspector arg1OI= PrimitiveObjectInspectorFactory.writableLongObjectInspector;
        ObjectInspector arg2OI= PrimitiveObjectInspectorFactory.writableLongObjectInspector;

        ObjectInspector[] arguments={arg1OI,arg2OI};
        udf.initialize(arguments);

        DeferredObject value1OI=new DeferredJavaObject(new LongWritable(1L));
        DeferredObject value2OI=new DeferredJavaObject(new LongWritable(2L));

        DeferredObject[] args={value1OI,value2OI};
        LongWritable output=(LongWritable) udf.evaluate(args);
        assertEquals(3L,output.get());
    }

    @Test
    public void testGenericAddNull() throws HiveException {
        GenericAddTest udf = new GenericAddTest();
        ObjectInspector arg1OI= PrimitiveObjectInspectorFactory.writableLongObjectInspector;
        ObjectInspector arg2OI= PrimitiveObjectInspectorFactory.writableLongObjectInspector;

        ObjectInspector[] arguments={arg1OI,arg2OI};
        udf.initialize(arguments);

        DeferredObject value1OI=new DeferredJavaObject(new LongWritable(1L));
        DeferredObject value2OI=new DeferredJavaObject(null);

        DeferredObject[] args={value1OI,value2OI};
        LongWritable output=(LongWritable) udf.evaluate(args);
        assertNull(output);

    }




}
