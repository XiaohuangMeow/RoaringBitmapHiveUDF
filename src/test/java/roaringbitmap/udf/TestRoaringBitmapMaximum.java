package roaringbitmap.udf;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredJavaObject;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.LongWritable;
import org.junit.Test;
import org.roaringbitmap.longlong.Roaring64NavigableMap;
import roaringbitmap.utils.RoaringBitmapSerializer;

import static org.junit.Assert.*;

public class TestRoaringBitmapMaximum {

    @Test
    public void testRoaringBitmapMaximumNull() throws HiveException {
        RoaringBitmapMaximum udf = new RoaringBitmapMaximum();
        ObjectInspector arg1OI= PrimitiveObjectInspectorFactory.writableBinaryObjectInspector;

        ObjectInspector[] arguments={arg1OI};
        udf.initialize(arguments);


        DeferredObject value1OI=new DeferredJavaObject(null);

        DeferredObject[] args={value1OI};
        Object output = udf.evaluate(args);
        assertNull(output);
    }

    @Test
    public void testRoaringBitmapMaximumEmpty() throws HiveException {
        RoaringBitmapMaximum udf = new RoaringBitmapMaximum();
        ObjectInspector arg1OI= PrimitiveObjectInspectorFactory.writableBinaryObjectInspector;

        ObjectInspector[] arguments={arg1OI};
        udf.initialize(arguments);


        Roaring64NavigableMap bitmap1=new Roaring64NavigableMap();
        DeferredObject value1OI=new DeferredJavaObject(RoaringBitmapSerializer.serialize(bitmap1));

        DeferredObject[] args={value1OI};
        Object output = udf.evaluate(args);
        assertNull(output);
    }

    @Test
    public void testRoaringRoaringBitmapMaximumNormal() throws HiveException {
        RoaringBitmapMaximum udf = new RoaringBitmapMaximum();
        ObjectInspector arg1OI= PrimitiveObjectInspectorFactory.writableBinaryObjectInspector;

        ObjectInspector[] arguments={arg1OI};
        udf.initialize(arguments);
        // case1
        Roaring64NavigableMap bitmap1=new Roaring64NavigableMap();
        bitmap1.addRange(3L,6L);
        bitmap1.addRange(-300L,-100L);

        DeferredObject value1OI=new DeferredJavaObject(RoaringBitmapSerializer.serialize(bitmap1));

        DeferredObject[] args1={value1OI};

        LongWritable output=(LongWritable) udf.evaluate(args1);
        assertEquals(5L,output.get());

    }


}
