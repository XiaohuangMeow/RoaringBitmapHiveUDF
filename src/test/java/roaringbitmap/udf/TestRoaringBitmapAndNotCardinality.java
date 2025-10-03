package roaringbitmap.udf;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredJavaObject;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.junit.Test;
import org.roaringbitmap.longlong.Roaring64NavigableMap;
import roaringbitmap.utils.RoaringBitmapSerializer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class TestRoaringBitmapAndNotCardinality {

    @Test
    public void testRoaringBitmapAndNotCardinalityNull() throws HiveException {
        RoaringBitmapAndNotCardinality udf = new RoaringBitmapAndNotCardinality();
        ObjectInspector arg1OI= PrimitiveObjectInspectorFactory.writableBinaryObjectInspector;
        ObjectInspector arg2OI= PrimitiveObjectInspectorFactory.writableBinaryObjectInspector;

        ObjectInspector[] arguments={arg1OI,arg2OI};
        udf.initialize(arguments);

        Roaring64NavigableMap bitmap1=new Roaring64NavigableMap();
        bitmap1.addRange(1L,3L);

        DeferredObject value1OI=new DeferredJavaObject(RoaringBitmapSerializer.serialize(bitmap1));
        DeferredObject[] args={null,value1OI};

        BytesWritable output=(BytesWritable) udf.evaluate(args);
        assertNull(output);
    }

    @Test
    public void testRoaringBitmapAndNotCardinalityNormal() throws HiveException {
        RoaringBitmapAndNotCardinality udf = new RoaringBitmapAndNotCardinality();
        ObjectInspector arg1OI= PrimitiveObjectInspectorFactory.writableBinaryObjectInspector;
        ObjectInspector arg2OI= PrimitiveObjectInspectorFactory.writableBinaryObjectInspector;

        ObjectInspector[] arguments={arg1OI,arg2OI};
        udf.initialize(arguments);

        Roaring64NavigableMap bitmap1=new Roaring64NavigableMap();
        bitmap1.addRange(1L,6L);
        Roaring64NavigableMap bitmap2=new Roaring64NavigableMap();
        bitmap2.addRange(3L,10L);


        DeferredObject value1OI=new DeferredJavaObject(RoaringBitmapSerializer.serialize(bitmap1));
        DeferredObject value2OI=new DeferredJavaObject(RoaringBitmapSerializer.serialize(bitmap2));

        DeferredObject[] args={value1OI,value2OI};

        LongWritable output=(LongWritable) udf.evaluate(args);

        assertEquals(2L,output.get());
    }




}
