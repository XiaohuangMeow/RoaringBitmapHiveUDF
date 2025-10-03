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

public class TestRoaringBitmapFill {

    @Test
    public void testRoaringBitmapFillNull() throws HiveException {
        RoaringBitmapFill udf = new RoaringBitmapFill();
        ObjectInspector arg1OI= PrimitiveObjectInspectorFactory.writableBinaryObjectInspector;
        ObjectInspector arg2OI= PrimitiveObjectInspectorFactory.writableLongObjectInspector;
        ObjectInspector arg3OI= PrimitiveObjectInspectorFactory.writableLongObjectInspector;

        ObjectInspector[] arguments={arg1OI,arg2OI,arg3OI};
        udf.initialize(arguments);

        Roaring64NavigableMap bitmap=new Roaring64NavigableMap();
        bitmap.addRange(1L,3L);

        DeferredObject value1OI=new DeferredJavaObject(RoaringBitmapSerializer.serialize(bitmap));
        DeferredObject value2OI=new DeferredJavaObject(null);
        DeferredObject value3OI=new DeferredJavaObject(new LongWritable(3L));

        DeferredObject[] args={value1OI,value2OI,value3OI};

        Object output = udf.evaluate(args);
        assertNull(output);
    }

    @Test
    public void testRoaringBitmapFillNormal() throws HiveException {
        RoaringBitmapFill udf = new RoaringBitmapFill();
        ObjectInspector arg1OI= PrimitiveObjectInspectorFactory.writableBinaryObjectInspector;
        ObjectInspector arg2OI= PrimitiveObjectInspectorFactory.writableLongObjectInspector;
        ObjectInspector arg3OI= PrimitiveObjectInspectorFactory.writableLongObjectInspector;

        ObjectInspector[] arguments={arg1OI,arg2OI,arg3OI};
        udf.initialize(arguments);

        Roaring64NavigableMap bitmap1=new Roaring64NavigableMap();
        bitmap1.addRange(1L,3L);


        DeferredObject value1OI=new DeferredJavaObject(RoaringBitmapSerializer.serialize(bitmap1));
        DeferredObject value2OI=new DeferredJavaObject(new LongWritable(3L));
        DeferredObject value3OI=new DeferredJavaObject(new LongWritable(7L));

        DeferredObject[] args={value1OI,value2OI,value3OI};
        BytesWritable output=(BytesWritable) udf.evaluate(args);
        Roaring64NavigableMap outputBitmap= RoaringBitmapSerializer.deserialize(output);
        Roaring64NavigableMap expectedBitmap=new Roaring64NavigableMap();
        expectedBitmap.addRange(1L,7L);

        assertEquals(expectedBitmap,outputBitmap);
    }




}
