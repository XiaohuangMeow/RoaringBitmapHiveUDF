package roaringbitmap.udf;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredJavaObject;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.BytesWritable;
import org.junit.Test;
import org.roaringbitmap.longlong.Roaring64NavigableMap;
import roaringbitmap.utils.RoaringBitmapSerializer;

import static org.junit.Assert.*;

public class TestRoaringBitmapAnd {

    @Test
    public void testRoaringBitmapAndNull() throws HiveException {
        RoaringBitmapAnd udf = new RoaringBitmapAnd();
        ObjectInspector arg1OI= PrimitiveObjectInspectorFactory.writableBinaryObjectInspector;
        ObjectInspector arg2OI= PrimitiveObjectInspectorFactory.writableBinaryObjectInspector;

        ObjectInspector[] arguments={arg1OI,arg2OI};
        udf.initialize(arguments);

        Roaring64NavigableMap bitmap1=new Roaring64NavigableMap();
        bitmap1.addRange(1L,3L);

        DeferredObject value1OI=new DeferredJavaObject(RoaringBitmapSerializer.serialize(bitmap1));

        DeferredObject[] args={value1OI,null};
        BytesWritable output=(BytesWritable) udf.evaluate(args);
        assertNull(output);
    }

    @Test
    public void testRoaringBitmapAndNormal() throws HiveException {
        RoaringBitmapAnd udf = new RoaringBitmapAnd();
        ObjectInspector arg1OI= PrimitiveObjectInspectorFactory.writableBinaryObjectInspector;
        ObjectInspector arg2OI= PrimitiveObjectInspectorFactory.writableBinaryObjectInspector;

        ObjectInspector[] arguments={arg1OI,arg2OI};
        udf.initialize(arguments);

        Roaring64NavigableMap bitmap1=new Roaring64NavigableMap();
        bitmap1.addRange(1L,3L);
        Roaring64NavigableMap bitmap2=new Roaring64NavigableMap();
        bitmap2.addRange(2L,5L);


        DeferredObject value1OI=new DeferredJavaObject(RoaringBitmapSerializer.serialize(bitmap1));
        DeferredObject value2OI=new DeferredJavaObject(RoaringBitmapSerializer.serialize(bitmap2));

        DeferredObject[] args={value1OI,value2OI};
        BytesWritable output=(BytesWritable) udf.evaluate(args);
        Roaring64NavigableMap outputBitmap= RoaringBitmapSerializer.deserialize(output);
        Roaring64NavigableMap expectedBitmap=new Roaring64NavigableMap();
        expectedBitmap.addLong(2L);
        assertEquals(expectedBitmap,outputBitmap);
    }




}
