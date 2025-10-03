package roaringbitmap.udf;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredJavaObject;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.BooleanWritable;
import org.junit.Test;
import org.roaringbitmap.longlong.Roaring64NavigableMap;
import roaringbitmap.utils.RoaringBitmapSerializer;

import static org.junit.Assert.*;

public class TestRoaringBitmapIntersect {

    @Test
    public void testRoaringBitmapIntersectNull() throws HiveException {
        RoaringBitmapIntersect udf = new RoaringBitmapIntersect();
        ObjectInspector arg1OI= PrimitiveObjectInspectorFactory.writableBinaryObjectInspector;
        ObjectInspector arg2OI= PrimitiveObjectInspectorFactory.writableBinaryObjectInspector;

        ObjectInspector[] arguments={arg1OI,arg2OI};
        udf.initialize(arguments);

        Roaring64NavigableMap bitmap1=new Roaring64NavigableMap();
        bitmap1.addRange(1L,3L);

        DeferredObject value1OI=new DeferredJavaObject(RoaringBitmapSerializer.serialize(bitmap1));

        DeferredObject[] args={value1OI,null};
        BooleanWritable output=(BooleanWritable) udf.evaluate(args);
        assertNull(output);
    }


    @Test
    public void testRoaringBitmapIntersectEmpty() throws HiveException {
        RoaringBitmapIntersect udf = new RoaringBitmapIntersect();
        ObjectInspector arg1OI= PrimitiveObjectInspectorFactory.writableBinaryObjectInspector;
        ObjectInspector arg2OI= PrimitiveObjectInspectorFactory.writableBinaryObjectInspector;

        ObjectInspector[] arguments={arg1OI,arg2OI};
        udf.initialize(arguments);

        Roaring64NavigableMap bitmap1=new Roaring64NavigableMap();
        Roaring64NavigableMap bitmap2=new Roaring64NavigableMap();

        DeferredObject value1OI=new DeferredJavaObject(RoaringBitmapSerializer.serialize(bitmap1));
        DeferredObject value2OI=new DeferredJavaObject(RoaringBitmapSerializer.serialize(bitmap2));

        DeferredObject[] args={value1OI,value2OI};
        BooleanWritable output=(BooleanWritable) udf.evaluate(args);
        assertFalse(output.get());
    }

    @Test
    public void testRoaringBitmapIntersectTrue() throws HiveException {
        RoaringBitmapIntersect udf = new RoaringBitmapIntersect();
        ObjectInspector arg1OI= PrimitiveObjectInspectorFactory.writableBinaryObjectInspector;
        ObjectInspector arg2OI= PrimitiveObjectInspectorFactory.writableBinaryObjectInspector;

        ObjectInspector[] arguments={arg1OI,arg2OI};
        udf.initialize(arguments);

        Roaring64NavigableMap bitmap1=new Roaring64NavigableMap();
        bitmap1.addRange(1L,3L);
        Roaring64NavigableMap bitmap2=new Roaring64NavigableMap();
        bitmap2.addRange(1L,3L);


        DeferredObject value1OI=new DeferredJavaObject(RoaringBitmapSerializer.serialize(bitmap1));
        DeferredObject value2OI=new DeferredJavaObject(RoaringBitmapSerializer.serialize(bitmap2));

        DeferredObject[] args={value1OI,value2OI};

        BooleanWritable output=(BooleanWritable) udf.evaluate(args);
        assertTrue(output.get());
    }

    @Test
    public void testRoaringBitmapIntersectFalse() throws HiveException {
        RoaringBitmapIntersect udf = new RoaringBitmapIntersect();
        ObjectInspector arg1OI= PrimitiveObjectInspectorFactory.writableBinaryObjectInspector;
        ObjectInspector arg2OI= PrimitiveObjectInspectorFactory.writableBinaryObjectInspector;

        ObjectInspector[] arguments={arg1OI,arg2OI};
        udf.initialize(arguments);

        Roaring64NavigableMap bitmap1=new Roaring64NavigableMap();
        bitmap1.addRange(1L,3L);
        Roaring64NavigableMap bitmap2=new Roaring64NavigableMap();
        bitmap2.addRange(3L,4L);

        DeferredObject value1OI=new DeferredJavaObject(RoaringBitmapSerializer.serialize(bitmap1));
        DeferredObject value2OI=new DeferredJavaObject(RoaringBitmapSerializer.serialize(bitmap2));

        DeferredObject[] args={value1OI,value2OI};
        BooleanWritable output=(BooleanWritable) udf.evaluate(args);
        assertFalse(output.get());
    }




}
