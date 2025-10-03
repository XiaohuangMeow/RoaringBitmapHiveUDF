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

public class TestRoaringBitmapIsEmpty {

    @Test
    public void testRoaringBitmapIsEmptyNull() throws HiveException {
        RoaringBitmapIsEmpty udf = new RoaringBitmapIsEmpty();
        ObjectInspector arg1OI= PrimitiveObjectInspectorFactory.writableBinaryObjectInspector;
        ObjectInspector arg2OI= PrimitiveObjectInspectorFactory.writableBinaryObjectInspector;

        ObjectInspector[] arguments={arg1OI};
        udf.initialize(arguments);


        DeferredObject value1OI=new DeferredJavaObject(null);

        DeferredObject[] args={value1OI};
        Object output = udf.evaluate(args);
        assertNull(output);
    }

    @Test
    public void testRoaringBitmapIsEmptyTrue() throws HiveException {
        RoaringBitmapIsEmpty udf = new RoaringBitmapIsEmpty();
        ObjectInspector arg1OI= PrimitiveObjectInspectorFactory.writableBinaryObjectInspector;

        ObjectInspector[] arguments={arg1OI};
        udf.initialize(arguments);

        Roaring64NavigableMap bitmap1=new Roaring64NavigableMap();

        DeferredObject value1OI=new DeferredJavaObject(RoaringBitmapSerializer.serialize(bitmap1));

        DeferredObject[] args={value1OI};

        BooleanWritable output=(BooleanWritable) udf.evaluate(args);
        assertTrue(output.get());
    }

    @Test
    public void testRoaringBitmapIsEmptyFalse() throws HiveException {
        RoaringBitmapIsEmpty udf = new RoaringBitmapIsEmpty();
        ObjectInspector arg1OI= PrimitiveObjectInspectorFactory.writableBinaryObjectInspector;

        ObjectInspector[] arguments={arg1OI};
        udf.initialize(arguments);

        Roaring64NavigableMap bitmap1=new Roaring64NavigableMap();
        bitmap1.addLong(3L);

        DeferredObject value1OI=new DeferredJavaObject(RoaringBitmapSerializer.serialize(bitmap1));

        DeferredObject[] args={value1OI};
        BooleanWritable output=(BooleanWritable) udf.evaluate(args);
        assertFalse(output.get());
    }




}
