package roaringbitmap.udf;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredJavaObject;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.BooleanWritable;
import org.junit.Test;
import org.roaringbitmap.longlong.Roaring64Bitmap;
import roaringbitmap.utils.RoaringBitmapSerializer;

import static org.junit.Assert.*;

public class TestRoaringBitmapNotEqual {

    @Test
    public void testRoaringBitmapNotEqualNull() throws HiveException {
        RoaringBitmapNotEqual udf = new RoaringBitmapNotEqual();
        ObjectInspector arg1OI= PrimitiveObjectInspectorFactory.writableBinaryObjectInspector;
        ObjectInspector arg2OI= PrimitiveObjectInspectorFactory.writableBinaryObjectInspector;

        ObjectInspector[] arguments={arg1OI,arg2OI};
        udf.initialize(arguments);

        Roaring64Bitmap bitmap1=new Roaring64Bitmap();
        bitmap1.addRange(1L,3L);

        DeferredObject value1OI=new DeferredJavaObject(RoaringBitmapSerializer.serialize(bitmap1));

        DeferredObject[] args={value1OI,null};
        Object output = udf.evaluate(args);
        assertNull(output);
    }


    @Test
    public void testRoaringBitmapNotEqualEmpty() throws HiveException {
        RoaringBitmapNotEqual udf = new RoaringBitmapNotEqual();
        ObjectInspector arg1OI= PrimitiveObjectInspectorFactory.writableBinaryObjectInspector;
        ObjectInspector arg2OI= PrimitiveObjectInspectorFactory.writableBinaryObjectInspector;

        ObjectInspector[] arguments={arg1OI,arg2OI};
        udf.initialize(arguments);

        Roaring64Bitmap bitmap1=new Roaring64Bitmap();
        Roaring64Bitmap bitmap2=new Roaring64Bitmap();

        DeferredObject value1OI=new DeferredJavaObject(RoaringBitmapSerializer.serialize(bitmap1));
        DeferredObject value2OI=new DeferredJavaObject(RoaringBitmapSerializer.serialize(bitmap2));

        DeferredObject[] args={value1OI,value2OI};
        BooleanWritable output=(BooleanWritable) udf.evaluate(args);
        assertFalse(output.get());
    }

    @Test
    public void testRoaringBitmapNotEqualFalse() throws HiveException {
        RoaringBitmapNotEqual udf = new RoaringBitmapNotEqual();
        ObjectInspector arg1OI= PrimitiveObjectInspectorFactory.writableBinaryObjectInspector;
        ObjectInspector arg2OI= PrimitiveObjectInspectorFactory.writableBinaryObjectInspector;

        ObjectInspector[] arguments={arg1OI,arg2OI};
        udf.initialize(arguments);

        Roaring64Bitmap bitmap1=new Roaring64Bitmap();
        bitmap1.addRange(1L,3L);
        Roaring64Bitmap bitmap2=new Roaring64Bitmap();
        bitmap2.addRange(1L,3L);


        DeferredObject value1OI=new DeferredJavaObject(RoaringBitmapSerializer.serialize(bitmap1));
        DeferredObject value2OI=new DeferredJavaObject(RoaringBitmapSerializer.serialize(bitmap2));

        DeferredObject[] args={value1OI,value2OI};

        BooleanWritable output=(BooleanWritable) udf.evaluate(args);
        assertFalse(output.get());
    }

    @Test
    public void testRoaringBitmapNotEqualTrue() throws HiveException {
        RoaringBitmapNotEqual udf = new RoaringBitmapNotEqual();
        ObjectInspector arg1OI= PrimitiveObjectInspectorFactory.writableBinaryObjectInspector;
        ObjectInspector arg2OI= PrimitiveObjectInspectorFactory.writableBinaryObjectInspector;

        ObjectInspector[] arguments={arg1OI,arg2OI};
        udf.initialize(arguments);

        Roaring64Bitmap bitmap1=new Roaring64Bitmap();
        bitmap1.addRange(1L,3L);
        Roaring64Bitmap bitmap2=new Roaring64Bitmap();
        bitmap2.addRange(2L,4L);

        DeferredObject value1OI=new DeferredJavaObject(RoaringBitmapSerializer.serialize(bitmap1));
        DeferredObject value2OI=new DeferredJavaObject(RoaringBitmapSerializer.serialize(bitmap2));

        DeferredObject[] args={value1OI,value2OI};
        BooleanWritable output=(BooleanWritable) udf.evaluate(args);
        assertTrue(output.get());
    }




}
