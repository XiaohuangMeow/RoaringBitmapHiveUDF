package roaringbitmap.udf;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredJavaObject;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.junit.Test;
import org.roaringbitmap.longlong.Roaring64Bitmap;
import roaringbitmap.utils.RoaringBitmapSerializer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class TestRoaringBitmapClear {

    @Test
    public void testRoaringBitmapClearNull() throws HiveException {
        RoaringBitmapClear udf = new RoaringBitmapClear();
        ObjectInspector arg1OI= PrimitiveObjectInspectorFactory.writableBinaryObjectInspector;
        ObjectInspector arg2OI= PrimitiveObjectInspectorFactory.writableLongObjectInspector;
        ObjectInspector arg3OI= PrimitiveObjectInspectorFactory.writableLongObjectInspector;

        ObjectInspector[] arguments={arg1OI,arg2OI,arg3OI};
        udf.initialize(arguments);

        Roaring64Bitmap bitmap=new Roaring64Bitmap();
        bitmap.addRange(1L,3L);

        DeferredObject value1OI=new DeferredJavaObject(RoaringBitmapSerializer.serialize(bitmap));
        DeferredObject value2OI=new DeferredJavaObject(null);
        DeferredObject value3OI=new DeferredJavaObject(new LongWritable(3L));

        DeferredObject[] args={value1OI,value2OI,value3OI};

        Object output = udf.evaluate(args);
        assertNull(output);
    }

    @Test
    public void testRoaringBitmapClearNormal() throws HiveException {
        RoaringBitmapClear udf = new RoaringBitmapClear();
        ObjectInspector arg1OI= PrimitiveObjectInspectorFactory.writableBinaryObjectInspector;
        ObjectInspector arg2OI= PrimitiveObjectInspectorFactory.writableLongObjectInspector;
        ObjectInspector arg3OI= PrimitiveObjectInspectorFactory.writableLongObjectInspector;

        ObjectInspector[] arguments={arg1OI,arg2OI,arg3OI};
        udf.initialize(arguments);

        Roaring64Bitmap bitmap1=new Roaring64Bitmap();
        bitmap1.addRange(1L,4L);


        DeferredObject value1OI=new DeferredJavaObject(RoaringBitmapSerializer.serialize(bitmap1));
        DeferredObject value2OI=new DeferredJavaObject(new LongWritable(2L));
        DeferredObject value3OI=new DeferredJavaObject(new LongWritable(3L));

        DeferredObject[] args={value1OI,value2OI,value3OI};
        BytesWritable output=(BytesWritable) udf.evaluate(args);
        Roaring64Bitmap outputBitmap= RoaringBitmapSerializer.deserialize(output);
        Roaring64Bitmap expectedBitmap=new Roaring64Bitmap();
        expectedBitmap.addLong(1L);
        expectedBitmap.addLong(3L);

        assertEquals(expectedBitmap,outputBitmap);
    }




}
