package roaringbitmap.udf;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredJavaObject;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.BytesWritable;
import org.junit.Test;
import org.roaringbitmap.longlong.Roaring64Bitmap;
import roaringbitmap.utils.RoaringBitmapSerializer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class TestRoaringBitmapXor {

    @Test
    public void testRoaringBitmapXorNull() throws HiveException {
        RoaringBitmapXor udf = new RoaringBitmapXor();
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
    public void testRoaringBitmapXorNormal() throws HiveException {
        RoaringBitmapXor udf = new RoaringBitmapXor();
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

        BytesWritable output=(BytesWritable) udf.evaluate(args);
        Roaring64Bitmap outputBitmap= RoaringBitmapSerializer.deserialize(output);
        Roaring64Bitmap expectedBitmap=new Roaring64Bitmap();
        expectedBitmap.addLong(1L);
        expectedBitmap.addLong(3L);
        assertEquals(expectedBitmap,outputBitmap);
    }




}
