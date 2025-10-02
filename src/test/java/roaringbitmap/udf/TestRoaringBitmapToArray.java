package roaringbitmap.udf;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredJavaObject;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.LongWritable;
import org.junit.Test;
import org.roaringbitmap.longlong.Roaring64Bitmap;
import roaringbitmap.utils.RoaringBitmapSerializer;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class TestRoaringBitmapToArray {

    @Test
    public void testRoaringBitmapToArrayNull() throws HiveException {
        RoaringBitmapToArray udf = new RoaringBitmapToArray();
        ObjectInspector arg1OI = PrimitiveObjectInspectorFactory.writableBinaryObjectInspector;
        ObjectInspector[] arguments={arg1OI};

        udf.initialize(arguments);

        DeferredObject[] args={null};
        Object output=udf.evaluate(args);

        assertNull(output);
    }

    @Test
    public void testRoaringBitmapToArrayNormal() throws HiveException {
        RoaringBitmapToArray udf = new RoaringBitmapToArray();

        ObjectInspector arg1OI = PrimitiveObjectInspectorFactory.writableBinaryObjectInspector;
        ObjectInspector[] arguments={arg1OI};

        udf.initialize(arguments);

        Roaring64Bitmap bitmap1=new Roaring64Bitmap();
        bitmap1.addRange(1L,4L);
        DeferredObject value1OI=new DeferredJavaObject(RoaringBitmapSerializer.serialize(bitmap1));
        DeferredObject[] args={value1OI};

        List<LongWritable> result=(List<LongWritable>)udf.evaluate(args);

        assertEquals(1L,result.get(0).get());
        assertEquals(2L,result.get(1).get());
        assertEquals(3L,result.get(2).get());
    }




}
