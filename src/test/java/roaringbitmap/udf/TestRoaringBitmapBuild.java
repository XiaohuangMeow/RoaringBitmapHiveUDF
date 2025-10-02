package roaringbitmap.udf;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredJavaObject;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.BytesWritable;
import org.junit.Test;
import org.roaringbitmap.longlong.Roaring64Bitmap;
import roaringbitmap.utils.RoaringBitmapSerializer;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class TestRoaringBitmapBuild {

    @Test
    public void testRoaringBitmapBuildNull() throws HiveException {
        RoaringBitmapBuild udf = new RoaringBitmapBuild();
        ObjectInspector arg1OI= ObjectInspectorFactory.getStandardListObjectInspector(
                PrimitiveObjectInspectorFactory.writableLongObjectInspector);

        ObjectInspector[] arguments={arg1OI};

        udf.initialize(arguments);

        DeferredObject[] args={null};
        Object output=udf.evaluate(args);

        assertNull(output);
    }

    @Test
    public void testRoaringBitmapBuildNormal() throws HiveException {
        RoaringBitmapBuild udf = new RoaringBitmapBuild();

        ObjectInspector arg1OI= ObjectInspectorFactory.getStandardListObjectInspector(
                PrimitiveObjectInspectorFactory.writableLongObjectInspector);

        ObjectInspector[] arguments={arg1OI};
        udf.initialize(arguments);

        List<Long> input= Arrays.asList(1L,2L,null,3L);

        DeferredObject value1OI=new DeferredJavaObject(input);

        DeferredObject[] args={value1OI};

        BytesWritable output=(BytesWritable) udf.evaluate(args);

        Roaring64Bitmap outputBitmap= RoaringBitmapSerializer.deserialize(output);
        Roaring64Bitmap expectedBitmap=new Roaring64Bitmap();

        expectedBitmap.addRange(1L,4L);
        assertEquals(expectedBitmap,outputBitmap);
    }




}
