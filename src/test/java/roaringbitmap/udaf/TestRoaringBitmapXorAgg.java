package roaringbitmap.udaf;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.BytesWritable;
import org.junit.Test;
import org.roaringbitmap.longlong.Roaring64NavigableMap;
import roaringbitmap.utils.RoaringBitmapSerializer;

import static org.junit.Assert.*;

public class TestRoaringBitmapXorAgg {

    @Test
    public void testRoaringBitmapXorAggNormal() throws HiveException {
        RoaringBitmapXorAgg rb_xor_agg=new RoaringBitmapXorAgg();

        //part1
        GenericUDAFEvaluator eval1=rb_xor_agg.getEvaluator(
                new TypeInfo[]{TypeInfoFactory.binaryTypeInfo}
        );

        ObjectInspector arg1OI= PrimitiveObjectInspectorFactory.writableBinaryObjectInspector;
        ObjectInspector[] arguments1={arg1OI};
        GenericUDAFEvaluator.AggregationBuffer buffer1=eval1.getNewAggregationBuffer();
        eval1.init(GenericUDAFEvaluator.Mode.PARTIAL1,arguments1);
        // partial1
        // test iterate, terminatePartial
        Roaring64NavigableMap bitmap_a1=new Roaring64NavigableMap();
        Roaring64NavigableMap bitmap_a2=new Roaring64NavigableMap();
        Roaring64NavigableMap bitmap_a3=new Roaring64NavigableMap();
        bitmap_a1.addRange(1L,5L);
        bitmap_a2.addRange(2L,6L);

        BytesWritable input_a1= RoaringBitmapSerializer.serialize(bitmap_a1);
        BytesWritable input_a2= RoaringBitmapSerializer.serialize(bitmap_a2);
        BytesWritable input_a3= RoaringBitmapSerializer.serialize(bitmap_a3);

        eval1.iterate(buffer1,new Object[]{input_a1});
        eval1.iterate(buffer1,new Object[]{input_a2});
        eval1.iterate(buffer1,new Object[]{input_a3});

        Object partialResult1=eval1.terminatePartial(buffer1);
        assertNotNull(partialResult1);
        Roaring64NavigableMap partialBitmap1= RoaringBitmapSerializer.deserialize((BytesWritable)partialResult1);
        Roaring64NavigableMap expectedBitmap1=new Roaring64NavigableMap();
        expectedBitmap1.addLong(1L);
        expectedBitmap1.addLong(5L);
        assertEquals(expectedBitmap1,partialBitmap1);

        // part2
        GenericUDAFEvaluator eval2=rb_xor_agg.getEvaluator(
                new TypeInfo[]{TypeInfoFactory.binaryTypeInfo}
        );

        ObjectInspector arg2OI= PrimitiveObjectInspectorFactory.writableBinaryObjectInspector;
        ObjectInspector[] arguments2={arg2OI};
        GenericUDAFEvaluator.AggregationBuffer buffer2=eval2.getNewAggregationBuffer();
        eval2.init(GenericUDAFEvaluator.Mode.PARTIAL1,arguments2);

        // partial2
        // test iterate, terminatePartial
        Roaring64NavigableMap bitmap_b1=new Roaring64NavigableMap();
        Roaring64NavigableMap bitmap_b2=new Roaring64NavigableMap();
        bitmap_b1.addRange(10L,15L);
        bitmap_b2.addRange(15L,20L);

        BytesWritable input_b1= RoaringBitmapSerializer.serialize(bitmap_b1);
        BytesWritable input_b2= RoaringBitmapSerializer.serialize(bitmap_b2);
        eval2.iterate(buffer2,new Object[]{null});
        eval2.iterate(buffer2,new Object[]{input_b1});
        eval2.iterate(buffer2,new Object[]{input_b2});

        Object partialResult2=eval2.terminatePartial(buffer2);
        assertNotNull(partialResult2);
        Roaring64NavigableMap partialBitmap2= RoaringBitmapSerializer.deserialize((BytesWritable)partialResult2);
        Roaring64NavigableMap expectedBitmap2=new Roaring64NavigableMap();
        expectedBitmap2.addRange(10L,20L);
        assertEquals(expectedBitmap2,partialBitmap2);

        // test merge, terminate
        GenericUDAFEvaluator eval3=rb_xor_agg.getEvaluator(
                new TypeInfo[]{TypeInfoFactory.binaryTypeInfo}
        );

        ObjectInspector arg3OI=PrimitiveObjectInspectorFactory.writableBinaryObjectInspector;
        ObjectInspector[] arguments3={arg3OI};
        GenericUDAFEvaluator.AggregationBuffer buffer3=eval3.getNewAggregationBuffer();
        eval3.init(GenericUDAFEvaluator.Mode.FINAL,arguments3);

        //
        eval3.merge(buffer3,partialResult1);
        eval3.merge(buffer3,partialResult2);

        Object result=eval3.terminate(buffer3);
        assertNotNull(result);
        Roaring64NavigableMap resultBitmap= RoaringBitmapSerializer.deserialize((BytesWritable)result);
        Roaring64NavigableMap expectedBitmap=new Roaring64NavigableMap();
        expectedBitmap.addLong(1L);
        expectedBitmap.addLong(5L);
        expectedBitmap.addRange(10L,20L);
        assertEquals(expectedBitmap,resultBitmap);
    }


    @Test
    public void testRoaringBitmapXorAggEmpty() throws HiveException {
        RoaringBitmapXorAgg rb_xor_agg=new RoaringBitmapXorAgg();

        //part1
        GenericUDAFEvaluator eval1=rb_xor_agg.getEvaluator(
                new TypeInfo[]{TypeInfoFactory.binaryTypeInfo}
        );

        ObjectInspector arg1OI= PrimitiveObjectInspectorFactory.writableBinaryObjectInspector;
        ObjectInspector[] arguments1={arg1OI};
        GenericUDAFEvaluator.AggregationBuffer buffer1=eval1.getNewAggregationBuffer();
        eval1.init(GenericUDAFEvaluator.Mode.PARTIAL1,arguments1);
        // partial1
        // test iterate, terminatePartial
        Roaring64NavigableMap bitmap_a1=new Roaring64NavigableMap();
        Roaring64NavigableMap bitmap_a2=new Roaring64NavigableMap();

        BytesWritable input_a1= RoaringBitmapSerializer.serialize(bitmap_a1);
        BytesWritable input_a2= RoaringBitmapSerializer.serialize(bitmap_a2);

        eval1.iterate(buffer1,new Object[]{input_a1});
        eval1.iterate(buffer1,new Object[]{input_a2});

        Object partialResult1=eval1.terminatePartial(buffer1);
        assertNotNull(partialResult1);
        Roaring64NavigableMap partialBitmap1= RoaringBitmapSerializer.deserialize((BytesWritable)partialResult1);
        assertTrue(partialBitmap1.isEmpty());

        // part2
        GenericUDAFEvaluator eval2=rb_xor_agg.getEvaluator(
                new TypeInfo[]{TypeInfoFactory.binaryTypeInfo}
        );

        ObjectInspector arg2OI= PrimitiveObjectInspectorFactory.writableBinaryObjectInspector;
        ObjectInspector[] arguments2={arg2OI};
        GenericUDAFEvaluator.AggregationBuffer buffer2=eval2.getNewAggregationBuffer();
        eval2.init(GenericUDAFEvaluator.Mode.PARTIAL1,arguments2);

        // partial2
        // test iterate, terminatePartial
        eval2.iterate(buffer2,new Object[]{null});
        eval2.iterate(buffer2,new Object[]{null});

        Object partialResult2=eval2.terminatePartial(buffer2);
        assertNotNull(partialResult2);
        Roaring64NavigableMap partialBitmap2= RoaringBitmapSerializer.deserialize((BytesWritable)partialResult2);
        assertTrue(partialBitmap2.isEmpty());

        // test merge, terminate
        GenericUDAFEvaluator eval3=rb_xor_agg.getEvaluator(
                new TypeInfo[]{TypeInfoFactory.binaryTypeInfo}
        );

        ObjectInspector arg3OI=PrimitiveObjectInspectorFactory.writableBinaryObjectInspector;
        ObjectInspector[] arguments3={arg3OI};
        GenericUDAFEvaluator.AggregationBuffer buffer3=eval3.getNewAggregationBuffer();
        eval3.init(GenericUDAFEvaluator.Mode.FINAL,arguments3);

        //
        eval3.merge(buffer3,partialResult1);
        eval3.merge(buffer3,partialResult2);

        Object result=eval3.terminate(buffer3);
        assertNotNull(result);
        Roaring64NavigableMap resultBitmap= RoaringBitmapSerializer.deserialize((BytesWritable)result);

        assertNotNull(resultBitmap);
        assertTrue(resultBitmap.isEmpty());
    }


    @Test
    public void testRoaringBitmapXorAggNull() throws HiveException {
        RoaringBitmapOrAgg rb_or_agg=new RoaringBitmapOrAgg();

        //part1
        GenericUDAFEvaluator eval3=rb_or_agg.getEvaluator(
                new TypeInfo[]{TypeInfoFactory.binaryTypeInfo}
        );

        ObjectInspector arg3OI= PrimitiveObjectInspectorFactory.writableBinaryObjectInspector;
        ObjectInspector[] arguments3={arg3OI};
        GenericUDAFEvaluator.AggregationBuffer buffer3=eval3.getNewAggregationBuffer();
        eval3.init(GenericUDAFEvaluator.Mode.FINAL,arguments3);

        //
        eval3.merge(buffer3,null);
        eval3.merge(buffer3,null);

        Object result=eval3.terminate(buffer3);
        assertNull(result);
    }



}
