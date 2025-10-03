package roaringbitmap.udaf;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.junit.Test;
import org.roaringbitmap.longlong.Roaring64NavigableMap;
import roaringbitmap.utils.RoaringBitmapSerializer;

import static org.junit.Assert.*;

public class TestRoaringBitmapOrCardinalityAgg {

    @Test
    public void testRoaringBitmapOrCardinalityAggNormal() throws HiveException {
        RoaringBitmapOrCardinalityAgg rb_or_cardinality_agg=new RoaringBitmapOrCardinalityAgg();

        //part1
        GenericUDAFEvaluator eval1=rb_or_cardinality_agg.getEvaluator(
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
        bitmap_a1.addRange(1L,30L);
        bitmap_a2.addRange(10L,40L);
        bitmap_a3.addRange(10L,20L);

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
        expectedBitmap1.addRange(1L,40L);
        assertEquals(expectedBitmap1,partialBitmap1);

        // part2
        GenericUDAFEvaluator eval2=rb_or_cardinality_agg.getEvaluator(
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
        bitmap_b1.addRange(90L,99L);
        bitmap_b2.addRange(80L,95L);

        BytesWritable input_b1= RoaringBitmapSerializer.serialize(bitmap_b1);
        BytesWritable input_b2= RoaringBitmapSerializer.serialize(bitmap_b2);
        eval2.iterate(buffer2,new Object[]{null});
        eval2.iterate(buffer2,new Object[]{input_b1});
        eval2.iterate(buffer2,new Object[]{input_b2});

        Object partialResult2=eval2.terminatePartial(buffer2);
        assertNotNull(partialResult2);
        Roaring64NavigableMap partialBitmap2= RoaringBitmapSerializer.deserialize((BytesWritable)partialResult2);
        Roaring64NavigableMap expectedBitmap2=new Roaring64NavigableMap();
        expectedBitmap2.addRange(80L,99L);
        assertEquals(expectedBitmap2,partialBitmap2);

        // test merge, terminate
        GenericUDAFEvaluator eval3=rb_or_cardinality_agg.getEvaluator(
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
        LongWritable output=(LongWritable) result;
        assertEquals(58L,output.get());

    }


    @Test
    public void testRoaringBitmapOrCardinalityAggEmpty() throws HiveException {
        RoaringBitmapOrCardinalityAgg rb_or_cardinality_agg=new RoaringBitmapOrCardinalityAgg();

        //part1
        GenericUDAFEvaluator eval1=rb_or_cardinality_agg.getEvaluator(
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
        GenericUDAFEvaluator eval2=rb_or_cardinality_agg.getEvaluator(
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
        GenericUDAFEvaluator eval3=rb_or_cardinality_agg.getEvaluator(
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
        LongWritable output=(LongWritable) result;
        assertEquals(0L,output.get());

    }


    @Test
    public void testRoaringBitmapOrCardinalityAggNull() throws HiveException {
        RoaringBitmapOrCardinalityAgg rb_or_agg=new RoaringBitmapOrCardinalityAgg();

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
        assertNotNull(result);
        LongWritable output=(LongWritable) result;
        assertEquals(0L,output.get());
    }



}
