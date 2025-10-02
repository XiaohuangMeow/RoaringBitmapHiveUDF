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
import org.roaringbitmap.longlong.Roaring64Bitmap;
import roaringbitmap.utils.RoaringBitmapSerializer;

import static org.junit.Assert.*;

public class TestRoaringBitmapXorCardinalityAgg {

    @Test
    public void testRoaringBitmapOrCardinalityAggNormal() throws HiveException {
        RoaringBitmapXorCardinalityAgg rb_xor_cardinality_agg=new RoaringBitmapXorCardinalityAgg();

        //part1
        GenericUDAFEvaluator eval1=rb_xor_cardinality_agg.getEvaluator(
                new TypeInfo[]{TypeInfoFactory.binaryTypeInfo}
        );

        ObjectInspector arg1OI= PrimitiveObjectInspectorFactory.writableBinaryObjectInspector;
        ObjectInspector[] arguments1={arg1OI};
        GenericUDAFEvaluator.AggregationBuffer buffer1=eval1.getNewAggregationBuffer();
        eval1.init(GenericUDAFEvaluator.Mode.PARTIAL1,arguments1);
        // partial1
        // test iterate, terminatePartial
        Roaring64Bitmap bitmap_a1=new Roaring64Bitmap();
        Roaring64Bitmap bitmap_a2=new Roaring64Bitmap();
        bitmap_a1.addRange(1L,5L);
        bitmap_a2.addRange(2L,6L);

        BytesWritable input_a1= RoaringBitmapSerializer.serialize(bitmap_a1);
        BytesWritable input_a2= RoaringBitmapSerializer.serialize(bitmap_a2);

        eval1.iterate(buffer1,new Object[]{input_a1});
        eval1.iterate(buffer1,new Object[]{input_a2});

        Object partialResult1=eval1.terminatePartial(buffer1);
        assertNotNull(partialResult1);
        Roaring64Bitmap partialBitmap1= RoaringBitmapSerializer.deserialize((BytesWritable)partialResult1);
        Roaring64Bitmap expectedBitmap1=new Roaring64Bitmap();
        expectedBitmap1.addLong(1L);
        expectedBitmap1.addLong(5L);
        assertEquals(expectedBitmap1,partialBitmap1);

        // part2
        GenericUDAFEvaluator eval2=rb_xor_cardinality_agg.getEvaluator(
                new TypeInfo[]{TypeInfoFactory.binaryTypeInfo}
        );

        ObjectInspector arg2OI= PrimitiveObjectInspectorFactory.writableBinaryObjectInspector;
        ObjectInspector[] arguments2={arg2OI};
        GenericUDAFEvaluator.AggregationBuffer buffer2=eval2.getNewAggregationBuffer();
        eval2.init(GenericUDAFEvaluator.Mode.PARTIAL1,arguments2);

        // partial2
        // test iterate, terminatePartial
        Roaring64Bitmap bitmap_b1=new Roaring64Bitmap();
        Roaring64Bitmap bitmap_b2=new Roaring64Bitmap();
        bitmap_b1.addRange(10L,15L);
        bitmap_b2.addRange(15L,20L);

        BytesWritable input_b1= RoaringBitmapSerializer.serialize(bitmap_b1);
        BytesWritable input_b2= RoaringBitmapSerializer.serialize(bitmap_b2);
        eval2.iterate(buffer2,new Object[]{null});
        eval2.iterate(buffer2,new Object[]{input_b1});
        eval2.iterate(buffer2,new Object[]{input_b2});

        Object partialResult2=eval2.terminatePartial(buffer2);
        assertNotNull(partialResult2);
        Roaring64Bitmap partialBitmap2= RoaringBitmapSerializer.deserialize((BytesWritable)partialResult2);
        Roaring64Bitmap expectedBitmap2=new Roaring64Bitmap();
        expectedBitmap2.addRange(10L,20L);
        assertEquals(expectedBitmap2,partialBitmap2);

        // test merge, terminate
        GenericUDAFEvaluator eval3=rb_xor_cardinality_agg.getEvaluator(
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
        assertEquals(12L,output.get());

    }


    @Test
    public void testRoaringBitmapOrCardinalityAggEmpty() throws HiveException {
        RoaringBitmapXorCardinalityAgg rb_xor_cardinality_agg=new RoaringBitmapXorCardinalityAgg();

        //part1
        GenericUDAFEvaluator eval1=rb_xor_cardinality_agg.getEvaluator(
                new TypeInfo[]{TypeInfoFactory.binaryTypeInfo}
        );

        ObjectInspector arg1OI= PrimitiveObjectInspectorFactory.writableBinaryObjectInspector;
        ObjectInspector[] arguments1={arg1OI};
        GenericUDAFEvaluator.AggregationBuffer buffer1=eval1.getNewAggregationBuffer();
        eval1.init(GenericUDAFEvaluator.Mode.PARTIAL1,arguments1);
        // partial1
        // test iterate, terminatePartial
        Roaring64Bitmap bitmap_a1=new Roaring64Bitmap();
        Roaring64Bitmap bitmap_a2=new Roaring64Bitmap();

        BytesWritable input_a1= RoaringBitmapSerializer.serialize(bitmap_a1);
        BytesWritable input_a2= RoaringBitmapSerializer.serialize(bitmap_a2);

        eval1.iterate(buffer1,new Object[]{input_a1});
        eval1.iterate(buffer1,new Object[]{input_a2});

        Object partialResult1=eval1.terminatePartial(buffer1);
        assertNotNull(partialResult1);
        Roaring64Bitmap partialBitmap1= RoaringBitmapSerializer.deserialize((BytesWritable)partialResult1);
        assertTrue(partialBitmap1.isEmpty());

        // part2
        GenericUDAFEvaluator eval2=rb_xor_cardinality_agg.getEvaluator(
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
        Roaring64Bitmap partialBitmap2= RoaringBitmapSerializer.deserialize((BytesWritable)partialResult2);
        assertTrue(partialBitmap2.isEmpty());

        // test merge, terminate
        GenericUDAFEvaluator eval3=rb_xor_cardinality_agg.getEvaluator(
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
        RoaringBitmapXorCardinalityAgg rb_xor_cardinality_agg=new RoaringBitmapXorCardinalityAgg();

        //part1
        GenericUDAFEvaluator eval3=rb_xor_cardinality_agg.getEvaluator(
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
