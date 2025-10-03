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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class TestRoaringBitmapCardinalityAgg {

    @Test
    public void testRoaringBitmapCardinalityAggNormal() throws HiveException {
        RoaringBitmapCardinalityAgg rb_cardinality_agg=new RoaringBitmapCardinalityAgg();

        //part1
        GenericUDAFEvaluator eval1=rb_cardinality_agg.getEvaluator(
                new TypeInfo[]{TypeInfoFactory.longTypeInfo}
        );

        ObjectInspector arg1OI= PrimitiveObjectInspectorFactory.writableLongObjectInspector;
        ObjectInspector[] arguments1={arg1OI};
        GenericUDAFEvaluator.AggregationBuffer buffer1=eval1.getNewAggregationBuffer();
        eval1.init(GenericUDAFEvaluator.Mode.PARTIAL1,arguments1);
        // partial1
        // test iterate, terminatePartial

        eval1.iterate(buffer1,new Object[]{new LongWritable(1L)});
        eval1.iterate(buffer1,new Object[]{new LongWritable(2L)});
        eval1.iterate(buffer1,new Object[]{new LongWritable(2L)});
        eval1.iterate(buffer1,new Object[]{new LongWritable(3L)});

        Object partialResult1=eval1.terminatePartial(buffer1);
        assertNotNull(partialResult1);
        Roaring64NavigableMap partialBitmap1= RoaringBitmapSerializer.deserialize((BytesWritable)partialResult1);
        Roaring64NavigableMap expectedBitmap1=new Roaring64NavigableMap();
        expectedBitmap1.addRange(1L,4L);
        assertEquals(expectedBitmap1,partialBitmap1);

        // part2
        GenericUDAFEvaluator eval2=rb_cardinality_agg.getEvaluator(
                new TypeInfo[]{TypeInfoFactory.longTypeInfo}
        );

        ObjectInspector arg2OI= PrimitiveObjectInspectorFactory.writableLongObjectInspector;
        ObjectInspector[] arguments2={arg2OI};
        GenericUDAFEvaluator.AggregationBuffer buffer2=eval2.getNewAggregationBuffer();
        eval2.init(GenericUDAFEvaluator.Mode.PARTIAL1,arguments2);

        // partial2
        // test iterate, terminatePartial
        eval2.iterate(buffer2,new Object[]{null});
        eval2.iterate(buffer2,new Object[]{new LongWritable(4L)});
        eval2.iterate(buffer2,new Object[]{new LongWritable(5L)});

        Object partialResult2=eval2.terminatePartial(buffer2);
        assertNotNull(partialResult2);
        Roaring64NavigableMap partialBitmap2= RoaringBitmapSerializer.deserialize((BytesWritable)partialResult2);
        Roaring64NavigableMap expectedBitmap2=new Roaring64NavigableMap();
        expectedBitmap2.addRange(4L,6L);
        assertEquals(expectedBitmap2,partialBitmap2);

        // test merge, terminate
        GenericUDAFEvaluator eval3=rb_cardinality_agg.getEvaluator(
                new TypeInfo[]{TypeInfoFactory.longTypeInfo}
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
        assertEquals(5L,output.get());
    }


    @Test
    public void testRoaringBitmapCardinalityAggNull() throws HiveException {
        RoaringBitmapCardinalityAgg rb_cardinality_agg=new RoaringBitmapCardinalityAgg();

        //part1
        GenericUDAFEvaluator eval3=rb_cardinality_agg.getEvaluator(
                new TypeInfo[]{TypeInfoFactory.longTypeInfo}
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
