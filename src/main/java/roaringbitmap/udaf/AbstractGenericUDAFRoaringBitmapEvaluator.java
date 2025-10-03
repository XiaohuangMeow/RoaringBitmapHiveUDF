package roaringbitmap.udaf;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.roaringbitmap.longlong.Roaring64NavigableMap;
import roaringbitmap.utils.RoaringBitmapSerializer;

public abstract class AbstractGenericUDAFRoaringBitmapEvaluator extends GenericUDAFEvaluator {

    protected transient PrimitiveObjectInspector inputOI;
    protected transient BinaryObjectInspector internalMergeOI;

    @Override
    public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
        super.init(m,parameters);
        if (m==Mode.PARTIAL1||m==Mode.COMPLETE) {
            // long->binary || long->long || binary->long
            inputOI=(PrimitiveObjectInspector) parameters[0];
        }
        else {
            internalMergeOI= (BinaryObjectInspector) parameters[0];
        }
        return null;
    }

    protected static class RoaringBitmapAggBuffer extends AbstractAggregationBuffer {
        Roaring64NavigableMap bitmap;
        boolean is_first_compute;
    }

    @Override
    public AggregationBuffer getNewAggregationBuffer() throws HiveException {
        RoaringBitmapAggBuffer aggBuffer= new RoaringBitmapAggBuffer();
        reset(aggBuffer);
        return aggBuffer;
    }

    @Override
    public void reset(AggregationBuffer aggregationBuffer) throws HiveException {
        RoaringBitmapAggBuffer myagg = (RoaringBitmapAggBuffer) aggregationBuffer;
        myagg.bitmap=new Roaring64NavigableMap();
        myagg.is_first_compute=true;
    }

    @Override
    public Object terminatePartial(AggregationBuffer aggregationBuffer) throws HiveException {
        RoaringBitmapAggBuffer myagg = (RoaringBitmapAggBuffer) aggregationBuffer;
        return RoaringBitmapSerializer.serialize(myagg.bitmap);

    }

}
