package roaringbitmap.udaf;


import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.BytesWritable;
import org.roaringbitmap.longlong.Roaring64Bitmap;
import roaringbitmap.utils.RoaringBitmapSerializer;

public class RoaringBitmapAndAgg extends AbstractGenericUDAFRoaringBitmapResolver {


    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters) throws UDFArgumentTypeException {
        checkArgSize(1,parameters);
        checkArgType("rb_and_agg",0,parameters, PrimitiveObjectInspector.PrimitiveCategory.BINARY);
        return new RoaringBitmapAndAggEvaluator();
    }

    public static class RoaringBitmapAndAggEvaluator extends AbstractGenericUDAFRoaringBitmapEvaluator {

        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
            super.init(m,parameters);
            return PrimitiveObjectInspectorFactory.writableBinaryObjectInspector;
        }

        @Override
        public void iterate(AggregationBuffer aggregationBuffer, Object[] parameters) throws HiveException {
            Object p = parameters[0];
            if (p==null) {
                return;
            }
            RoaringBitmapAggBuffer myagg = (RoaringBitmapAggBuffer) aggregationBuffer;
            Roaring64Bitmap inputBitmap= RoaringBitmapSerializer.deserialize(PrimitiveObjectInspectorUtils.getBinary(p, this.inputOI));
            if (myagg.is_first_compute) {
                myagg.bitmap=inputBitmap;
                myagg.is_first_compute=false;
            }
            else {
                myagg.bitmap.and(inputBitmap);
            }
        }

        @Override
        public void merge(AggregationBuffer aggregationBuffer, Object partial) throws HiveException {
            if (partial == null) {
                return;
            }
            RoaringBitmapAggBuffer myagg = (RoaringBitmapAggBuffer) aggregationBuffer;
            BytesWritable bytes=PrimitiveObjectInspectorUtils.getBinary(partial,this.internalMergeOI);
            Roaring64Bitmap partialBitmap= RoaringBitmapSerializer.deserialize(bytes);
            if (myagg.is_first_compute) {
                myagg.bitmap=partialBitmap;
                myagg.is_first_compute=false;
            }
            else {
                myagg.bitmap.and(partialBitmap);
            }

        }

        @Override
        public Object terminate(AggregationBuffer aggregationBuffer) throws HiveException {
            RoaringBitmapAggBuffer myagg=(RoaringBitmapAggBuffer) aggregationBuffer;
            if (myagg.is_first_compute) {
                return null;
            }
            return RoaringBitmapSerializer.serialize(myagg.bitmap);
        }
    }

}
