package roaringbitmap.udf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.roaringbitmap.longlong.Roaring64NavigableMap;
import roaringbitmap.utils.RoaringBitmapSerializer;

public class RoaringBitmapRangeCardinality extends AbstractGenericUDFRoaringBitmapBase{

    @Override
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        initFunctionParameters("rb_range_cardinality",3,3,ObjectInspector.Category.PRIMITIVE,"bigint");
        ObjectInspector res=super.initialize(objectInspectors);
        checkArgCategory(0,ObjectInspector.Category.PRIMITIVE, PrimitiveObjectInspector.PrimitiveCategory.BINARY);
        checkArgCategory(1,ObjectInspector.Category.PRIMITIVE, PrimitiveObjectInspector.PrimitiveCategory.LONG);
        checkArgCategory(2,ObjectInspector.Category.PRIMITIVE, PrimitiveObjectInspector.PrimitiveCategory.LONG);
        return res;
    }

    @Override
    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
        if (deferredObjects[0]==null||deferredObjects[1]==null||deferredObjects[2]==null) {
            return null;
        }
        if (deferredObjects[0].get()==null || deferredObjects[1].get()==null || deferredObjects[2].get()==null) {
            return null;
        }

        BytesWritable bytes=((BinaryObjectInspector)this.argumentsOIs[0]).getPrimitiveWritableObject(deferredObjects[0].get());
        Roaring64NavigableMap bitmap= RoaringBitmapSerializer.deserialize(bytes);
        LongObjectInspector longObjectInspector1=(LongObjectInspector)this.argumentsOIs[1];
        LongObjectInspector longObjectInspector2=(LongObjectInspector)this.argumentsOIs[2];

        long start = longObjectInspector1.get(deferredObjects[1].get());
        long end = longObjectInspector2.get(deferredObjects[2].get());
        if (start>=end) {
            throw new UDFArgumentException("Invalid range, start must be less than end.");
        }
        Roaring64NavigableMap range=new Roaring64NavigableMap();
        // start>=0
        // deduce end>0
        if (start>=0L) {
            range.addRange(start,end);
        }
        // start<0
        // end=0
        else if (end==0L){
            range.addRange(start,-1L);
            range.addLong(-1L);
        }
        // start<0
        // end<0
        else {
            range.addRange(start,end);
        }
        range.addRange(start,end);
        range.and(bitmap);
        return new LongWritable(range.getLongCardinality());
    }
}
