package roaringbitmap.udf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.roaringbitmap.longlong.Roaring64NavigableMap;
import roaringbitmap.utils.RoaringBitmapSerializer;

public class RoaringBitmapCardinality extends AbstractGenericUDFRoaringBitmapBase{

    @Override
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        initFunctionParameters("rb_cardinality",1,1,ObjectInspector.Category.PRIMITIVE,"bigint");
        ObjectInspector res=super.initialize(objectInspectors);
        checkArgCategory(0,ObjectInspector.Category.PRIMITIVE, PrimitiveObjectInspector.PrimitiveCategory.BINARY);
        return res;
    }

    @Override
    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
        if (deferredObjects[0]==null) {
            return null;
        }
        if (deferredObjects[0].get()==null) {
            return null;
        }
        BytesWritable bytes1=((BinaryObjectInspector)this.argumentsOIs[0]).getPrimitiveWritableObject(deferredObjects[0].get());
        Roaring64NavigableMap bitmap= RoaringBitmapSerializer.deserialize(bytes1);
        return new LongWritable(bitmap.getLongCardinality());
    }
}
