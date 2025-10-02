package roaringbitmap.udf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.io.BytesWritable;
import org.roaringbitmap.longlong.Roaring64Bitmap;
import org.roaringbitmap.longlong.Roaring64NavigableMap;
import roaringbitmap.utils.RoaringBitmapSerializer;

public class RoaringBitmapRange extends AbstractGenericUDFRoaringBitmapBase{

    @Override
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        initFunctionParameters("rb_range",3,3,ObjectInspector.Category.PRIMITIVE,"binary");
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
        Roaring64Bitmap bitmap= RoaringBitmapSerializer.deserialize(bytes);
        LongObjectInspector longObjectInspector1=(LongObjectInspector)this.argumentsOIs[1];
        LongObjectInspector longObjectInspector2=(LongObjectInspector)this.argumentsOIs[2];

        long start = longObjectInspector1.get(deferredObjects[1].get());
        long end = longObjectInspector2.get(deferredObjects[2].get());
        Roaring64Bitmap range=new Roaring64Bitmap();
        range.addRange(start,end);
        range.and(bitmap);
        return RoaringBitmapSerializer.serialize(range);
    }
}
