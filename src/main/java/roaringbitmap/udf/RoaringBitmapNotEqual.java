package roaringbitmap.udf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.roaringbitmap.longlong.Roaring64NavigableMap;
import roaringbitmap.utils.RoaringBitmapSerializer;

public class RoaringBitmapNotEqual extends AbstractGenericUDFRoaringBitmapBase{

    @Override
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        initFunctionParameters("rb_not_equal",2,2,ObjectInspector.Category.PRIMITIVE,"boolean");
        ObjectInspector res=super.initialize(objectInspectors);
        checkArgCategory(0,ObjectInspector.Category.PRIMITIVE, PrimitiveObjectInspector.PrimitiveCategory.BINARY);
        checkArgCategory(1,ObjectInspector.Category.PRIMITIVE, PrimitiveObjectInspector.PrimitiveCategory.BINARY);
        return res;
    }

    @Override
    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
        if (deferredObjects[0]==null||deferredObjects[1]==null) {
            return null;
        }
        if (deferredObjects[0].get()==null||deferredObjects[1].get()==null) {
            return null;
        }
        BytesWritable bytes1=((BinaryObjectInspector)this.argumentsOIs[0]).getPrimitiveWritableObject(deferredObjects[0].get());
        BytesWritable bytes2=((BinaryObjectInspector)this.argumentsOIs[0]).getPrimitiveWritableObject(deferredObjects[1].get());
        Roaring64NavigableMap bitmap1= RoaringBitmapSerializer.deserialize(bytes1);
        Roaring64NavigableMap bitmap2= RoaringBitmapSerializer.deserialize(bytes2);

        return new BooleanWritable(!bitmap1.equals(bitmap2));
    }
}
