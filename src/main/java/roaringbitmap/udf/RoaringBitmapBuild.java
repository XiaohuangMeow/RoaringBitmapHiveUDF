package roaringbitmap.udf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.roaringbitmap.longlong.Roaring64Bitmap;
import org.roaringbitmap.longlong.Roaring64NavigableMap;
import roaringbitmap.utils.RoaringBitmapSerializer;

import java.util.List;

public class RoaringBitmapBuild extends AbstractGenericUDFRoaringBitmapBase{

    @Override
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        initFunctionParameters("rb_build",1,1,ObjectInspector.Category.PRIMITIVE,"binary");
        ObjectInspector res=super.initialize(objectInspectors);
        checkArgCategory(0, ObjectInspector.Category.LIST, PrimitiveObjectInspector.PrimitiveCategory.LONG);
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
        ListObjectInspector listOI=(ListObjectInspector) this.argumentsOIs[0];
        List<Long> arg1=(List<Long>) listOI.getList(deferredObjects[0].get());
        Roaring64Bitmap bitmap=new Roaring64Bitmap();
        for (Long element:arg1) {
            if (element==null) continue;
            bitmap.add(element);
        }
        return RoaringBitmapSerializer.serialize(bitmap);
    }
}
