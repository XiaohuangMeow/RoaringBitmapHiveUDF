package roaringbitmap.udaf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

public abstract class AbstractGenericUDAFRoaringBitmapResolver extends AbstractGenericUDAFResolver {

    protected static void checkArgSize(int argCount, TypeInfo[] parameters) throws UDFArgumentTypeException {
        if (argCount != parameters.length) {
            throw new UDFArgumentTypeException(parameters.length - 1
                    , "Exactly" + argCount + "argument is expected.");
        }
    }

    protected static void checkArgType(String functionName, int idx, TypeInfo[] parameters, PrimitiveObjectInspector.PrimitiveCategory expectedCategory) throws UDFArgumentTypeException {
        if (parameters[idx].getCategory()!= ObjectInspector.Category.PRIMITIVE) {
            throw new UDFArgumentTypeException(0,
                    "Only primitive type arguments are accepted but "
                        + parameters[idx].getTypeName() + " is passed.");
        }
        PrimitiveTypeInfo primitiveTypeInfo = (PrimitiveTypeInfo) parameters[idx];
        if (!primitiveTypeInfo.getPrimitiveCategory().equals(expectedCategory)) {
            throw new UDFArgumentTypeException(0,
                    "Only " + expectedCategory.name() + " type arguments are accepted but "
                            + parameters[idx].getTypeName() + " is passed.");
        }

    }
}
