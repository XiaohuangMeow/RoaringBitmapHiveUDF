package roaringbitmap.udf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public abstract class AbstractGenericUDFRoaringBitmapBase extends GenericUDF {

    private String functionName=null;
    private int minArgCount=-1;
    private int maxArgCount=-1;
    private ObjectInspector.Category outputCategory=null;
    private String outputTypeName=null;
    private final static List<String> primitiveTypeList=new ArrayList<>(Arrays.asList("bigint","boolean","binary"));
    protected transient ObjectInspector[] argumentsOIs;

    protected void initFunctionParameters(String functionName,int minArgCount,int maxArgCount,ObjectInspector.Category outputCategory,String outputTypeName){
        this.functionName=functionName;
        this.minArgCount=minArgCount;
        this.maxArgCount=maxArgCount;
        this.outputCategory=outputCategory;
        this.outputTypeName=outputTypeName;
    }


    @Override
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        checkArgsSize(objectInspectors,minArgCount,maxArgCount);
        checkOutputCategory();
        this.argumentsOIs=objectInspectors;

        if (outputCategory==ObjectInspector.Category.LIST) {
            return ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableLongObjectInspector);
        }
        // bigint, boolean, binary
        switch (this.outputTypeName) {
            case "bigint":
                return PrimitiveObjectInspectorFactory.writableLongObjectInspector;
            case "boolean":
                return PrimitiveObjectInspectorFactory.writableBooleanObjectInspector;
            case "binary":
                return PrimitiveObjectInspectorFactory.writableBinaryObjectInspector;
        }
        return null;
    }


    @Override
    public String getDisplayString(String[] strings) {
        return getStandardDisplayString(functionName.toLowerCase(),strings);
    }

    private void checkOutputCategory() throws UDFArgumentException {
        // output type is Array<Bigint>
        if (outputCategory==ObjectInspector.Category.LIST) {
            if (!outputTypeName.equals("Array<Bigint>")) {
                throw new UDFArgumentException("Internal Code Error, the outputTypeName must be consistent with outputTypeName(List)");
            }
        }
        // output type is Primitive(including bigint, binary, boolean, in primitiveTypeList)
        else if (outputCategory==ObjectInspector.Category.PRIMITIVE) {
            boolean isPrimitiveTypeMatch=false;
            for (String primitiveType:primitiveTypeList) {
                if (primitiveType.equals(outputTypeName)) {
                    isPrimitiveTypeMatch=true;
                    break;
                }
            }
            if (!isPrimitiveTypeMatch) {
                throw new UDFArgumentException("Internal Code Error, the outputTypeName must be consistent with outputTypeName(Primitive)");
            }
        }
        else {
            throw new UDFArgumentException("Internal Code Error");
        }
    }

    protected void checkArgCategory(
            int idx
            , ObjectInspector.Category expectedCategory
            , PrimitiveObjectInspector.PrimitiveCategory expectedPrimitiveCategory
    ) throws UDFArgumentTypeException {
        if (expectedCategory.equals(ObjectInspector.Category.LIST)) {
            checkArgListCategory(this.argumentsOIs[idx],this.functionName,idx);
        }
        else if (expectedCategory.equals(ObjectInspector.Category.PRIMITIVE)) {
            checkArgPrimitiveCategory(this.argumentsOIs[idx],this.functionName,idx,expectedPrimitiveCategory);
        }
        else {
            throw new UDFArgumentTypeException(idx, "Internal Code Error of checkArgCategory");
        }

    }

    private static void checkArgPrimitiveCategory(ObjectInspector objectInspector, String functionName, int idx, PrimitiveObjectInspector.PrimitiveCategory expectedCategory) throws UDFArgumentTypeException {
        if (!objectInspector.getCategory().equals(ObjectInspector.Category.PRIMITIVE)) {
            throw new UDFArgumentTypeException(idx,
                    "Argument " + idx + " of function " + functionName + " must be " +expectedCategory.name()
                        + ", but " +objectInspector.getTypeName() + " was found.");
        }
    }

    private static void checkArgListCategory(ObjectInspector objectInspector,String functionName,int idx) throws UDFArgumentTypeException {
        if (!objectInspector.getCategory().equals(ObjectInspector.Category.LIST)) {
            throw new UDFArgumentTypeException(idx,
                    "Argument " + idx + " of function " + functionName + " must be " + "an array"
                        + ", but " + objectInspector.getTypeName() + " was found.");
        }
        ObjectInspector elementOI=((ListObjectInspector)objectInspector).getListElementObjectInspector();
        if (!elementOI.getCategory().equals(ObjectInspector.Category.PRIMITIVE)) {
            throw new UDFArgumentTypeException(idx,
                    "Argument " + idx + " of function " + functionName + " must be " + "an array<bigint>"
                            + ", but " + objectInspector.getTypeName() + " was found.");
        }
        PrimitiveObjectInspector primitiveObjectInspector=(PrimitiveObjectInspector)elementOI;
        if (!primitiveObjectInspector.getPrimitiveCategory().equals(PrimitiveObjectInspector.PrimitiveCategory.LONG)) {
            throw new UDFArgumentTypeException(idx,
                    "Argument " + idx + " of function " + functionName + " must be " + "an array<bigint>"
                            + " ,but " + "Array<" + elementOI.getTypeName() + "> was found.");
        }


    }

}
