package in.chopl.hive.udaf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.Text;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

public class FirstOrderedByTest {
    FirstOrderedBy firstOrderedBy;
    GenericUDAFEvaluator evaluator;
    ObjectInspector[] inputOIs;
    ObjectInspector[] partialOI;
    FirstOrderedBy.GenericUDAFFirstOrderedByEvaluator.FirstAgg agg;

    String cmpColOrder1 = "ASC";
    String cmpColOrder2 = "DESC";
    Object[] param1 = {"foo", 1, new Text(cmpColOrder1), 1, new Text(cmpColOrder2)};
    Object[] param2 = {"foo", 2, new Text(cmpColOrder1), 2, new Text(cmpColOrder2)};
    Object[] param3 = {"foo", 0, new Text(cmpColOrder1), 1, new Text(cmpColOrder2)};
    Object[] param4 = {"foo", 0, new Text(cmpColOrder1), 4, new Text(cmpColOrder2)};
    String[] orders = {cmpColOrder1, cmpColOrder2};

    @Before
    public void setUp() throws Exception {
        firstOrderedBy = new FirstOrderedBy();

        String[] typeStrs = {"string", "int", "string", "int", "string"};
        TypeInfo[] types = makePrimitiveTypeInfoArray(typeStrs);
        evaluator = firstOrderedBy.getEvaluator(types);

        inputOIs = new ObjectInspector[5];
        inputOIs[0] =  PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(
                PrimitiveObjectInspector.PrimitiveCategory.STRING);
        inputOIs[1] =  PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(
                PrimitiveObjectInspector.PrimitiveCategory.INT);
        inputOIs[2] =  PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(
                PrimitiveObjectInspector.PrimitiveCategory.STRING, new Text(cmpColOrder1));
        inputOIs[3] =  PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(
                PrimitiveObjectInspector.PrimitiveCategory.INT);
        inputOIs[4] =  PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(
                PrimitiveObjectInspector.PrimitiveCategory.STRING, new Text(cmpColOrder2));

        List<String> fieldNames = new ArrayList<String>(5);
        fieldNames.add("val_col");
        fieldNames.add("cmp_col1");
        fieldNames.add("cmp_col_order1");
        fieldNames.add("cmp_col2");
        fieldNames.add("cmp_col_order2");

        partialOI = new ObjectInspector[1];
        partialOI[0] = ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, Arrays.asList(inputOIs));

        agg = (FirstOrderedBy.GenericUDAFFirstOrderedByEvaluator.FirstAgg) evaluator.getNewAggregationBuffer();
    }

    @After
    public void tearDown() throws Exception {

    }

    @Test(expected = UDFArgumentLengthException.class)
    public void testGetEvaluateorWithNoCmpCol() throws Exception {
        String[] typeStrs = {"string"};
        TypeInfo[] types = makePrimitiveTypeInfoArray(typeStrs);
        firstOrderedBy.getEvaluator(types);
    }

    @Test(expected = UDFArgumentLengthException.class)
    public void testGetEvaluateorWithNoCmpColOrder() throws Exception {
        String[] typeStrs = {"string", "int"};
        TypeInfo[] types = makePrimitiveTypeInfoArray(typeStrs);
        firstOrderedBy.getEvaluator(types);
    }

    @Test(expected = UDFArgumentTypeException.class)
    public void testGetEvaluateorWithInvalidOrderType() throws Exception {
        String[] typeStrs = {"string", "int", "int"};
        TypeInfo[] types = makePrimitiveTypeInfoArray(typeStrs);
        firstOrderedBy.getEvaluator(types);
    }

    @Test(expected = UDFArgumentTypeException.class)
    public void testGetEvaluateorWithMapAsCmpCol() throws Exception {
        TypeInfo[] types = new TypeInfo[3];
        types[0] = TypeInfoFactory.getPrimitiveTypeInfo("string");
        types[1] = TypeInfoFactory.getMapTypeInfo(
                        TypeInfoFactory.getPrimitiveTypeInfo("string"),
                        TypeInfoFactory.getPrimitiveTypeInfo("string"));
        types[2] = TypeInfoFactory.getPrimitiveTypeInfo("string");
        firstOrderedBy.getEvaluator(types);
    }

    @Test(expected = UDFArgumentTypeException.class)
    public void testInitWithNonConstantOrderString() throws Exception {
        inputOIs[2] =  PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(
                PrimitiveObjectInspector.PrimitiveCategory.STRING);

        evaluator.init(GenericUDAFEvaluator.Mode.PARTIAL1, inputOIs);
    }

    @Test(expected = UDFArgumentTypeException.class)
    public void testInitWithInvaliOrderString() throws Exception {
        inputOIs[2] =  PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(
                PrimitiveObjectInspector.PrimitiveCategory.STRING, new Text("foo"));

        evaluator.init(GenericUDAFEvaluator.Mode.PARTIAL1, inputOIs);
    }

    @Test
    public void testIterate() throws Exception {
        evaluator.init(GenericUDAFEvaluator.Mode.PARTIAL1, inputOIs);
        evaluator.reset(agg);


        evaluator.iterate(agg, param1);
        assertArrayEquals(agg.objects, param1);
        assertArrayEquals(agg.orders, orders);

        evaluator.iterate(agg, param2);
        assertArrayEquals(agg.objects, param1);

        evaluator.iterate(agg, param3);
        assertArrayEquals(agg.objects, param3);

        evaluator.iterate(agg, param4);
        assertArrayEquals(agg.objects, param4);
    }

    @Test
    public void testTerminatePartial() throws Exception {
        evaluator.init(GenericUDAFEvaluator.Mode.PARTIAL1, inputOIs);
        evaluator.reset(agg);

        evaluator.iterate(agg, param1);
        evaluator.iterate(agg, param2);
        evaluator.iterate(agg, param3);
        evaluator.iterate(agg, param4);

        Object partial = evaluator.terminatePartial(agg);

        assertTrue(partial instanceof Object[]);
        assertArrayEquals((Object[]) partial, param4);
    }

    @Test
    public void testMerge() throws Exception {
        evaluator.init(GenericUDAFEvaluator.Mode.PARTIAL1, inputOIs);
        evaluator.reset(agg);
        evaluator.iterate(agg, param1);
        evaluator.iterate(agg, param2);
        Object partial1 = evaluator.terminatePartial(agg);

        evaluator.init(GenericUDAFEvaluator.Mode.PARTIAL1, inputOIs);
        evaluator.reset(agg);
        evaluator.iterate(agg, param3);
        evaluator.iterate(agg, param4);
        Object partial2 = evaluator.terminatePartial(agg);

        evaluator.init(GenericUDAFEvaluator.Mode.PARTIAL2, partialOI);
        evaluator.reset(agg);

        evaluator.merge(agg, partial1);
        assertArrayEquals(agg.objects, param1);

        evaluator.merge(agg, partial2);
        assertArrayEquals(agg.objects, param4);
    }

    @Test
    public void testTerminate() throws Exception {
        evaluator.init(GenericUDAFEvaluator.Mode.COMPLETE, inputOIs);
        evaluator.reset(agg);

        evaluator.iterate(agg, param1);
        evaluator.iterate(agg, param2);
        evaluator.iterate(agg, param3);
        evaluator.iterate(agg, param4);
        Object term = evaluator.terminate(agg);

        assertTrue(term instanceof String);
        assertEquals(term, param4[0]);
    }

    private TypeInfo[] makePrimitiveTypeInfoArray(String[] typeStrs) {
        int len = typeStrs.length;

        TypeInfo[] types = new TypeInfo[len];

        for (int i = 0; i < len; i++) {
            types[i] = TypeInfoFactory.getPrimitiveTypeInfo(typeStrs[i]);
        }

        return types;
    }
}
