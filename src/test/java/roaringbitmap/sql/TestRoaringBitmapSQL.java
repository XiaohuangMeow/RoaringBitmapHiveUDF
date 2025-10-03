package roaringbitmap.sql;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class TestRoaringBitmapSQL {

    private static SparkSession spark;
    private static Dataset<Row> raw_df;
    private static Dataset<Row> df;

    @BeforeClass
    public static void initSpark() {
        spark=SparkSession.builder()
                .enableHiveSupport()
                .master("local")
                .getOrCreate();

        spark.sql("CREATE TEMPORARY FUNCTION rb_and as 'roaringbitmap.udf.RoaringBitmapAnd'");
        spark.sql("CREATE TEMPORARY FUNCTION rb_and_cardinality as 'roaringbitmap.udf.RoaringBitmapAndCardinality'");
        spark.sql("CREATE TEMPORARY FUNCTION rb_andnot as 'roaringbitmap.udf.RoaringBitmapAndNot'");
        spark.sql("CREATE TEMPORARY FUNCTION rb_andnot_cardinality as 'roaringbitmap.udf.RoaringBitmapAndNotCardinality'");
        spark.sql("CREATE TEMPORARY FUNCTION rb_build as 'roaringbitmap.udf.RoaringBitmapBuild'");
        spark.sql("CREATE TEMPORARY FUNCTION rb_cardinality as 'roaringbitmap.udf.RoaringBitmapCardinality'");
        spark.sql("CREATE TEMPORARY FUNCTION rb_clear as 'roaringbitmap.udf.RoaringBitmapClear'");
        spark.sql("CREATE TEMPORARY FUNCTION rb_contains as 'roaringbitmap.udf.RoaringBitmapContains'");
        spark.sql("CREATE TEMPORARY FUNCTION rb_equal as 'roaringbitmap.udf.RoaringBitmapEqual'");
        spark.sql("CREATE TEMPORARY FUNCTION rb_fill as 'roaringbitmap.udf.RoaringBitmapFill'");
        spark.sql("CREATE TEMPORARY FUNCTION rb_intersect as 'roaringbitmap.udf.RoaringBitmapIntersect'");
        spark.sql("CREATE TEMPORARY FUNCTION rb_is_empty as 'roaringbitmap.udf.RoaringBitmapIsEmpty'");
        spark.sql("CREATE TEMPORARY FUNCTION rb_maximum as 'roaringbitmap.udf.RoaringBitmapMaximum'");
        spark.sql("CREATE TEMPORARY FUNCTION rb_minimum as 'roaringbitmap.udf.RoaringBitmapMinimum'");
        spark.sql("CREATE TEMPORARY FUNCTION rb_not_equal as 'roaringbitmap.udf.RoaringBitmapNotEqual'");
        spark.sql("CREATE TEMPORARY FUNCTION rb_or as 'roaringbitmap.udf.RoaringBitmapOr'");
        spark.sql("CREATE TEMPORARY FUNCTION rb_or_cardinality as 'roaringbitmap.udf.RoaringBitmapOrCardinality'");
        spark.sql("CREATE TEMPORARY FUNCTION rb_range as 'roaringbitmap.udf.RoaringBitmapRange'");
        spark.sql("CREATE TEMPORARY FUNCTION rb_range_cardinality as 'roaringbitmap.udf.RoaringBitmapRangeCardinality'");
        spark.sql("CREATE TEMPORARY FUNCTION rb_to_array as 'roaringbitmap.udf.RoaringBitmapToArray'");
        spark.sql("CREATE TEMPORARY FUNCTION rb_xor as 'roaringbitmap.udf.RoaringBitmapXor'");
        spark.sql("CREATE TEMPORARY FUNCTION rb_xor_cardinality as 'roaringbitmap.udf.RoaringBitmapXorCardinality'");
        // udaf
        spark.sql("CREATE TEMPORARY FUNCTION rb_and_agg as 'roaringbitmap.udaf.RoaringBitmapAndAgg'");
        spark.sql("CREATE TEMPORARY FUNCTION rb_and_cardinality_agg as 'roaringbitmap.udaf.RoaringBitmapAndCardinalityAgg'");
        spark.sql("CREATE TEMPORARY FUNCTION rb_build_agg as 'roaringbitmap.udaf.RoaringBitmapBuildAgg'");
        spark.sql("CREATE TEMPORARY FUNCTION rb_cardinality_agg as 'roaringbitmap.udaf.RoaringBitmapCardinalityAgg'");
        spark.sql("CREATE TEMPORARY FUNCTION rb_or_agg as 'roaringbitmap.udaf.RoaringBitmapOrAgg'");
        spark.sql("CREATE TEMPORARY FUNCTION rb_or_cardinality_agg as 'roaringbitmap.udaf.RoaringBitmapOrCardinalityAgg'");
        spark.sql("CREATE TEMPORARY FUNCTION rb_xor_agg as 'roaringbitmap.udaf.RoaringBitmapXorAgg'");
        spark.sql("CREATE TEMPORARY FUNCTION rb_xor_cardinality_agg as 'roaringbitmap.udaf.RoaringBitmapXorCardinalityAgg'");

        // data
        List<Row> rowList= new ArrayList<>(Arrays.asList(
                RowFactory.create(1L,new Long[]{1L,2L,3L},new Long[]{3L,4L,5L}),
                RowFactory.create(2L,new Long[]{1L,2L,3L},new Long[]{2L,3L,4L,5L}),
                RowFactory.create(3L,new Long[]{1L,2L,null},new Long[]{3L,4L,5L}),
                RowFactory.create(4L,new Long[]{null},new Long[]{null,null}),
                RowFactory.create(5L,null,null)
        ));
//        for (long i=0;i<3000000;i++) {
//            if (i%1000000L==0) System.out.println(i);
//            rowList.add(
//                    RowFactory.create(i*5000000000L,new Long[]{1L,2L,3L},new Long[]{3L,4L,5L})
//                    );
//        };

        StructType schema = new StructType(new StructField[]{
                new StructField("id", DataTypes.LongType,false, Metadata.empty()),
                new StructField("array1", DataTypes.createArrayType(DataTypes.LongType),true, Metadata.empty()),
                new StructField("array2", DataTypes.createArrayType(DataTypes.LongType),true, Metadata.empty()),
        });
        // raw_table
        raw_df=spark.createDataFrame(rowList,schema);
        raw_df.createOrReplaceTempView("raw_table");
        // roaringbitmap table
        df=spark.sql( "select id,array1,array2,rb_build(array1) as roaringbitmap1,rb_build(array2) as roaringbitmap2 from raw_table");
        df.show();
        df.createOrReplaceTempView("table");
    }

    @Test
    public void testRoardsadasdsaingBitmapCardinalityAgg() {
        spark.sql("select rb_cardinality_agg(cast(id as bigint)) from table").show();
    }

    @AfterClass
    public static void tearDownAfterClass() {
        if (spark != null) {
            spark.close();
        }
    }

    @Test
    public void testRoaringBitmapAnd() {
        spark.sql("select id,array1,array2,rb_to_array(rb_and(roaringbitmap1,roaringbitmap2)) from table").show();
    }

    @Test
    public void testRoaringBitmapAndCardinality() {
        spark.sql("select id,array1,array2,rb_and_cardinality(roaringbitmap1,roaringbitmap2) from table").show();
    }

    @Test
    public void testRoaringBitmapAndNot() {
        spark.sql("select id,array1,array2,rb_to_array(rb_andnot(roaringbitmap1,roaringbitmap2)) from table").show();
    }

    @Test
    public void testRoaringBitmapAndNotCardinality() {
        spark.sql("select id,array1,array2,rb_andnot_cardinality(roaringbitmap1,roaringbitmap2) from table").show();
    }

    @Test
    public void testRoaringBitmapCardinality() {
        spark.sql("select id,array1,rb_cardinality(roaringbitmap1) from table").show();
    }

    @Test
    public void testRoaringBitmapClear() {
        spark.sql("select id,array1,rb_to_array(rb_clear(roaringbitmap1,1L,2L)) from table").show();
    }

    @Test
    public void testRoaringBitmapContains() {
        spark.sql("select id,array1,array2,rb_contains(roaringbitmap1,roaringbitmap2) from table").show();
    }

    @Test
    public void testRoaringBitmapEqual() {
        spark.sql("select id,array1,array2,rb_equal(roaringbitmap1,roaringbitmap2) from table").show();
    }

    @Test
    public void testRoaringBitmapFill() {
        spark.sql("select id,array1,rb_to_array(rb_fill(roaringbitmap1,3L,6L)) from table").show();
    }

    @Test
    public void testRoaringBitmapIntersect() {
        spark.sql("select id,array1,array2,rb_intersect(roaringbitmap1,roaringbitmap2) from table").show();
    }

    @Test
    public void testRoaringBitmapIsEmpty() {
        spark.sql("select id,array1,rb_is_empty(roaringbitmap1) from table").show();
    }

    @Test
    public void testRoaringBitmapMaximum() {
        spark.sql("select id,array1,rb_maximum(roaringbitmap1) from table").show();
    }

    @Test
    public void testRoaringBitmapMinimum() {
        spark.sql("select id,array1,rb_minimum(roaringbitmap1) from table").show();
    }

    @Test
    public void testRoaringBitmapNotEqual() {
        spark.sql("select id,array1,array2,rb_not_equal(roaringbitmap1,roaringbitmap2) from table").show();
    }

    @Test
    public void testRoaringBitmapOr() {
        spark.sql("select id,array1,array2,rb_to_array(rb_or(roaringbitmap1,roaringbitmap2)) from table").show();
    }

    @Test
    public void testRoaringBitmapOrCardinality() {
        spark.sql("select id,array1,array2,rb_or_cardinality(roaringbitmap1,roaringbitmap2) from table").show();
    }

    @Test
    public void testRoaringBitmapRange() {
        spark.sql("select id,array1,rb_to_array(rb_range(roaringbitmap1,2L,4L)) from table").show();
    }

    @Test
    public void testRoaringBitmapRangeCardinality() {
        spark.sql("select id,array1,rb_range_cardinality(roaringbitmap1,2L,4L) from table").show();
    }


    @Test
    public void testRoaringBitmapXor() {
        spark.sql("select id,array1,array2,rb_to_array(rb_xor(roaringbitmap1,roaringbitmap2)) from table").show();
    }

    @Test
    public void testRoaringBitmapXorCardinality() {
        spark.sql("select id,array1,array2,rb_xor_cardinality(roaringbitmap1,roaringbitmap2) from table").show();
    }

    @Test
    public void testRoaringBitmapAndAgg() {
        spark.sql("select rb_to_array(rb_and_agg(roaringbitmap1)),rb_to_array(rb_and_agg(roaringbitmap2)) from table where id in (1,2,3,5)").show();
    }

    @Test
    public void testRoaringBitmapAndCardinalityAgg() {
        spark.sql("select rb_and_cardinality_agg(roaringbitmap1),rb_and_cardinality_agg(roaringbitmap2) from table where id in (1,2,3,5)").show();
    }

    @Test
    public void testRoaringBitmapBuildAgg() {
        spark.sql("select rb_to_array(rb_build_agg(cast(id as bigint))) from table").show();
    }

    @Test
    public void testRoaringBitmapCardinalityAgg() {
        spark.sql("select rb_cardinality_agg(cast(id as bigint)) from table").show();
    }

    @Test
    public void testRoaringBitmapOrAgg() {
        spark.sql("select rb_to_array(rb_or_agg(roaringbitmap1)),rb_to_array(rb_or_agg(roaringbitmap2)) from table").show();
    }

    @Test
    public void testRoaringBitmapOrCardinalityAgg() {
        spark.sql("select rb_or_cardinality_agg(roaringbitmap1),rb_or_cardinality_agg(roaringbitmap2) from table").show();
    }

    @Test
    public void testRoaringBitmapXorAgg() {
        spark.sql("select rb_to_array(rb_xor_agg(roaringbitmap1)),rb_to_array(rb_xor_agg(roaringbitmap2)) from table").show();
    }

    @Test
    public void testRoaringBitmapXorCardinalityAgg() {
        spark.sql("select rb_xor_cardinality_agg(roaringbitmap1),rb_xor_cardinality_agg(roaringbitmap2) from table").show();
    }




}
