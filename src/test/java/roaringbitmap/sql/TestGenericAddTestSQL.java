package roaringbitmap.sql;

import org.apache.spark.sql.SparkSession;
import org.junit.Before;
import org.junit.Test;

public class TestGenericAddTestSQL {
    private SparkSession spark= SparkSession.builder()
            .enableHiveSupport()
            .master("local")
            .getOrCreate();

    @Before
    public void initRegisterFunction() {
        spark.sql("CREATE TEMPORARY FUNCTION my_add as 'roaringbitmap.test.GenericAddTest'");
    }

    @Test
    public void testGenericAddTest() {
        spark.sql("select my_add(1L,2L)").show();
    }

}
