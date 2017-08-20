package uy.kohesive.elasticsearch.dataimport.udf;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.types.DataTypes;

public class Udfs {

    public static <T extends String, RT extends String> void registerStringToStringUdf(final SparkSession spark, final String name, final UDF1<T, RT> f) {
        spark.udf().register(name, f, DataTypes.StringType);
    }

    public static <T1 extends Object, T2 extends Object, RT extends Object> void registerAnyAnyToTimestampUdf(final SparkSession spark, final String name, final UDF2<T1, T2, RT> f) {
        spark.udf().register(name, f, DataTypes.TimestampType);
    }

    public static <T1 extends String, T2 extends String, RT extends String> void registerStringStringToStringUdf(final SparkSession spark, final String name, final UDF2<T1, T2, RT> f) {
        spark.udf().register(name, f, DataTypes.StringType);
    }


}
