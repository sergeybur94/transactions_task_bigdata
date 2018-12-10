package ru.beeline.anomalySearch;

//import org.apache.spark.api.java.JavaRDD;
//import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
//import org.apache.spark.sql.types.DataTypes;
//import org.apache.spark.sql.types.StructField;
//import org.apache.spark.sql.types.StructType;

//import java.util.Arrays;
//import java.util.List;
//import java.util.function.Function;

//import org.apache.spark.sql.functions;

public class SparkTest {
  public static void main(String[] args) {
    SparkSession spark = SparkSession
        .builder()
        .appName("DataFrame-FromRowsAndSchema")
        .master("local[4]")
        .getOrCreate();

//    List<Row> customerRows = Arrays.asList(
//        RowFactory.create(1, "Widget Co", 120000.00, 0.00, "AZ"),
//        RowFactory.create(2, "Acme Widgets", 410500.00, 500.00, "CA"),
//        RowFactory.create(3, "Widgetry", 410500.00, 200.00, "CA"),
//        RowFactory.create(4, "Widgets R Us", 410500.00, 0.0, "CA"),
//        RowFactory.create(5, "Ye Olde Widgete", 500.00, 0.0, "MA")
//    );
//
//    List<StructField> fields = Arrays.asList(
//        DataTypes.createStructField("id", DataTypes.IntegerType, true),
//        DataTypes.createStructField("name", DataTypes.StringType, true),
//        DataTypes.createStructField("sales", DataTypes.DoubleType, true),
//        DataTypes.createStructField("discount", DataTypes.DoubleType, true),
//        DataTypes.createStructField("state", DataTypes.StringType, true)
////        DataTypes.createStructField("ban_src", DataTypes.LongType, false),
////        DataTypes.createStructField("subscriber_no", DataTypes.StringType, false),
////        DataTypes.createStructField("soc", DataTypes.StringType, false),
////        DataTypes.createStructField("soc_seq_no", DataTypes.LongType, false),
////        DataTypes.createStructField("state", DataTypes.StringType, true)
//    );
//    StructType customerSchema = DataTypes.createStructType(fields);
//
//    Dataset<Row> customerDF =
//        spark.createDataFrame(customerRows, customerSchema);
//
//    System.out.println("*** the schema created");
//    customerDF.printSchema();
//
//    System.out.println("*** the data");
//    customerDF.show();
//
//    System.out.println("*** just the rows from CA");
//    customerDF.filter(functions.col("state").equalTo("CA")).show();

//    List<StructField> fields = Arrays.asList(
//        DataTypes.createStructField("ban_src", DataTypes.LongType, false),                    //1
//        DataTypes.createStructField("subscriber_no", DataTypes.StringType, false),            //2
//        DataTypes.createStructField("soc", DataTypes.StringType, false),                      //3
//        DataTypes.createStructField("soc_seq_no", DataTypes.LongType, false),                 //4
//        DataTypes.createStructField("service_ftr_seq_no", DataTypes.DoubleType, false),       //5
//        DataTypes.createStructField("sys_creation_date", DataTypes.StringType, false),
//        DataTypes.createStructField("sys_update_date", DataTypes.StringType, false),
//        DataTypes.createStructField("operator_id", DataTypes.DoubleType, false),
//        DataTypes.createStructField("application_id", DataTypes.StringType, false),
//        DataTypes.createStructField("dl_service_code", DataTypes.StringType, false),
//        DataTypes.createStructField("dl_update_stamp", DataTypes.DoubleType, false),
//        DataTypes.createStructField("soc_effective_date", DataTypes.StringType, false),
//        DataTypes.createStructField("customer_id", DataTypes.DoubleType, false),
//        DataTypes.createStructField("feature_code", DataTypes.StringType, false),
//        DataTypes.createStructField("service_type", DataTypes.StringType, false),
//        DataTypes.createStructField("soc_level_code", DataTypes.StringType, false),
//        DataTypes.createStructField("ftr_effective_date", DataTypes.StringType, false),
//        DataTypes.createStructField("ftr_eff_rsn_code", DataTypes.StringType, false),
//        DataTypes.createStructField("ftr_expiration_date", DataTypes.StringType, false),
//        DataTypes.createStructField("ftr_exp_rsn_code", DataTypes.StringType, false),
//        DataTypes.createStructField("rc_waiver_eff_date", DataTypes.StringType, false),
//        DataTypes.createStructField("rc_waiver_expr_date", DataTypes.StringType, false),
//        DataTypes.createStructField("rc_waiver_rsn", DataTypes.StringType, false),
//        DataTypes.createStructField("additional_info_amt", DataTypes.DoubleType, false),
//        DataTypes.createStructField("additional_info_type", DataTypes.StringType, false),
//        DataTypes.createStructField("additional_info", DataTypes.StringType, false),
//        DataTypes.createStructField("charge_level_code", DataTypes.StringType, false),
//        DataTypes.createStructField("ben", DataTypes.DoubleType, false),
//        DataTypes.createStructField("revenue_code", DataTypes.StringType, false),
//        DataTypes.createStructField("soc_related", DataTypes.StringType, false),
//        DataTypes.createStructField("bl_prom_sort_code", DataTypes.DoubleType, false),
//        DataTypes.createStructField("service_class", DataTypes.StringType, false),
//        DataTypes.createStructField("rate_code", DataTypes.StringType, false),
//        DataTypes.createStructField("rc_expiration_date", DataTypes.StringType, false),
//        DataTypes.createStructField("rc_advpym_waive_eff_dt", DataTypes.StringType, false),
//        DataTypes.createStructField("rc_advpym_waive_expr_dt", DataTypes.StringType, false),
//        DataTypes.createStructField("rc_advpym_waive_rsn", DataTypes.StringType, false),
//        DataTypes.createStructField("rc_advpym_bill_seq_no", DataTypes.DoubleType, false),
//        DataTypes.createStructField("renewal_rate_code", DataTypes.StringType, false),
//        DataTypes.createStructField("secondary_tn", DataTypes.StringType, false),
//        DataTypes.createStructField("conv_run_no", DataTypes.DoubleType, false),
//        DataTypes.createStructField("port_in_start_date", DataTypes.StringType, false),
//        DataTypes.createStructField("port_in_end_date", DataTypes.StringType, false),
//        DataTypes.createStructField("ban", DataTypes.StringType, false)
//
//        );
//
//    StructType schema = DataTypes.createStructType(fields);

    Dataset<Row> csv = spark.read().format("csv")
        .option("header","false")
        .option("inferSchema", "true")
        .option("delimiter", "\u0001")
        .load("example")///service_feature_2018_example_100k
        .toDF("ban_src", "subscriber_no", "soc", "soc_seq_no", "service_ftr_seq_no", "sys_creation_date",
            "sys_update_date", "operator_id", "application_id", "dl_service_code", "dl_update_stamp",
            "soc_effective_date", "customer_id", "feature_code", "service_type", "soc_level_code",
            "ftr_effective_date", "ftr_eff_rsn_code", "ftr_expiration_date", "ftr_exp_rsn_code",
            "rc_waiver_eff_date", "rc_waiver_expr_date", "rc_waiver_rsn", "additional_info_amt",
            "additional_info_type", "additional_info", "charge_level_code", "ben", "revenue_code",
            "soc_related", "bl_prom_sort_code", "service_class", "rate_code", "rc_expiration_date",
            "rc_advpym_waive_eff_dt", "rc_advpym_waive_expr_dt", "rc_advpym_waive_rsn", "rc_advpym_bill_seq_no",
            "renewal_rate_code", "secondary_tn", "conv_run_no", "port_in_start_date", "port_in_end_date",
            "ban");
    //csv.show(80);

    System.out.println(csv.count());
//    //A8.2
//    System.out.println("*/*/*/*/*/*/*/*/*/*/*/*/*/*/");
//    csv.filter(functions.col("service_class").equalTo("PP"))
//        .filter(functions.col("service_type").notEqual("P")).show();
//    //A8.1
//    csv.filter(functions.col("service_class").equalTo("SOC"))
//        .filter(functions.col("service_type").notEqual("O")).show();
//    //A8.3
//    csv.filter(functions.col("service_class").notEqual("SOC"))
//        .filter(functions.col("service_class").notEqual("PP")).show();
    //
//    csv.filter("soc == '77LEGO_1'")
//        .show();
//    //A7
//    csv.filter("ftr_expiration_date < ftr_effective_date")
//        .show(100);
//    //A6
//    csv.filter("ftr_effective_date < soc_effective_date")
//        .show(100);
//    //A5
//    csv.filter("ban_src != customer_id")
//        .show(100);
//    //A4.1
//    csv.filter(functions.col("operator_id").equalTo("\\N"))
//        .filter(functions.col("application_id").equalTo("\\N"))// AND application_id != '\\N'")
//        .show(100);
//    //A4.2
//    csv.filter(functions.col("operator_id").notEqual("\\N"))
//        .filter(functions.col("application_id").notEqual("\\N"))// AND application_id != '\\N'")
//        .show(100);
//    //A3
//    csv.filter("sys_creation_date < soc_effective_date")// AND application_id != '\\N'")
//        .show(100);
//    //A2
//    csv.filter("sys_update_date < sys_creation_date")// AND application_id != '\\N'")
//        .show(100);
        //A2
    csv.filter("ban_src == 688212691")// AND application_id != '\\N'")
        .show(100);

    //csv.orderBy(functions.asc("soc_seq_no")).show(10000);

    spark.stop();
  }
}
