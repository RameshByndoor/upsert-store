import io.delta.tables.DeltaTable;
import org.apache.hudi.DataSourceReadOptions;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class TimeTravel {
    private final static String STORE_PATH = "lake";

    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder().appName("Parquet Reader").master("local").getOrCreate();

        String tablePath = "E:\\dev\\Projects\\delta-lake\\hudi_ticker";
//
//        DeltaTable deltaTable = DeltaTable.forPath(sparkSession,STORE_PATH);
//        deltaTable.toDF().show(100,false);
        sparkSession.sparkContext().setLogLevel("error");

        Dataset<Row> hudiIncQueryDF = sparkSession
                .read()
                .format("org.apache.hudi")
                .option(DataSourceReadOptions.QUERY_TYPE_OPT_KEY(), DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL())
//                .option(DataSourceReadOptions.BEGIN_INSTANTTIME_OPT_KEY(),"20200703142100")
//                .option(DataSourceReadOptions.END_INSTANTTIME_OPT_KEY(),  "20200703142200")

                .load(tablePath+"/*/*");
        hudiIncQueryDF.show(100, false);

        hudiIncQueryDF = sparkSession
                .read()
                .format("org.apache.hudi")
                .option(DataSourceReadOptions.QUERY_TYPE_OPT_KEY(), DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL())
                .option(DataSourceReadOptions.BEGIN_INSTANTTIME_OPT_KEY(),"20200703142100")
                .option(DataSourceReadOptions.END_INSTANTTIME_OPT_KEY(),  "20200703152700")

                .load(tablePath);
        hudiIncQueryDF.show(100, false);

        hudiIncQueryDF = sparkSession
                .read()
                .format("org.apache.hudi")
                .option(DataSourceReadOptions.QUERY_TYPE_OPT_KEY(), DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL())
                .option(DataSourceReadOptions.BEGIN_INSTANTTIME_OPT_KEY(),"20200703142500")
                .option(DataSourceReadOptions.END_INSTANTTIME_OPT_KEY(),  "20200703143000")
                .load(tablePath);

        hudiIncQueryDF.show(100, false);

        hudiIncQueryDF = sparkSession
                .read()
                .format("org.apache.hudi")
                .option(DataSourceReadOptions.QUERY_TYPE_OPT_KEY(), DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL())
                .option(DataSourceReadOptions.BEGIN_INSTANTTIME_OPT_KEY(),"20200703143000")
                .option(DataSourceReadOptions.END_INSTANTTIME_OPT_KEY(),  "20200703153000")
                .load(tablePath);

        hudiIncQueryDF.show(100, false);


        System.exit(1);
        hudiIncQueryDF.createOrReplaceTempView("hudi_trips_incremental");
        sparkSession.sql("select * from  hudi_trips_incremental ").show();

        Dataset<Row> df2 = sparkSession
                .read()
                .format("org.apache.hudi")
                .option(DataSourceReadOptions.QUERY_TYPE_OPT_KEY(), DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL())
                .option(DataSourceReadOptions.END_INSTANTTIME_OPT_KEY(), "20200711042400")
                .option(DataSourceReadOptions.BEGIN_INSTANTTIME_OPT_KEY(),"20200611042400")
                .load(tablePath + "/*/*");
        df2.show(100, false);
//        Dataset<Row> df = sparkSession.read().format("parquet").option("header", true).load("lake/*/*.parquet");
//
////        df.show(55,false);
////        System.out.println(df.count());
//
        sparkSession.read().format("delta").option("timestampAsOf", "2020-07-03 03:10:23").load("lake").show(100, false);
        sparkSession.read().format("delta").option("timestampAsOf", "2020-07-03 03:15:23").load("lake").show(100, false);
        sparkSession.read().format("delta").option("timestampAsOf", "2020-07-03 03:25:23").load("lake").show(100, false);

        sparkSession.read().format("delta").load("lake").show(100, false);

        Dataset<Row> pDF = sparkSession.read().format("parquet").option("header", true).load("lake/*/*.parquet");

        pDF.show(200, false);

    }
}
