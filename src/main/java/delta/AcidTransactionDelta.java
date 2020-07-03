package delta;

import io.delta.tables.DeltaTable;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.delta.DeltaLog;
import org.apache.spark.sql.types.StructType;

/**
 * AcidTransactionWithDeltaLake is a class to illustrate how spark offer ACID Transaction with delta Lake.
 */
public class AcidTransactionDelta {


    private static final Logger LOGGER = Logger.getLogger(AcidTransactionDelta.class);
    private final static String SPARK_APPLICATION_NAME = "Spark with Delta Lake Example";
    private final static String SPARK_APPLICATION_RUNNING_MODE = "local";
    private final static String FILE_PATH = "sparkdata/deltalakedata";
    private final static String STORE_PATH = "lake";


    public static void main(String[] args) {
        // Turn off spark's default logger
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        // Create Spark Session
        SparkSession sparkSession = SparkSession.builder().appName(SPARK_APPLICATION_NAME)
                .master(SPARK_APPLICATION_RUNNING_MODE)
                .getOrCreate();


        StructType schema = new StructType()
                .add("id", "string")
                .add("ticker", "string")
                .add("email", "string")
                .add("created_at", "string")
                .add("updated_at", "string");

//                .add(new StructField("updated", DataTypes.DateType,true, Metadata.empty()));
        Dataset<Row> data = sparkSession.read().format("csv").option("delimiter", "|").schema(schema)
                .load("ticker.csv");
        data.show(100,false);
//        data.write().partitionBy("created_at").option("header", true).format("delta").save(STORE_PATH);

        DeltaTable deltaTable = DeltaTable.forPath(sparkSession, STORE_PATH);
        deltaTable.toDF().show();

        Dataset<Row> overWriteData = sparkSession.read().format("csv").option("delimiter", "|").schema(schema)
                .load("update.csv");


        deltaTable.as("ticker").merge(overWriteData.coalesce(1).as("new_ticks"),
                "ticker.id=new_ticks.id").whenMatched().updateAll().whenNotMatched().insertAll().execute();
        deltaTable.toDF().show(30, false);

        deltaTable.generate("symlink_format_manifest");

        DeltaLog deltaLog = DeltaLog.forTable(sparkSession,STORE_PATH);
        sparkSession.sessionState().conf().setConfString("spark.databricks.delta.retentionDurationCheck.enabled","false");
//        deltaTable.executeVacuum(deltaLog, new Some(1));
//        data.show();
//
//        Dataset<Row> overwriteData =  sparkSession.read().format("csv").option("delimiter", "|").schema(schema)
//                .load("update.csv");
//
//
////        df = sparkSession.read.format("csv").option("header", "true").load("csvfile.csv")
//        //Job-1
////        data.write().format("delta").save(FILE_PATH);
////        LOGGER.info("records created after Job-1 " + sparkSession.read().format("delta").load(FILE_PATH).count());
////        data.show();
//
//        //-Job-2
//
//        try {
////            Dataset<Long> overwriteData = sparkSession.range(100);
//            overwriteData.map((MapFunction<Long, Integer>)
//                    delta.AcidTransactionDelta::getInteger, Encoders.INT())
//                    .write().format("delta").mode("overwrite").option("overwriteSchema", "true").save(FILE_PATH);
//            overwriteData.show();
//        } catch (Exception e) {
//            if (e.getCause() instanceof SparkException) {
//                LOGGER.warn("failed while OverWriteData into data source", e.getCause());
//                LOGGER.info("records created after Job-2 " + sparkSession.read().format("delta").load(FILE_PATH).count());
//                sparkSession.read().format("delta").load(FILE_PATH).show();
//            }
//
//            throw new RuntimeException("Runtime exception!");
//        }

        //close Spark Session
//        sparkSession.close();
    }

    /**
     * Failed job in the middle.
     *
     * @param num number from the record.
     * @return Integer to be write in data Lake.
     */
    private static Integer getInteger(Long num) {
        if (num > 100) {
            throw new RuntimeException("Oops! Atomicity failed");
        }
        return Math.toIntExact(num);
    }

}
