package hudi;

import org.apache.hudi.DataSourceWriteOptions;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.StructType;

public class AcidWithHudiTicker {

    private static String tableType = HoodieTableType.COPY_ON_WRITE.name();
    private static String tableName = "hoodie_rt";

    private static String tablePath = "E:\\dev\\Projects\\delta-lake\\hudi_ticker";
    private static String wareHouse = "E:\\dev\\Projects\\delta-lake\\spark_warehouse";

    public static void main(String[] args) {


        SparkSession spark = SparkSession.builder().appName(AcidWithHudiTicker.class.getSimpleName())
                .config("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
                .config("spark.eventLog.enabled", true)
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .config("spark.sql.parquet.binaryAsString", true)
                .config("spark.driver.memory", "1g")
                .config("spark.executor.memory", "2g")
                .config("spark.sql.shuffle.partitions", "2")
                .config("spark.sql.warehouse.dir", wareHouse)
                .config("spark.default.parallelism", "2")
                .master("local[*]")
                .getOrCreate();
//        spark.sparkContext().setLogLevel("error");



        StructType schema = new StructType()
                .add("id", "string")
                .add("ticker", "string")
                .add("price", "double")
                .add("created_at", "string")
                .add("updated_at", "string");

        Dataset<Row> data = spark.read().format("csv").option("delimiter", "|").schema(schema)
                .load("ticker.csv");

        data.show();

        Dataset<Row> df2 = data.coalesce(1);
        df2.write().format("org.apache.hudi")
                .option("hoodie.insert.shuffle.parallelism", "1")
                .option("hoodie.upsert.shuffle.parallelism", "1")
                .option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY(), "id")
                .option(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY(), "created_at")
                .option(DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY(), "updated_at")
                .option(HoodieWriteConfig.TABLE_NAME, "hudi_ticker")
                .option(DataSourceWriteOptions.MOR_TABLE_TYPE_OPT_VAL(),HoodieTableType.MERGE_ON_READ.name())
                .mode(SaveMode.Append).save(tablePath);
//        // Create the write client to write some records in
//        HoodieWriteConfig cfg = HoodieWriteConfig.newBuilder().withPath(tablePath)
//                .withSchema(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA).withParallelism(2, 2).forTable(tableName)
//                .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.BLOOM).build())
//                .withCompactionConfig(HoodieCompactionConfig.newBuilder().archiveCommitsWith(20, 30).build()).build();
//        HoodieWriteClient client = new HoodieWriteClient<>(jsc, cfg);
//        client.upsertPreppedRecords()

    }

}
