import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;

public class MetricsAggregation {

    public static final String INPUT_CSV = "src/main/resources/testMetricsBigger.csv";
    public static final String OUTPUT_CSV = "src/main/resources/result";

    public static final String TIME_BUCKET = "12 hours";

    public static final String TIMESTAMP_COL = "timestamp";
    public static final String METRIC_COL = "metric";
    public static final String AVERAGE_COL = "average";
    public static final String MIN_COL = "min";
    public static final String MAX_COL = "max";

    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder()
                .appName("MetricsAggregation")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> inputDF = spark.read()
                .option("header", "true")
                .csv(INPUT_CSV)
                .repartition(col(TIMESTAMP_COL).substr(1, 10)); // repartition by the date part of timestamp
        ;

        inputDF = inputDF
                .withColumn(TIMESTAMP_COL, to_timestamp(col(TIMESTAMP_COL), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"));


        // Perform aggregation using time window and metric and few aggregate functions
        Dataset<Row> aggregatedDF = inputDF.groupBy(
                window(col(TIMESTAMP_COL), TIME_BUCKET),
                col(METRIC_COL)
        ).agg(
                format_number(avg("value"), 2).alias(AVERAGE_COL),
                min("value").alias(MIN_COL),
                max("value").alias(MAX_COL)
        ).orderBy(
                col("window"),
                col(METRIC_COL)
        ).select("window.start", "window.end", METRIC_COL, AVERAGE_COL, MIN_COL, MAX_COL);

        aggregatedDF.cache(); // cache to prevent inout data reread for 2 outputs

        aggregatedDF.show();
        aggregatedDF.write()
                .option("header", "true")
                .mode(SaveMode.Overwrite)
                .csv(OUTPUT_CSV);

        spark.stop();
    }
}
