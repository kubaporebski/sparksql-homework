import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


public class Task {

    public static void main(String[] args) {

        SparkSession spark = SparkSession
                .builder()
                .master("local[*]")
                .appName("SparkSQL")
                .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
                .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
                .config("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
                .config("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
                .getOrCreate();

        Dataset<Row> data = spark
                .read()
                .option("header", true)
                .option("inferSchema", true)
                .csv("gs://storage-bucket-secure-frog/output");


        data.createOrReplaceTempView("data");

        spark.sql("SELECT * FROM data LIMIT 1")
                .coalesce(1)
                .write()
                .csv("gs://storage-bucket-secure-frog/output_sql");

        spark.stop();
    }
}



