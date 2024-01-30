package org.example;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import java.util.Properties;
import java.util.logging.Logger;

public class Main {
    public static void main(String[] args) {

        String jdbcUrl = "jdbc:postgresql://localhost:5432/postgres";
        String tableName = "user_consent.user_details";

        String appName = "JDBC Spark";
        String nodes = "*";

        String username = "postgres";
        String password = "1234";

        SparkConf config = new SparkConf();
        config.set("credentialsFile", "src/main/resources/key.json");
        config.set("spark.jars.packages","com.google.cloud.spark:spark-bigquery-with-dependencies_2.13:0.28.0");
        SparkSession sparkSession = SparkSession.builder()
                .appName(appName)
                .config(config)
                .master("local["+nodes+"]")
                .getOrCreate();

        Properties connectionProperties = new Properties();
        connectionProperties.setProperty("user", username);
        connectionProperties.setProperty("password", password);

        try {
            // Load the JDBC driver
            Class.forName("org.postgresql.Driver");

            Dataset<Row> df = sparkSession.read().jdbc(jdbcUrl, tableName, connectionProperties);

            df.write().format("com.google.cloud.spark.bigquery")
                    .option("writeMethod", "direct")
                    .mode(SaveMode.Append)
                    .save("gcp-spark-407407.person_dataset.user_details_2");


        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
//        df.show();
    }
}