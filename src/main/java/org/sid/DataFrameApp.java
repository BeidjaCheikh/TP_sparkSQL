package org.sid;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class DataFrameApp {
    public static void main(String[] args) {
        // Créer une session Spark
        SparkSession ss = SparkSession.builder().appName("TP SPARK SQL").master("local[*]").getOrCreate();

        // Lire les données à partir de la table "consultations" dans MySQL
        Dataset<Row> df1 = ss.read().format("jdbc")
                .option("driver", "com.mysql.jdbc.Driver")
                .option("url", "jdbc:mysql://localhost:3306/db_ecommerce")
//                .option("dbtable", "produits")
                .option("query","select * from produits")
                .option("user", "root")
                .option("password", "")
                .load();
                df1.show();
                df1.printSchema();
    }
}
