package org.sid;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class App1 {
    public static void main(String[] args) {

        // Créer une session Spark
        SparkSession ss = SparkSession.builder().appName("TP SQPARKS SQL").master("local[*]").getOrCreate();

        // Lire le fichier CSV des incidents
        Dataset<Row> df1 = ss.read().option("header", true).option("inferSchema", true).csv("incidents.csv");

        // Calculer le nombre d'incidents par service
        Dataset<Row> incidentsByServiceDF = df1.groupBy("service").count();

        // Afficher les résultats
        incidentsByServiceDF.show();
    }
}
