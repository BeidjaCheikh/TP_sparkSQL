package org.sid;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class App2 {

    public static void main(String[] args) {

        // Créer une session Spark
        SparkSession ss = SparkSession.builder().appName("TP SQPARKS SQL").master("local[*]").getOrCreate();

        // Lire le fichier CSV des incidents
        Dataset<Row> df1 = ss.read().option("header", true).option("inferSchema", true).csv("incidents.csv");

        // Extraire l'année de chaque incident
        Dataset<Row> incidentsWithYearDF = df1.withColumn("year", df1.col("date").substr(0, 4));

        // Calculer le nombre d'incidents par année
        Dataset<Row> incidentsByYearDF = incidentsWithYearDF.groupBy("year").count();


        // Trier les résultats par nombre d'incidents
        Dataset<Row> incidentsByYearDFSorted = incidentsByYearDF.sort(incidentsByYearDF.col("count").desc());

        // Afficher les deux premières lignes
        incidentsByYearDFSorted.show(2);

    }}
