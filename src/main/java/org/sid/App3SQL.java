package org.sid;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count;

public class App3SQL {

    public static void main(String[] args) {
        // Créer une session Spark
        SparkSession ss = SparkSession.builder().appName("TP SPARK SQL").master("local[*]").getOrCreate();

        // Exécuter la première requête : Afficher le nombre de consultations par jour
        System.out.println("Nombre de consultations par jour :");

        // Chargement des données de consultations
        Dataset<Row> df1 = ss.read().format("jdbc")
                .option("url", "jdbc:mysql://localhost:3306/db_hopital")
                .option("driver", "com.mysql.jdbc.Driver")
                .option("user", "root")
                .option("password", "")
                .option("query", "select * from consultations")
                .load();

        // Chargement des données des médecins
        Dataset<Row> df2 = ss.read().format("jdbc")
                .option("url", "jdbc:mysql://localhost:3306/db_hopital")
                .option("query", "select * from medecins")
                .option("user", "root")
                .option("password", "")

                .load();

        // Afficher le nombre de consultations par jour
        df1.groupBy(col("DATE_CONSULTATION").alias("DATE de CONSULTATION")).count().show();

        // Afficher le nombre de consultations par médecin
        Dataset<Row> dfConsultations = df1.groupBy(col("id_medecin")).count();

        // Renommer la colonne "id" de df2 pour correspondre à dfConsultations
        Dataset<Row> dfMedecins = df2.withColumnRenamed("id", "id_medecin");

        // Jointure des données des médecins avec le nombre de consultations
        Dataset<Row> joinedDF = dfMedecins
                .join(dfConsultations, dfMedecins.col("id_medecin").equalTo(dfConsultations.col("id_medecin")), "inner")
                .select(dfMedecins.col("nom"), dfMedecins.col("prenom"), dfConsultations.col("count").alias("NOMBRE_DE_CONSULTATION"))
                .orderBy(col("NOMBRE_DE_CONSULTATION").desc());

        // Afficher le résultat
        joinedDF.show();

        // Afficher le nombre de patients assistés par chaque médecin
        Dataset<Row> dfPatientsParMedecin = df1.groupBy(col("id_medecin"))
                .agg(count("id_patient").alias("NOMBRE_DE_PATIENTS"));
        dfPatientsParMedecin.show();

    }
}
