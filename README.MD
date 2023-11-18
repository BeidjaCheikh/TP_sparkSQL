# Big Data Spark SQL


```
TP: Spark SQL
```

## Table of Contents
1. [Overview](#overview)
2. [Dependencies](#dependencies)
3. [Applications](#applications)
    * [Incidents SQL](#incidents-sql)
    * [Hopital Database](#hopital-database)


## Overview

### Spark SQL

Spark SQL est un module Spark pour le traitement de données structurées. Contrairement à l'API Spark RDD de base, les interfaces fournies par Spark SQL fournissent à Spark plus d'informations sur la structure des données et sur le calcul effectué. En interne, Spark SQL utilise ces informations supplémentaires pour effectuer des optimisations supplémentaires.
## Dependencies

* Spark SQL

  Ajoutez ces dépendances maven au fichier pom.xml:

  ```maven
       <dependency>
           <groupId>org.apache.spark</groupId>
           <artifactId>spark-core_2.13</artifactId>
           <version>3.4.1</version>
       </dependency>
       <dependency>
           <groupId>org.apache.spark</groupId>
           <artifactId>spark-sql_2.13</artifactId>
           <version>3.4.1</version>
       </dependency>
  ```

* Java

## Applications
### Incidents SQL
Nous souhaitons développer une application Spark pour une entreprise industrielle qui traite les incidents de chaque service.
Les incidents sont stockés dans un fichier CSV.

![image](https://github.com/BeidjaCheikh/TP_sparkSQL/blob/master/images/img1.png)

```
SparkSession ss = SparkSession.builder().appName("TP SQPARKS SQL").master("local[*]").getOrCreate();
// Lire le fichier CSV des incidents
Dataset<Row> df1 = ss.read().option("header", true).option("inferSchema", true).csv("incidents.csv");
```

* Afficher le nombre d'incidents par service.

```
  // Calculer le nombre d'incidents par service
     Dataset<Row> incidentsByServiceDF = df1.groupBy("service").count();
  // Afficher les résultats
     incidentsByServiceDF.show();
```

![image](https://github.com/BeidjaCheikh/TP_sparkSQL/blob/master/images/img2.png)

* Affichez les deux années avec le plus d'incidents.

```
     // Extraire l'année de chaque incident
        Dataset<Row> incidentsWithYearDF = df1.withColumn("year", df1.col("date").substr(0, 4));
     // Calculer le nombre d'incidents par année
        Dataset<Row> incidentsByYearDF = incidentsWithYearDF.groupBy("year").count();
     // Trier les résultats par nombre d'incidents
        Dataset<Row> incidentsByYearDFSorted = incidentsByYearDF.sort(incidentsByYearDF.col("count").desc());
     // Afficher les deux premières lignes
        incidentsByYearDFSorted.show(2);
```

![image](https://github.com/BeidjaCheikh/TP_sparkSQL/blob/master/images/img3.png)

### Hopital Database

Créez une base de données MySQL nommée DB_HOPITAL, qui contient trois tables

* PATIENTS :

![image](https://github.com/BeidjaCheikh/TP_sparkSQL/blob/master/images/img4.png)

* MEDECINS :

![image](https://github.com/BeidjaCheikh/TP_sparkSQL/blob/master/images/img5.png)

* CONSULTATIONS :

![image](https://github.com/BeidjaCheikh/TP_sparkSQL/blob/master/images/img6.png)

* -// Créer une session Spark:

```
SparkSession ss = SparkSession.builder().appName("TP SPARK SQL").master("local[*]").getOrCreate();

```

* // Chargement des données de consultations :

```
       Dataset<Row> df1 = ss.read().format("jdbc")
                .option("url", "jdbc:mysql://localhost:3306/db_hopital")
                .option("driver", "com.mysql.jdbc.Driver")
                .option("user", "root")
                .option("password", "")
                .option("query", "select * from consultations")
                .load();
```
* // Chargement des données des médecins :
```
 // Chargement des données des médecins
        Dataset<Row> df2 = ss.read().format("jdbc")
                .option("url", "jdbc:mysql://localhost:3306/db_hopital")
                .option("query", "select * from medecins")
                .option("user", "root")
                .option("password", "")

                .load();
```
*  // Afficher le nombre de consultations par jour :
```
   df1.groupBy(col("DATE_CONSULTATION").alias("DATE de CONSULTATION")).count().show();
```
![image](https://github.com/el-moudni-hicham/bigdata-spark-sql/assets/85403056/a20eaf6d-cdaf-4864-a2e9-8122f7724aab)

