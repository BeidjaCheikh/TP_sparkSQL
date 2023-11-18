# Big Data Spark SQL


```
This project includes a brief but informative and simple explanation of Apache Spark and
Spark SQL terms with java implementation. There are few structured examples to clear
the concept and terms in Apache Spark and Spark SQL altogether.
```

## Table of Contents
1. [Overview](#overview)
2. [Dependencies](#dependencies)
3. [Applications](#applications)
    * [Products SQL](#products-sql)
    * [Incidents SQL](#incidents-sql)
    * [Hopital Database](#hopital-database)


## Overview

### Spark SQL

Spark SQL is a Spark module for structured data processing. Unlike the basic Spark RDD API, the interfaces provided by Spark SQL provide Spark with more information about the structure of both the data and the computation being performed. Internally, Spark SQL uses this extra information to perform extra optimizations.

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

![image](https://github.com/el-moudni-hicham/bigdata-spark-sql/assets/85403056/12cd8ab1-d6d5-4f85-b1d7-5162608c6b83)

* MEDECINS :

![image](https://github.com/el-moudni-hicham/bigdata-spark-sql/assets/85403056/23e115fb-f9ed-42a0-a1b8-5d6ce0fe332c)

* CONSULTATIONS :

![image](https://github.com/el-moudni-hicham/bigdata-spark-sql/assets/85403056/3a488794-074a-4189-89bf-7969e8a8674b)

* Get access to data in database :

```java
        SparkSession ss = SparkSession.builder().appName("MySQL").master("local[*]").getOrCreate();

        Map<String, String> option = new HashMap<>();
        option.put("driver", "com.mysql.jdbc.Driver");
        option.put("url", "jdbc:mysql://localhost:3306/db_hopital_spark");
        option.put("user","root");
        option.put("password","");

        Dataset<Row> df1 = ss.read().format("jdbc")
                .options(option)
                //.option("table","table_name")
                .option("query","select * from consultations")
                .load();
        Dataset<Row> df2 = ss.read().format("jdbc")
                .options(option)
                .option("query","select * from medecins")
                .load();
```

* Display the number of consultations per day :

```java
df1.groupBy(col("DATE_CONSULTATION").alias("day")).count().show();
```

![image](https://github.com/el-moudni-hicham/bigdata-spark-sql/assets/85403056/9dfd46f1-5606-46d7-91d8-b24a300b631c)


* Display the number of consultations per doctor. The display format is as follows:

`NAME | SURNAME | NUMBER OF CONSULTATIONS`

```java
        Dataset<Row> dfConsultations = df1.groupBy(col("id_medecin")).count();
        Dataset<Row> dfMedicins = df2.select("id","nom","prenom");

        Dataset<Row> joinedDF = dfMedicins
                .join(dfConsultations, dfMedicins.col("id").equalTo(dfConsultations.col("id_medecin")), "inner")
                .select(dfMedicins.col("nom"), dfMedicins.col("prenom"),
                 dfConsultations.col("count").alias("NOMBRE DE CONSULTATION"))
                .orderBy(col("NOMBRE DE CONSULTATION").desc());

        joinedDF.show();
```

![image](https://github.com/el-moudni-hicham/bigdata-spark-sql/assets/85403056/a20eaf6d-cdaf-4864-a2e9-8122f7724aab)

