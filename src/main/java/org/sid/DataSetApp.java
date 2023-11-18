package org.sid;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.List;

public class DataSetApp {

public static void main(String[] args) {
    // Créer une session Spark
    SparkSession ss = SparkSession.builder().appName("TP SPARK SQL").master("local[*]")
            .getOrCreate();
    List<Product> productList = new ArrayList<>();
    productList.add(new Product("DELL", 18000, 6));
    productList.add(new Product("Mac", 39000, 3));
    productList.add(new Product("acer", 20000,8));
    productList.add(new Product("hp", 32990, 7));
    Encoder<Product> productEncoder = Encoders.bean(Product.class);
    Dataset<Product> ds1 = ss.createDataset(productList, productEncoder);
    ds1.filter((FilterFunction<Product>) product -> product.getPrice()>16000).show();
}
}
