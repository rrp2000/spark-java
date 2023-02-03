package com.tutorial.spark;

import com.tutorial.spark.model.PersonModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;

public class Main {
    public static void main(String[] args){
        try{
            System.out.println("hello");
            SparkSession spark = SparkSession.builder().appName("Simple Application").master("local").getOrCreate();
//            Dataset<Row> csv = spark.read().format("csv")
//                    .option("header","true")
//                    .load("C:\\Users\\ranja\\Downloads\\spark-starter\\src\\main\\java\\resources\\csvFile.csv");
//            csv.show();

            Dataset<Row> data = spark.createDataFrame(Arrays.asList(
                    new PersonModel("Rosan","Patel"),
                    new PersonModel("Ranjan","Patel")
            ), PersonModel.class);
            data.show();
            // Write the DataFrame to a CSV file
            data.write().format("csv").mode(SaveMode.Overwrite).save("C:\\Users\\ranja\\Downloads\\spark-starter\\src\\main\\java\\resources\\person.csv");
//            data.write().csv("C:\\Users\\ranja\\Downloads\\spark-starter\\src\\main\\java\\resources\\csvPersons");

            spark.stop();



        }catch (Exception e){
            System.out.println(e.getMessage());
        }


    }
}
