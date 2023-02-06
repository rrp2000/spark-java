package com.tutorial.spark;

import com.tutorial.spark.model.PersonModel;
import org.apache.spark.sql.*;

import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.col;

public class Main {
    public static void main(String[] args){
        try{
//            System.out.println("hello");
            SparkSession spark = SparkSession.builder().appName("Simple Application").master("local").getOrCreate();
//            Dataset<Row> csv = spark.read().format("csv")
//                    .option("header","true")
//                    .option("delimiter","\t")
//                    .load("src/main/java/resources/csvFile.csv");
////
//            csv.printSchema();
//            csv.show();
//            csv.select(col("Location")).show();

//            //writing it into another csv file
//
//            csv.coalesce(1).write().option("header",true).mode(SaveMode.Append).csv("C:\\Users\\ranja\\Downloads\\spark-starter\\src\\main\\java\\resources\\generated");

            //selecting columns
//            Dataset<Row> selectedData = csv.select(col("column1"), col("column2"), col("column3"));
//            selectedData.show();

            //selecting cols
//            Dataset<Row> selectedCols = csv.select(col("Age"));
//            selectedCols.show();

            //filtering data
//            Dataset<Row> filteredData = csv.filter(col("Age").as("Age").gt(10));
//            filteredData.show();




            // writing into csv file


            Dataset<Row> data = spark.createDataFrame(Arrays.asList(
                    new PersonModel("Rosan",21,"a@g.com","rerere"),
                    new PersonModel("Ranjan",100,"a@g.com","rerere")
            ), PersonModel.class);
            data.show();
            // Write the DataFrame to a CSV file
//            data.write().format("csv").mode(SaveMode.Overwrite).option("header",true).save("C:\\Users\\ranja\\Downloads\\spark-starter\\src\\main\\java\\resources\\person.csv");
            data.write().option("header",true).mode(SaveMode.Append).csv("C:\\Users\\ranja\\Downloads\\spark-starter\\src\\main\\java\\resources\\generate");
//
//            spark.stop();

// Show the first 10 records in the DataFrame
//            csv.show(10);

// Register the DataFrame as a temporary view
//            csv.createOrReplaceTempView("data");

// Query the data using Spark SQL
//            Dataset<Row> result = spark.sql("SELECT * FROM data WHERE age > 20");

// Show the first 10 records in the result
//            result.show(10);

// Group the data by the 'gender' column and calculate the average 'age'
//            Dataset<Row> grouped = csv.groupBy(col("gender")).agg(avg(col("age")));

// Show the result
//            grouped.show();

// Write the result to a Parquet file
//            grouped.write().format("parquet").mode(SaveMode.Overwrite).save("path/to/output");

//
//            List<PersonModel> people = Arrays.asList(
//                    new PersonModel("Horvath", 30),
//                    new PersonModel("Brad", 25),
//                    new PersonModel("Jane", 35),
//                    new PersonModel("Jay", 35));

            // Convert the list to a Dataset
//            Dataset<PersonModel> peopleDataset = spark.createDataset(people, Encoders.bean(PersonModel.class));
//
//            peopleDataset.show();

            //selecting cols            //selecting columns
//                csv.show();
//            Dataset<Row> selectedCols = csv.select(col("Department,"));
//            selectedCols.show();


            //data from sql
//
//            String url = "jdbc:mysql://localhost:3306/student";
//            String driver = "com.mysql.jdbc.Driver";
//            String user = "root";
//            String password = "admin";

//            Dataset<Row> df =  spark.read()
//                .format("jdbc")
//                .option("driver", driver)
//                .option("url", url)
//                .option("user", user)
//                .option("password", password)
//                .option("dbtable", "students")
//                .load();
//
//            System.out.println("start");
////            df.count();
//            df.show(10);
//            System.out.println("end");
//
//            spark.stop();


        }catch (Exception e){
            System.out.println(e.getMessage());
        }


    }
}
