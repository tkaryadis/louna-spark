## λouna
- Experimental project
- Uses sparql like queries(in s-expression syntax), to generate spark-SQL.
- Clojars [[louna/louna-spark "0.1.0-SNAPSHOT"]](https://clojars.org/louna/louna-spark)  
- Documentation [https://tkaryadis.github.io/louna-spark](https://tkaryadis.github.io/louna-spark/)  

## Examples
[Solutions to Spark The definitive guide](https://github.com/tkaryadis/louna-spark-def-guide)  
From the book [Spark: The Definitive Guide by Matei Zaharia, Bill Chambers](https://www.oreilly.com/library/view/spark-the-definitive/9781491912201/)  
Book scala code and data [here](https://github.com/databricks/Spark-The-Definitive-Guide)  
To run the examples read test/sparkdefinitive/ch1Readme.clj  
(Examples and the book solutions are included also inside the louna-spark main repository inside test package)    
The examples are tested with **Apache Spark 2.4.0** and **JDK 8**  

Example query

```
(q  df
    ((.and (=_ ?StockCode "DOT") 
           (.or (>_ ?UnitPrice 600)
                (includes? ?Description "POSTAGE"))) ?expensive)
    ((?expensive))
    [?expensive ?unitPrice]
    (.show 5))
```

Equivalent to
```
val DOTCodeFilter = col("StockCode") === "DOT"
val priceFilter = col("UnitPrice") > 600
val descripFilter = col("Description").contains("POSTAGE") 
df.withColumn("isExpensive", 
              DOTCodeFilter.and(priceFilter.or(descripFilter)))
  .where("isExpensive")
  .select("unitPrice", "isExpensive")
  .show(5) 
```
