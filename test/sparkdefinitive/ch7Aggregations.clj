(ns sparkdefinitive.ch7Aggregations
  (:require sparkdefinitive.init-settings
            [louna.state.settings :as settings]
            [louna.datasets.sql :as sql]
            [louna.datasets.grouped-datasets :as g]
            [louna.library.scalaInterop :as scala-inter])
  (:use louna.datasets.column
        louna.datasets.sql-functions
        louna.q.run
        louna.library.util)
  (:import [org.apache.spark.sql functions Dataset RelationalGroupedDataset]
           (org.apache.spark.sql.expressions Window WindowSpec)))


(sparkdefinitive.init-settings/init)

;;---------------------------------------------------------------------

(def df (-> (.read (get-session))
            (.format "csv")
            (.option "header" "true")
            (.option "inferSchema" "true")
            (.load (str (settings/get-base-path)
                        "/data/retail-data/all/*.csv"))
            (.coalesce 5)))

;coalesce=  poses partition thelo,alla den kani full shuffle,episis
;;          partitions isos oxi idio megethos,opos einai me repartition()

(.cache df)
(.createOrReplaceTempView df "dfTable")


;;-------------------Aggregations in Louna--------------------------------

;;1)No groupBy first = Table aggregations (the dataframe=1group)
;;  select + use of aggregating function of sql.fucntions
;;  [(agrfun1 ...) ((agrfun2 ...) ?myQvar)]   ;;binds also in select
;;  I can do the same using the .agg method of dataset so its obvious that its aggregation
;;  but for now binds dont work (sometime maybe make one agg functions,and one extra group-agg for 2.2) case )

;;2)GroupBy first (=> RelationalGroupedDataset object)
;;   1)1 aggregate function  => use of methods of the RelationalGroupedDataset object (if exist the one i need)
;;     (some may produce many collumns for example i can use (.aggr col1 col2 ..)
;;      but all new columns will be result of the same aggr function)
;;     Example
;;     (q df
;         (groupBy ?InvoiceNo ?CustomerId)
;         .count  ;;method tou RelationalGroupedDataset
;         (.show))
;;   2)multiple aggregate functions => (aggr + functions of sql.functions)
;;     Example
;;     (q df
;         (groupBy ?InvoiceNo)
;         (agg (sum ?Quantity) ((count_ ?Quantity) ?quan))   ;;normal aggr sql.functions,i can bind also
;         (.show))


;;-------------------Aggr all the DATAFRAME--------------------------------
;;-------------------(dataframe=1 group (no grouping))---------------------

;;df.count() == 541909
(q df .count prn)


;;df.select(count("StockCode")).show()
(q df [(count_ ?StockCode)] .show)

;;df.select(countDistinct("StockCode")).show() // 4070
(q df [(countDistinct ?StockCode)] .show)
(q df [((countDistinct ?StockCode) ?dc)] .show)


;;df.select(approx_count_distinct("StockCode", 0.1)).show() // 3364

(q df
   [(functions/approx_count_distinct ?StockCode 0.1)]
   .show)

;;first/last row
(q df
   [(functions/first ?StockCode) (functions/last ?StockCode)]
   (.show))


;;min-max (numericals,strings)
(q df
   [(min_ ?StockCode) (max_ ?StockCode)]
   (.show))

;;sum
(q df
   [(sum ?Quantity)]
   (.show))

;;sumDistinct
(q df
   [(functions/sumDistinct ?Quantity)]
   (.show))


;;avg(sum/#rows)
(q df
   [(avg ?Quantity)]
   (.show))

;;df.select(
;    count("Quantity").alias("total_transactions"),
;    sum("Quantity").alias("total_purchases"),
;    avg("Quantity").alias("avg_purchases"),
;    expr("mean(Quantity)").alias("mean_purchases"))
;  .selectExpr(
;    "total_purchases/total_transactions",
;    "avg_purchases",
;    "mean_purchases").show()

(q df
   [((count_ ?Quantity) ?totalTransactions)
    ((sum ?Quantity) ?totalPurchases)
    ((avg ?Quantity) ?avgPurchases)
    ((mean ?Quantity) ?meanPurchases)]
   [(div ?totalPurchases ?totalTransactions) ?avgPurchases ?meanPurchases]
   .show)


;;TODO skipped some statistic methodss
;;df.select(var_pop("Quantity"), var_samp("Quantity"),
;  stddev_pop("Quantity"), stddev_samp("Quantity")).show()
;;df.select(skewness("Quantity"), kurtosis("Quantity")).show()
;;df.select(corr("InvoiceNo", "Quantity"), covar_samp("InvoiceNo", "Quantity"),
;    covar_pop("InvoiceNo", "Quantity")).show()


;;df.agg(collect_set("Country"), collect_list("Country")).show()
;;TODO why use agg when select auto-understand aggregation functions?


;;creates a set and a list with the values of the column
;;both are array types
(def complexdf (q df
                   [(functions/collect_set ?Country) (functions/collect_list ?Country)]))

(.printSchema complexdf)
(.show complexdf)

;;-----------------group+aggr(aggregation on groups,not to all the table)-----------
;;----------------------------------------------------------------------------------

;;DataFrame -grouping-> RelationalGroupedDataset -> Dataframe

;;df.groupBy("InvoiceNo", "CustomerId").count().show()
(q df
   (groupBy ?InvoiceNo ?CustomerId)
   .count  ;;method of RelationalGroupedDataset
   (.show))



;;df.groupBy("InvoiceNo").agg(
;  count("Quantity").alias("quan"),
;  expr("count(Quantity)")).show()
(q df
   (groupBy ?InvoiceNo)
   (agg ((count_ ?Quantity) ?quan))
   (.show))


;;df.groupBy("InvoiceNo").agg("Quantity"->"avg").show()
;;no code in "..." so thing to avoid
(q df
   (groupBy ?InvoiceNo)
   (.agg (apply hash-map ["Quantity" "avg"]))
   (.show))


(def dfNoNull (q df drop-nulls))
;;val dfNoNull = dfWithDate.drop()


;;-----------------window functions----------------------------
;;-------------------------------------------------------------

;;Binding
;;1)row->row (row bindings using functions)

;;Aggr Binding
;;2)rows->groups-> 1 value per group

;;Window functions
;;Grouping to use the value in the function and do a bind
;;  1)rows->groups(frames)
;;  2)rows+use_of_frame->rows (using the above frame)
;;  For example add a column (true/false) to the employers that take more money than the average than their department
;;  One solution would be to produce a table with the averages,and then use this add a bind with value true/false
;;  That is what window function does,with 1 query.

;;Example

;;import org.apache.spark.sql.functions.{col, to_date}
;val dfWithDate = df.withColumn("date", to_date(col("InvoiceDate"),
;"MM/d/yyyy H:mm"))
;dfWithDate.createOrReplaceTempView("dfWithDate")

;;import org.apache.spark.sql.expressions.Window
;import org.apache.spark.sql.functions.col
;val windowSpec = Window
;.partitionBy("CustomerId", "date")
;.orderBy(col("Quantity").desc).rowsBetween(Window.unboundedPreceding, Window.currentRow)

;;val maxPurchaseQuantity = max(col("Quantity")).over(windowSpec)
;;val purchaseDenseRank = dense_rank().over(windowSpec)
;;val purchaseRank = rank().over(windowSpec)

;;dfWithDate.where("CustomerId IS NOT NULL").orderBy("CustomerId")
;.select(
;col("CustomerId"),col("date"),
;col("Quantity"),
;purchaseRank.alias("quantityRank"),
;purchaseDenseRank.alias("quantityDenseRank"),
;maxPurchaseQuantity.alias("maxPurchaseQuantity")).show()


;;Define windowSpec(frame) + Run the query
;;one customer maybe bought many things during one date and in different quantities
;;for example  customer=12347 on date=2010-12-07,bought 36 things,then another 30,then another 24 etc

;;Here we use each row from initial df + the frame(we defined) to produce
;;  1)1 value,the max things he brought in one day (we dont need the row)
;;  2)the rank for each quantity (for example if he bought  36,30,24,20  the rank of 30 will be 2)
;;  3)dense_rank?

;;rows between defines the boundaries of the frame,he every row will belong in a frame start -> current_row
;;its ordered by quantity so the rank of a row will be how many from the start,the next we dont care

(let [myWindowSpec
      (q (Window/partitionBy (ca ?CustomerId ?date))                   ;;creates a  WindowSpec
         (order-by (desc ?Quantity))                                   ;;order the WindowSpec
         (.rowsBetween (Window/unboundedPreceding) (Window/currentRow)))]
  (q df
     ((functions/to_date ?InvoiceDate "MM/d/yyyy H:mm") ?date)
     ((not-nil_? ?CustomerId))
     (order-by ?CustomerId)
     [?CustomerId ?date ?Quantity
      ((.over (functions/rank) myWindowSpec) ?rank)
      ((.over (functions/dense_rank) myWindowSpec) ?denseRank)
      ((.over (max_ ?Quantity) myWindowSpec) ?maxq)]
     .show))

;;-------------------------Grouping sets-----------------------------------

;;aggr -> groups -> values (1 per group)
;;grouping sets -> aggr on that values
;;its aggr on aggr results

;;not supported in Dataframes,instead we use Rollups
;;When using rollups/cubes we remove the nulls else results might problem

;;-----------------Cubes-------------------------------------------

;;cube=Group by ,but with don't care variables (all combinations of dont_care variables)
;;Dont care variable => match with every value in original table
;;                      takes null value in the final table

;;group by x,y
;;union
;;group by x_dont_care(null value),y
;;union
;;group by  x,y_dont_care(null value)
;;union
;;group by x_adiaforo(null_timi),y_adiaforo(null_timi)  ;;all the original table match


;; +---+---+
;; |  x|  y|
;; +---+---+
;; |foo|  1|
;; |foo|  2|
;; |bar|  2|
;; |bar|  2|
;; +---+---+

(def testdf (sql/seq->df [["foo" 1]
                          ["foo" 2]
                          ["bar" 2]
                          ["bar" 2]]
                         [["x"] ["y" :long]]))

(q testdf
   (.cube (ca ?x ?y))
   .count                                                   ;;count the same rows
   (order-by ?count)
   .show)


;; +----+----+-----+
;; |   x|   y|count|
;; +----+----+-----+
;; |null|   1|    1|    x_whatever , y=1
;; |null|   2|    3|    x_whatever , y=2
;; | foo|null|    2|    x=foo      , y_whatever
;; | bar|   2|    2|    x = bar    , y = 2
;; | foo|   1|    1|    x = foo , y = 1
;; | foo|   2|    1|    x = foo , y = 2
;; |null|null|    4|    x_whatever,y_whatever
;; | bar|null|    2|    x=bar,y_whatever
;; +----+----+-----+


;;----------------Grouping Metadata-------------------------------

;;Spark gives numbers to the grouping type,if i want
;;3  x_whatever y_whatever
;;2   x_value y_whatever
;;1   x_whatever y_value
;;0   x_value y_value

;;i can see that metadata using the grouping_id function

(q testdf
   (.cube (ca ?x ?y))
   (agg (functions/grouping_id (scala-inter/clj->scala [])))
   (order-by "?grouping_id()")
   .show)

;;-----------------Roll ups----------------------------------------

;;Original
;; +---+---+
;; |  x|  y|
;; +---+---+
;; |foo|  1|
;; |foo|  2|
;; |bar|  2|
;; |bar|  2|
;; +---+---+

;; +----+----+-----+
;; |   x|   y|count|
;; +----+----+-----+
;; | foo|null|    2|   x=foo, y_whatever
;; | bar|   2|    2|   x=bar ,y =2
;; | foo|   1|    1|   x=foo ,y=1
;; | foo|   2|    1|   x=foo ,y=2
;; |null|null|    4|   x_whatever,y_whatever
;; | bar|null|    2|   x=bar ,y_whatever
;; +----+----+-----+

;;it allows x_whatever only when y_whatever (=> dont have the x_whatever,y=something)
;;its like a restricted cude
(q testdf
   (.rollup (ca ?x ?y))
   .count
   (order-by ?count)
   .show)

;;--------------------Pivot--------------------------------------

;;each distinct row in the aggregation -> 1 column ?

;;val pivoted = dfWithDate.groupBy("date").pivot("Country").sum()


;;here i see for each date what each country did
(q df
   ((functions/to_date ?InvoiceDate "MM/d/yyyy H:mm") ?date) ;;data now is in the form 2010-12-01
   (groupBy ?date)
   (.pivot ?Country)
   (g/sum)
   .show)


;;TODO 2 EXAMPLES statistical pages 130,131
;;Completed
