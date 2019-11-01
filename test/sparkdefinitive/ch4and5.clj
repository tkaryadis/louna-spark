(ns sparkdefinitive.ch4and5
  (:require louna.datasets.schema
            [louna.datasets.sql :as sql]
            [louna.state.settings :as settings]
            sparkdefinitive.init-settings)
  (:use louna.datasets.column
        louna.datasets.sql-functions
        louna.q.run)
  (:import (org.apache.spark.sql Dataset)
           (org.apache.spark.sql.types DataTypes Metadata)
           [org.apache.spark.sql functions Row SparkSession Column]))


(sparkdefinitive.init-settings/init)

;;----------------------------ch4--------------------------------------------------

;;val df = spark.range(500).toDF("number")
;;df.select(df.col("number") + 10)
;;spark.range(2).toDF().collect()
;;import org.apache.spark.sql.types._
;;val b = ByteType

(def df (sql/seq->df (range 20) [["number" :long]]))

(.show df)

(sql/print-rows (.collect df))

;;Array of rows -> collection or vectors
(prn (map sql/row->seq (.collect df)))

(def b (:byte louna.datasets.schema/schema-types))

;;Array of rows
(prn (type (.collect df)))
(prn (type b))

;;---------------------------Schema-auto/manual------------------------------------
;;---------------------------------------------------------------------


;;val df = spark.read.format("json")
;  .load("/data/flight-data/json/2015-summary.json")
;df.printSchema()

;;val df = spark.read.format("json").schema(myManualSchema)
;;.load("/data/flight-data/json/2015-summary.json")

;;val myManualSchema = StructType(Array(
;  StructField("DEST_COUNTRY_NAME", StringType, true),
;  StructField("ORIGIN_COUNTRY_NAME", StringType, true),
;  StructField("count", LongType, false,
;    Metadata.fromJson("{\"hello\":\"world\"}"))
;))

(def myManualSchema
  (louna.datasets.schema/build-schema
    [["DEST_COUNTRY_NAME"]
     ["ORIGIN_COUNTRY_NAME"]
     ["count" :long false (Metadata/fromJson "{\"hello\":\"world\"}")]]))

(def df (-> (.read (get-session))
            (.format "json")
            ;;(.schema myManualSchema)  ;;without it auto-refered schema
            (.load (str (settings/get-base-path) "data/flight-data/json/2015-summary.json"))))

(prn (str (.schema df)))
(.printSchema df)

;;---------------------------Columns-----------------------------------
;;---------------------------------------------------------------------

;;import org.apache.spark.sql.functions.{col, column}
;;col("someColumnName")
;;column("someColumnName")

;;object of org.apache.spark.sql.Column
(prn (type (col "aname")))

(prn (type (col 'aname)))

;;df.col("count")

;;refer to column named "count" in the dataframe df
(.col df "count")


;;(((col("someCol") + 5) * 200) - 6) < col("otherCol")

(def myCol (<_ (*_ (+_ (col "someCol") 5) 200)
               (col "otherCol")))

(prn (type myCol))

;;spark.read.format("json").load("/data/flight-data/json/2015-summary.json")
;;.columns

;;print header
(prn (sql/get-header df))

;;---------------------------ROWS--------------------------------------
;;---------------------------------------------------------------------


;;df.first()

;;org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
(prn (type (.first df)))

;;print the first row
(sql/print-row (.first df))

;;import org.apache.spark.sql.Row
;;val myRow = Row("Hello", null, 1, false)

;;create a row   Row(value1, value2, value3, ...)
(def myrow (sql/seq->row ["hello" nil 1 false]))

;;org.apache.spark.sql.catalyst.expressions.GenericRow (no schema)
(prn (type myrow))
(sql/print-row myrow)

;;myRow(0) // type Any
;myRow(0).asInstanceOf[String] // String
;myRow.getString(0) // String
;myRow.getInt(2) // Int

;;get column value in a row by index
(prn (sql/get-row myrow 0))
(prn (.getString myrow 0))
(prn (type (.getLong myrow 2)))                             ;;;TODO

;;get column value in a row by column name
(prn (sql/get-row (.first df) "DEST_COUNTRY_NAME"))

;;-----------------------memmory  Seq->df+schemas-------------------------------
;;---------------------------------------------------------------------

;;val myManualSchema = new StructType(Array(
;  new StructField("some", StringType, true),
;  new StructField("col", StringType, true),
;  new StructField("names", LongType, false)))
;val myRows = Seq(Row("Hello", null, 1L))
;val myRDD = spark.sparkContext.parallelize(myRows)
;val myDf = spark.createDataFrame(myRDD, myManualSchema)
;myDf.show()
;
;// in Scala
;val myDF = Seq(("Hello", 2, 1L)).toDF("col1", "col2", "col3")          ;;;TODO schema infer from seq in memory

(def myManualSchema [["some"] ["col"] ["names" :long false]])
(def mydf (sql/seq->df [["Hello" nil 1]] myManualSchema))

(.show mydf)
(.printSchema mydf)

;;-----------------Names of columns----------------------------------

;;den bazo kena i -,ean balo prepei escaped px `my-column name` mesa
;;stis expressiosn toulaxiston  sel 79


;;Spark is CASE IGNORING,so names for capitals are the same if capital or
;;not,to change it
;;set spark.sql.caseSensitive true

;;-----------------------Select columns--------------------------------

;;selectExpr is for SQL expression,but i will use the functions
;;versions with no SQL

;;df.select("DEST_COUNTRY_NAME").show(2)

(q (df ?dest)
   (.show 2))

;;or (_ mean select it with its original name)

(q (df _)
   (.show 2))

;;or

(q df
   [?DEST_COUNTRY_NAME]
   (.show 2))


;;df.select("DEST_COUNTRY_NAME", "ORIGIN_COUNTRY_NAME").show(2)

(q  (df ?dest ?origin)
    (.show 2))

;;or

(q  (df _ _)
    (.show 2))

;;or
(q  df
    [?DEST_COUNTRY_NAME ?ORIGIN_COUNTRY_NAME]
    (.show 2))


;;df.select(
;    df.col("DEST_COUNTRY_NAME"),
;    col("DEST_COUNTRY_NAME"),
;    column("DEST_COUNTRY_NAME"),
;    'DEST_COUNTRY_NAME,
;    $"DEST_COUNTRY_NAME",
;    expr("DEST_COUNTRY_NAME"))
;  .show(2)

(q df
   [(.col df ?:DEST_COUNTRY_NAME)]
   (.show 3))


;;or  (sql-expr can be used but the point is to not use mixed clojure+sql)

(q (df ?DEST_COUNTRY_NAME)
   (.show 3))

;;df.select(expr("DEST_COUNTRY_NAME AS destination")).show(2)

(q (df ?destination)
   (.show 2))

;;df.select(expr("DEST_COUNTRY_NAME as destination").alias("DEST_COUNTRY_NAME"))
;  .show(2)

(q (df ?destination)
   (rename ?destination ?DEST_COUNTRY_NAME)                 ;;withColumnRenamed
   (.show 2))

;;or

(q (df ?destination)
   [(.alias ?destination ?:DEST_COUNTRY_NAME)]              ;;select + column functions
   (.show 2))

;;df.selectExpr(
;    "*",
;    "(DEST_COUNTRY_NAME = ORIGIN_COUNTRY_NAME) as withinCountry")
;  .show(2)

;;df.withColumn("withinCountry", expr("ORIGIN_COUNTRY_NAME == DEST_COUNTRY_NAME"))
;  .show(2)

;;__ mean select all the ones i didn't mention
(q (df ?dest ?origin __)
   ((=_ ?dest ?origin) ?withinCountry)
   (.show 2))


;;df.selectExpr("avg(count)", "count(distinct(DEST_COUNTRY_NAME))").show(2)

(q df
   [(avg ?count) (countDistinct ?DEST_COUNTRY_NAME)]
   (.show 2))

;;---------------------Add columns------------------------------------

;;df.select(expr("*"), lit(1).as("One")).show(2)
;;df.withColumn("numberOne", lit(1)).show(2)

(q df
   [* (.alias (lit 1) ?:one)]
   (.show 2))

(q df
   ((lit 1) ?numberOne)
   (.show 2))

;;val dfWithLongColName = df.withColumn(
;  "This Long Column-Name",
;  expr("ORIGIN_COUNTRY_NAME"))

;;things like "`This Long Column-Name` as `new col`" not related here
;;we avoid sql expressions

;;df.withColumn("count2", col("count").cast("long"))

(q df
   ((cast_ ?count :integer) ?count2)
   (.printSchema))


;;-------------------Delete Columns----------------------------------

;;df.drop("ORIGIN_COUNTRY_NAME").columns

(q df
   (drop-col ?ORIGIN_COUNTRY_NAME)
   sql/get-header
   prn)

;;drop >1 columns

(q df
   (drop-col ?ORIGIN_COUNTRY_NAME ?COUNT)
   (.show 2))

;;or

(q (df ?dest)
   (.show 2))

;;----------------------filter---------------------------------------

;;df.filter(col("count") < 2).show(2)
;;df.where("count < 2").show(2)

(q (df - - ?count)
   ((<_ ?count 2))
   (.show 2))

;;=== ?3:count   count is the third column in original header(starting from 1)
;; here is only 3 ,this notation is only for big headers like >10

(q (df ?3:count)
   ((<_ ?count 2))
   (.show 2))

;;df.where(col("count") < 2).where(col("ORIGIN_COUNTRY_NAME") == "Croatia")
;  .show(2)

(q df
   ((<_ ?count 2))
   ((=_ ?ORIGIN_COUNTRY_NAME "Croatia"))
   (.show 2))

;;or

(q df
   ((<_ ?count 2) (=_ ?ORIGIN_COUNTRY_NAME "Croatia"))
   (.show 2))

;;---------------------distinct--------------------------------------

;;df.select("ORIGIN_COUNTRY_NAME", "DEST_COUNTRY_NAME").distinct().count()

(q (df ?dest ?origin)
   (.distinct)
   (.count)
   (prn))

;;df.select("ORIGIN_COUNTRY_NAME").distinct().count()

(q (df - ?origin)
   (.distinct)
   (.count)
   (prn))


;;--------------------take a random sample--------------------------

;;val seed = 5
;val withReplacement = false
;val fraction = 0.5
;df.sample(withReplacement, fraction, seed).count()

;;here we expect to get a number ~count/2 (count=256 and here we get 126)
(-> df
    (.sample false                                        ;;replacement => no row is re-picked
             0.5                                          ;;fraction of rows,here 50%
             5                                            ;;seed
             )
    .count
    prn)


;;-------------------Random splits (sel 83)------------------

;;val dataFrames = df.randomSplit(Array(0.25, 0.75), seed)
;dataFrames(0).count() > dataFrames(1).count() // False

;;here we spit at 2 with sizes 0.25+0.75=1 (always will be 1 or auto converted to 1)
;;we get an array of 2 datasets
(def splitted-df (-> df
                     (.randomSplit (double-array [0.25 0.75]) 5)))

(prn (.count (aget splitted-df 0)))                         ;;60
(prn (.count (aget splitted-df 1)))                         ;;196=  ~3x60


;;------------------Union(append rows)-----------------------

;;import org.apache.spark.sql.Row
;val schema = df.schema
;val newRows = Seq(
;  Row("New Country", "Other Country", 5L),
;  Row("New Country 2", "Other Country 3", 1L)
;)
;val parallelizedRows = spark.sparkContext.parallelize(newRows)
;val newDF = spark.createDataFrame(parallelizedRows, schema)
;df.union(newDF)
;  .where("count = 1")
;  .where($"ORIGIN_COUNTRY_NAME" =!= "United States")
;  .show() // get all of them and we'll see our new rows at the end

(def newDF (sql/seq->df [["New Country" "Other Country" 5]
                         ["New Country 2" "Other Country 3" 1]]
                        (.schema df)))

;;just append the rows(duplicate is ok its not set operation)
;;datasets must have the same schema to use union

(q df
   (.union newDF)
   ((=_ ?count 1) (not=_ ?ORIGIN_COUNTRY_NAME "United States"))
   (.show))

;;------------------Sort-------------------------------------

;;sort=orderBy (exactly the same)

;;;df.sort("count").show(5)
;df.orderBy("count", "DEST_COUNTRY_NAME").show(5)
;df.orderBy(col("count"), col("DEST_COUNTRY_NAME")).show(5)

(q df
   (order-by ?count)
   (.show 5))

(q (df - ?dest ?count)
   (order-by ?count ?dest)
   (.show 5))


;;import org.apache.spark.sql.functions.{desc, asc}
;df.orderBy(expr("count desc")).show(2)
;df.orderBy(desc("count"), asc("DEST_COUNTRY_NAME")).show(2)

(q df
   (order-by (desc ?count))
   (.show 5))

(q df
   (order-by (desc ?count) (asc ?DEST_COUNTRY_NAME))
   (.show 5))

;;spark.read.format("json").load("/data/flight-data/json/*-summary.json")
;  .sortWithinPartitions("count")

;;na do sortwithin partitions sel 86 kai  ;;TODO
;;repartition sel 87

;;df.rdd.getNumPartitions // 1

(prn (.getNumPartitions (.rdd df)))

;;----------------limit--------------------------------------

;;df.limit(5).show()

(.show (.limit df 5))

;;df.orderBy(expr("count desc")).limit(6).show()

(q df (order-by (desc ?count)) (.limit 6) .show)

;;---------------partitions-related------------------------------

(def mydf (sql/seq->df (range 10) [["number" :long]]))

;;for me its 8 (maybe 1 per thread)
(prn (.getNumPartitions (.rdd mydf)))

;;coalesce
;;to only reduce number of partitions(move the data only from the partitions that will be deleted)
;;no data shuffle,i guess not equal partitions also ,but FAST
(prn (.getNumPartitions (.rdd (.coalesce mydf 4))))

;;repartition (increase or decrease)
;;data shuffle => slow , goal=equal distribution of data on partitions
;;i only do it if i want INCREASE on partitions,else coalesce
(prn (.getNumPartitions (.rdd (.repartition mydf 20))))

;;repartion by column
;;each partition will have the same value on dest_country_name ?
;;When partitioning by a column, Spark will create a minimum of 200 partitions by default
;;(maybe many of them completly empty)
;;Results in very fast filter
(q df
   (.repartition (ca ?DEST_COUNTRY_NAME)))

;;df.repartition(5, col("DEST_COUNTRY_NAME")).coalesce(2)
(q df
   (.repartition 5 (ca ?DEST_COUNTRY_NAME))
   (.coalesce 2))


;;---------------collect/take------------------------------------

;;H basiki diafora einai oti autes den epistrefoun dataset,opos px
;;i limit,alla local collection stin mnimi tou ipologisti me to driver

;;collect takes it all,take take the FIRST n
;;they both return => array with Rows


;;val collectDF = df.limit(10)
;collectDF.take(5) // take works with an Integer count
;collectDF.collect()
;collectDF.toLocalIterator()

(def collectDF (.limit df 10))

(prn (map sql/row->seq (.collect collectDF)))
(sql/print-rows (.take collectDF 5))
(prn (map sql/row->seq (iterator-seq (.toLocalIterator collectDF))))

;;;Completed
