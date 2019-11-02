(ns sparkdefinitive.ch6DataTypes
  (:require sparkdefinitive.init-settings
            [louna.state.settings :as settings])
  (:use louna.datasets.column
        louna.datasets.sql-functions
        louna.q.run
        louna.library.util)
  (:import (org.apache.spark.sql SparkSession Dataset)
           [org.apache.spark.sql functions Column DataFrameStatFunctions]))


(sparkdefinitive.init-settings/init)

;;--------------------------Load test data--------------------------------

(def df (-> (.read (get-session))
            (.format "csv")
            (.option "header" "true")
            (.option "inferSchema" "true")
            (.load (str (settings/get-base-path)
                        "/data/retail-data/by-day/2010-12-01.csv"))))

(.printSchema df)

;;-----------------------------lit----------------------------------------
;;lit=>creates a column with that value(every row get 1 value)

;;df.select(lit(5), lit("five"), lit(5.0))

(q df
   [(lit 5) (lit "five") (lit 5.0)]
   (.show))

;;or

(q df
   ((lit 5) ?col1)
   ((lit "five") ?col2)
   ((lit 5.0) ?col3)
   [?col1 ?col2 ?col3]
   (.show))

;;-----------------------Filter methods---------------------------------

;;Column API

;;Column + Column , or Column with value

;;equalTo(Object other) notEqual(Object other)
;;gt(Object other)  lt,leq
;;bitwiseAND(Object other) bitwiseOR
;;contains(Object other)  px gia string
;;isin timi melos tis collection

;;Column sigrisi me alo Column => epistrfoun Column
;;or and

;;df.where(col("InvoiceNo").equalTo(536365))
;  .select("InvoiceNo", "Description")
;  .show(5, false)

;;df.where(col("InvoiceNo") === 536365)
;  .select("InvoiceNo", "Description")
;  .show(5, false)

(q (df ?invNo - ?desc)
   ((=_ ?invNo 536365))
   (.show 5 false))

;;df.where("InvoiceNo = 536365")
;  .show(5, false)

(q df
   ((=_ ?InvoiceNo 536365))
   (.show 5 false))

;;df.where("InvoiceNo <> 536365")
;  .show(5, false)

(q df
   ((not=_ ?InvoiceNo 536365))
   (.show 5 false))


;;val priceFilter = col("UnitPrice") > 600
;val descripFilter = col("Description").contains("POSTAGE")
;df.where(col("StockCode").isin("DOT")).where(priceFilter.or(descripFilter))
;  .show()

(q  df
    ((contains_? ?StockCode "DOT") (.or (>_ ?UnitPrice 600)
                                        (includes? ?Description "POSTAGE")))
    (.show 5))

;;val DOTCodeFilter = col("StockCode") === "DOT"
;val priceFilter = col("UnitPrice") > 600
;val descripFilter = col("Description").contains("POSTAGE")
;df.withColumn("isExpensive", DOTCodeFilter.and(priceFilter.or(descripFilter)))
;  .where("isExpensive")
;  .select("unitPrice", "isExpensive").show(5)

;;filter keep only the expensive,bind as true (all passed are expensive)
(q  df
    ((contains_? ?StockCode "DOT") (.or (>_ ?UnitPrice 600)
                                        (includes? ?Description "POSTAGE")))
    ((lit true) ?expensive)
    [?expensive ?unitPrice]
    (.show 5))

;;or

;;bind and filter to remove the not-expensive
(q  df
    ((.and (contains_? ?StockCode "DOT")
           (.or (>_ ?UnitPrice 600)
                (includes? ?Description "POSTAGE"))) ?expensive)
    ((true_? ?expensive))
    [?expensive ?unitPrice]
    (.show 5))


;;df.withColumn("isExpensive", not(col("UnitPrice").leq(250)))
;  .filter("isExpensive")
;  .select("Description", "UnitPrice").show(5)
;df.withColumn("isExpensive", expr("NOT UnitPrice <= 250"))
;  .filter("isExpensive")
;  .select("Description", "UnitPrice").show(5)

(q df
   ((not_ (<=_ ?UnitPrice 250)) ?isExpensive)
   ((true_? ?isExpensive))
   [?Description ?UnitPrice]
   (.show 5))


;;df.where(col("Description").eqNullSafe("hello")).show()

(q df
   ((.eqNullSafe ?Description "hello"))
   (.show))

;;--------------------numbers---------------------------------------

;;kano xrisi sinartiseon sql.fuctions px pow, kai Column sinartisis
;;autes epistrefoun Column results

;;pow,round,bround,multi

;;genika ean o tipos apotixi,epistrefoun null,px (round (lit "1s"))

;;iparxoun methods apo stats API kai describe

;;val fabricatedQuantity = pow(col("Quantity") * col("UnitPrice"), 2) + 5
;df.select(expr("CustomerId"), fabricatedQuantity.alias("realQuantity")).show(2)

(q (df ?Quantity:qt ?UnitPrice:price)
   ((+_ (pow (*_ ?qt ?price) 2.0) 5) ?realQt)
   (.show 2))

;;df.select(round(col("UnitPrice"), 1).alias("rounded"), col("UnitPrice")).show(5)
;;round allowing max 1 digit after .
(q (df ?UnitPrice)
   ((round ?UnitPrice 1) ?rounded)
   (.show 5))


;;df.select(round(lit("2.5")), bround(lit("2.5"))).show(2)

;bround => .5 to lower intenger,  round=> to higher integer
(q df
   [(round (lit "2.5")) (functions/bround (lit "2.5"))]
   (.show 2))


;;---------------------------statistic functions----------------------------


;;df.stat.corr("Quantity", "UnitPrice")

;;Pearson correlation coefficient for two columns
;;stat create a DataFrameStatFunctions object,that provides many statistical functions
(q df .stat (.corr ?:Quantity ?:UnitPrice))

;df.select(corr("Quantity", "UnitPrice")).show()
;;using sql functions
(q df
   [(functions/corr ?Quantity ?UnitPrice)]
   (.show))

;;df.describe().show()
;;count,mean,stddev,min,max for a numerical column i specify

(q df
   (.describe (sa ?:Quantity ?:UnitPrice))
   (.show))

;;val colName = "UnitPrice"
;val quantileProbs = Array(0.5)
;val relError = 0.05
;df.stat.approxQuantile("UnitPrice", quantileProbs, relError) // 2.51

;;stat => Class DataFrameStatFunctions

(q df
   .stat
   (.approxQuantile ?:UnitPrice (double-array [0.5]) 0.05)
   vec
   first
   prn)

;;df.stat.crosstab("StockCode", "Quantity").show()

(q df
   .stat
   (.crosstab ?:StockCode ?:Quantity)
   .show)


;;df.stat.freqItems(Seq("StockCode", "Quantity")).show()

(q df
   .stat
   (.freqItems (sa ?:StockCode ?:Quantity))
   (.show false))

;;df.select(monotonically_increasing_id()).show(2)

(q df
   [(functions/monotonically_increasing_id)]
   (.show 2))

;;df.select(initcap(col("Description"))).show(2, false)

(q df
   [(functions/initcap ?Description)]
   (.show 2 false))

;;-------------------strings---------------------------------------

;;methodoi pali apo sql.fuctions

;;px initcap(proto grama kefalaio),upper,lower,trim

(q (df ?Description:desc)
   ((lower ?desc) ?ldesc)
   ((upper ?desc) ?udesc)
   (.show 2))

;;df.select(
;    ltrim(lit("    HELLO    ")).as("ltrim"),
;    rtrim(lit("    HELLO    ")).as("rtrim"),
;    trim(lit("    HELLO    ")).as("trim"),
;    lpad(lit("HELLO"), 3, " ").as("lp"),
;    rpad(lit("HELLO"), 10, " ").as("rp")).show(2)

;;lpad rpad,if second argument biggest than the string length,adds the 3 arguments as many times until second argument
(let [hello (lit "    HELLO    ")
      hello1 (lit "HELLO")]
  (q df
     ((functions/ltrim hello) ?ltrim)
     ((functions/rtrim hello) ?rtrim)
     ((functions/trim hello) ?trim)
     ((functions/lpad hello1 3 " ") ?lp)
     ((functions/rpad hello1 10 " ") ?rp)
     (.show 2)))


;;------------------regular expressions ---------------------------------
;;like Java

;;val simpleColors = Seq("black", "white", "red", "green", "blue")
;val regexString = simpleColors.map(_.toUpperCase).mkString("|")
;// the | signifies `OR` in regular expression syntax
;df.select(
;  regexp_replace(col("Description"), regexString, "COLOR").alias("color_clean"),
;  col("Description")).show(2)


;;replace the colors with the string COLOR on the desctription column
(q df
   ((regexp_replace ?Description
                    "BLACK|WHITE|RED|GREEN|BLUE"
                    "COLOR") ?color_clean)
   [?color_clean ?Description]
   (.show false))

;;or

(q  df
    [?Description (.alias
                    (regexp_replace ?Description "BLACK|WHITE|RED|GREEN|BLUE" "COLOR")
                    ?:color_clean)]
    (.show false))



;;change characters,same index = corresponding character
;;for exampl "LET" "137"  L->1  E->3 E->3 T->7

;;df.select(translate(col("Description"), "LEET", "1337"), col("Description"))
;  .show(2)

(q df
   [?Description (functions/translate ?Description "LET" "137")]
   (.show 2))

;;val regexString = simpleColors.map(_.toUpperCase).mkString("(", "|", ")")
;// the | signifies OR in regular expression syntax
;df.select(
;     regexp_extract(col("Description"), regexString, 1).alias("color_clean"),
;     col("Description")).show(2)

(q df
   ((functions/regexp_extract ?Description
                              "(BLACK|WHITE|RED|GREEN|BLUE)"
                              1) ?color_clean)
   [?color_clean ?Description]
   (.show false))

;;val containsBlack = col("Description").contains("BLACK")
;val containsWhite = col("DESCRIPTION").contains("WHITE")
;df.withColumn("hasSimpleColor", containsBlack.or(containsWhite))
;  .where("hasSimpleColor")
;  .select("Description").show(3, false)

(q df
   ((.or  (includes? ?Description "BLACK") (includes? ?Description "WHITE")) ?BlackOrWhite)
   ((true_? ?BlackOrWhite))
   [?Description]
   (.show 3 false))     ;;false simeni min bali ... print akoma kai megalo na einai kapio string



;;val simpleColors = Seq("black", "white", "red", "green", "blue")
;val selectedColumns = simpleColors.map(color => {
;   col("Description").contains(color.toUpperCase).alias(s"is_$color")
;}):+expr("*") // could also append this value
;df.select(selectedColumns:_*).where(col("is_white").or(col("is_red")))
;  .select("Description").show(3, false)

(q df
   (sql-select (concat (map (fn [color]
                          (.alias (includes? ?Description (clojure.string/upper-case color))
                                  (str "is" color)))
                        ["black" "white" "red" "green" "blue"])
                   (list "*")))                                ;;TODO without use of select,with []
   ((.or (true_? ?iswhite) (true_? ?isred)))
   [?Description]
   (.show 3 false))


;;import org.apache.spark.sql.functions.{current_date, current_timestamp}
;val dateDF = spark.range(10)
;  .withColumn("today", current_date())
;  .withColumn("now", current_timestamp())
;dateDF.createOrReplaceTempView("dateTable")


;;dates and timestamps sel 104
;;basikes sinartisis  sql.functions
;;current_date()  (returns date type = only date no time)
;;current_timestamp() (returns timestamp type =date+time)
;;san strings einai px
;;2019-05-01   KAI   2019-05-01 21:59:02.449

(def datedf (q (.range (get-session) 10)
               ((functions/current_date) ?today)
               ((functions/current_timestamp) ?now)
               [?today ?now]))

(.printSchema datedf)
(.show datedf false)


;;dateDF.select(date_sub(col("today"), 5), date_add(col("today"), 5)).show(1)

(q datedf
   [(functions/date_sub ?today 5) (functions/date_add ?today 5)]
   (.show 1))

;;dateDF.withColumn("week_ago", date_sub(col("today"), 7))
;  .select(datediff(col("week_ago"), col("today"))).show(1)



(q datedf
   ((functions/date_sub ?today 7) ?week_ago)
   [(functions/datediff ?week_ago ?today)]
   (.show 1))

;dateDF.select(
;    to_date(lit("2016-01-01")).alias("start"),
;    to_date(lit("2017-05-22")).alias("end"))
;  .select(months_between(col("start"), col("end"))).show(1)

(q datedf
   ((functions/to_date (lit "2016-01-01")) ?start)
   ((functions/to_date (lit "2017-05-22")) ?end)
   [(functions/months_between ?start ?end)]
   (.show 1))

;;spark.range(5).withColumn("date", lit("2017-01-01"))
;  .select(to_date(col("date"))).show(1)

(q (.range (get-session) 5)
   ((lit "2017-01-01") ?date)
   [(functions/to_date ?date)]
   (.show 1))

;;dateDF.select(to_date(lit("2016-20-12")),to_date(lit("2017-12-11"))).show(1)

(q datedf
   [(functions/to_date (lit "2016-20-12"))                  ;;invalid date =>null
    (functions/to_date (lit "2017-12-11"))]
   (.show 1))

;;val dateFormat = "yyyy-dd-MM"
;val cleanDateDF = spark.range(1).select(
;    to_date(lit("2017-12-11"), dateFormat).alias("date"),
;    to_date(lit("2017-20-12"), dateFormat).alias("date2"))
;cleanDateDF.createOrReplaceTempView("dateTable2")

(def dateformat "yyyy-dd-MM")

(def datedf2 (q (.range (get-session) 1)
                  ((functions/to_date (lit "2017-12-11") dateformat) ?date)
                  ((functions/to_date (lit "2017-20-12") dateformat) ?date2) ;;now ok with date format arg
                  ))

(.show datedf2)

;;cleanDateDF.select(to_timestamp(col("date"), dateFormat)).show()

(q datedf2
   [(functions/to_timestamp ?date dateformat)]
   (.show))


;;cleanDateDF.filter(col("date2") > lit("2017-12-12")).show()

(q datedf2
   ((>_ ?date2 (lit "2017-12-12")))
   (.show))

;;cleanDateDF.filter(col("date1") > "'2017-12-12'").show()

(q datedf2
   ((>_ ?date (lit "2017-12-12")))
   (.show))

;;----------------------nulls------------------------------------------

;;if value in description!=null ,we keep it,else we take the value of CustomerId etc ..
;;returns 1 collumn with the first not nil value
;;df.select(coalesce(col("Description"), col("CustomerId"))).show()

(q df
   [(coalesce ?Description ?CustomerId)]
   (.show))

;;ifnull, nullIf, nvl, and nvl2   sql.functions
;;ifnull(null, 'return_value')
;;oles einai kapos etsi,epistrefoun to alo orisma ean null to proto

;;df.na.drop()         ;;drop a row if has a null value(default = "any")
;;==
;df.na.drop("any")

(q df
   (drop-nulls))
;;==
(q df
   (drop-nulls "any"))

;df.na.drop("all")

;;null or NaN
(q df
   (drop-nulls "all"))

;;;;df.na.drop("all", Seq("StockCode", "InvoiceNo"))
(q df
   (drop-nulls "all" (sa ?:StockCode ?:InvoiceNo)))

;;df.na.fill("All Null values become this string")

(q df
   (fill-rows "All Null values become this string"))

;;df.na.fill(5, Seq("StockCode", "InvoiceNo"))
(q df
   (fill-rows 5 (sa ?:StockCode ?:InvoiceNo)))


;;val fillColValues = Map("StockCode" -> 5, "Description" -> "No Value")
;df.na.fill(fillColValues)

(q df
   (fill-rows {"StockCode" 5
               "Description" "No Value"})
   (.show))

;;df.na.replace("Description", Map("" -> "UNKNOWN"))

(q df
   (replace-rows ?:Description {"" "UNKNOWN"}))

;;ordering of nulls
;;asc_nulls_first, desc_nulls_first,
;asc_nulls_last, or desc_nulls_last

;;-------------------complex types--------------------------------------

;;Complex types(3 types)
;;   structs
;;   arrays
;;   maps

;;1)
;;struct = like dataframe inside dataframe (the outer dataframe is a stuct with fields its columns)
;;         it can be freely nested,like struct that contains structs
;;         for example a column can be a struct,and in that struct one column can be a struct etc
;;to access the members i use .getField


;;df.selectExpr("(Description, InvoiceNo) as complex", "*")
;df.selectExpr("struct(Description, InvoiceNo) as complex", "*")
;=== in Scala
;import org.apache.spark.sql.functions.struct
;val complexDF = df.select(struct("Description", "InvoiceNo").alias("complex"))
;complexDF.createOrReplaceTempView("complexDF")

;;struct will also have a "header" so we can select by name
;;struct is contructed with members existing columns
(def complexDF
  (q df
     ((struct_ ?Description ?InvoiceNo) ?complex)))

(.show complexDF 5 false)

(q complexDF
   [(.getField ?complex ?:Description)]
   (.show 5 false))

(.printSchema complexDF)

;;2)
;;Arrays => value is Array

;;import org.apache.spark.sql.functions.split
;df.select(split(col("Description"), " ")).show(2)
;// in Scala
;df.select(split(col("Description"), " ").alias("array_col"))
;  .selectExpr("array_col[0]").show(2)

;;split value intro an array,and select the myarray[0]
;;here the array dont have a header,INDEX here works
(q df
   ((split ?Description " ") ?myarray)
   [(.getItem ?myarray 0)]
   (.show 2 false))


;;df.select(size(split(col("Description"), " "))).show(2) // shows 5 and 3
;;size of the array
(q df
   [(functions/size (split ?Description " "))]
   (.show 2))


;;df.select(array_contains(split(col("Description"), " "), "WHITE")).show(2)

(q df
   ((functions/array_contains (split ?Description " ") "WHITE"))
   (.show 2))

;;df.withColumn("splitted", split(col("Description"), " "))
;  .withColumn("exploded", explode(col("splitted")))
;  .select("Description", "InvoiceNo", "exploded").show(2)

;;explode => if n elements the array,we add n same rows,1 row/elemnt with value that element
(q df
   ((split ?Description " ") ?splitted)                     ;;array
   ((functions/explode ?splitted) ?exploded)                      ;;exploted => each element of array=row
   [?Description ?InvoiceNo ?exploded]
   (.show 2))


;;explode bazi tis times tou array stin column
;;diladi kathe stixio tou array einai simetexi se ena neo row
;;neo row = stixio tou array + duplicate oi times ton allon column
;;          tis palias row
;;px i row   [1 2] "takis" ->  1 takis
;;                             2 takis
;;.withColumn("exploded", explode(col("splitted")))



;;3)
;;Maps = 2 diaforetikon column values(key value) sto dataframe
;;       ginonte 1 key-value pair(map= 1 pair edo)
;;edo map den enoo map me pola keys,alla 2 values tou row,ginonte
;;ena pair(to proto einai to key kai to deutero to value)
;;ara i column pou paragete einai mia column me pairs(auto to pair=map)

;;to store a hash-map in a column {},one solution is to store 2 arrays in 2 columns
;;because this map is a pair not a hash-map


;;df.select(map(col("Description"), col("InvoiceNo")).alias("complex_map")).show(2)

(q df
   ((pair ?Description ?InvoiceNo) ?pair)
   [?pair]
   (.show 5 false))


;;df.select(map(col("Description"), col("InvoiceNo")).alias("complex_map"))
;  .selectExpr("explode(complex_map)").show(2)

(q df
   ((pair ?Description ?InvoiceNo) ?pair)
   [(functions/explode ?pair)]
   (.show 5 false))

;;df.select(map(col("Description"), col("InvoiceNo")).alias("complex_map"))
;  .selectExpr("complex_map['WHITE METAL LANTERN']").show(2)

;;getItem based on key,ean kapio pair exi alo key,epistefi nul
(q df
   ((pair ?Description ?InvoiceNo) ?pair)
   ((.getItem ?pair "WHITE METAL LANTERN") ?wpair)
   [?wpair]
   (.show 5 false))


;;458 scala code some more about JSON,and UDF

;;---------------------------------json---------------------------------

(-> (.range (get-session) 1)
    (.selectExpr (into-array ["'{\"myJSONKey\" : {\"myJSONValue\" : [1, 2, 3]}' as jsonString"]))
    (.show false))


;;Completed(1 in regular,and page 458 JSON,and UDF example)

