(ns sparkdefinitive.ch3IntroTools
  (:require louna.datasets.schema
            [louna.datasets.sql :as sql]
            sparkdefinitive.init-settings
            [louna.state.settings :as settings]
            clj-bean.core
            [louna.datasets.grouped-datasets :as g])
  (:use louna.datasets.column
        louna.datasets.sql-functions
        louna.q.run
        louna.datasets.datasets)
  (:import [org.apache.spark.sql functions Encoders]))


(sparkdefinitive.init-settings/init)

;;---------------------------Datasets examples------------------------------------------------


;;import spark.implicits._
;case class Flight(DEST_COUNTRY_NAME: String,
;                  ORIGIN_COUNTRY_NAME: String,
;                  count: BigInt)
;val flightsDF = spark.read
;  .parquet("/data/flight-data/parquet/2010-summary.parquet/")
;val flights = flightsDF.as[Flight]
;
;
;// COMMAND ----------
;
;// in Scala
;flights
;  .filter(flight_row => flight_row.ORIGIN_COUNTRY_NAME != "Canada")
;  .map(flight_row => flight_row)
;  .take(5)
;
;flights
;  .take(5)
;  .filter(flight_row => flight_row.ORIGIN_COUNTRY_NAME != "Canada")
;  .map(fr => Flight(fr.DEST_COUNTRY_NAME, fr.ORIGIN_COUNTRY_NAME, fr.count + 5))

(clj-bean.core/defbean sparkdefinitive.ch3IntroTools.Flight
                       [[String DEST_COUNTRY_NAME]
                        [String ORIGIN_COUNTRY_NAME]
                        [long count]])

(defn printFlight [flight]
  (println [(.getORIGIN_COUNTRY_NAME flight)
            (.getDEST_COUNTRY_NAME flight)
            (.getCount flight)]))

(def flightsDF
  (-> (get-session)
      .read
      (.parquet (str (settings/get-base-path) "/data/flight-data/parquet/2010-summary.parquet/"))))

(def flightsDS (.as flightsDF (Encoders/bean sparkdefinitive.ch3IntroTools.Flight)))

(-> flightsDS
    (filter_ (fn [flight] (not= (.getORIGIN_COUNTRY_NAME flight) "Canada")))
    .show)

;;this works also its the same with SQL
(q flightsDS
   ((=_ ?ORIGIN_COUNTRY_NAME "Canada"))
   .show)

;;;  .filter(flight_row => flight_row.ORIGIN_COUNTRY_NAME != "Canada")
;;  .map(fr => Flight(fr.DEST_COUNTRY_NAME, fr.ORIGIN_COUNTRY_NAME, fr.count + 5))

(def localSample (.take flightsDS 5))
(println "sample")
(dorun (map printFlight localSample))


(def notFromIreland
  (filter (fn [flight] (not= (.getORIGIN_COUNTRY_NAME flight) "Ireland")) localSample))
(println "notFromIreland")
(dorun (map printFlight notFromIreland))

(def notFromIrelandNewObjects
  (map (fn [flight]
         (sparkdefinitive.ch3IntroTools.Flight. (.getDEST_COUNTRY_NAME flight)
                                          (.getORIGIN_COUNTRY_NAME flight)
                                          (+ (.getCount flight) 5)))
       notFromIreland))
(println "notFromIrelandNewObjects")
(dorun (map printFlight notFromIrelandNewObjects))

;;---------------------------------SQL examples----------------------------------

;;val staticDataFrame = spark.read.format("csv")
;  .option("header", "true")
;  .option("inferSchema", "true")
;  .load("/data/retail-data/by-day/*.csv")
;
;staticDataFrame.createOrReplaceTempView("retail_data")
;val staticSchema = staticDataFrame.schema

(def staticDataFrame (-> (.read (get-session))
                         (.format "csv")
                         (.option "header" "true")
                         (.option "inferSchema" "true")
                         (.load (str (settings/get-base-path)
                                     "/data/retail-data/by-day/*.csv"))))

(def staticSchema (.schema staticDataFrame))

;;staticDataFrame
;.selectExpr(
;"CustomerId",
;"(UnitPrice * Quantity) as total_cost",
;"InvoiceDate")
;.groupBy(
;col("CustomerId"), window(col("InvoiceDate"), "1 day"))
;.sum("total_cost")
;.show(5)

;;CustomerId|window|sum(total_cost)
;;for every customer total_cost per day
;;if he didnt buy that day customerID null
(q staticDataFrame
   [?CustomerId ((*_ ?UnitPrice ?Quantity) ?total_cost) ?InvoiceDate]
   (groupBy ?CustomerId (functions/window ?InvoiceDate "1 day"))
   (g/sum ?total_cost)
   (.show 5))

;;---------------------------stream example------------------------------

;;val streamingDataFrame = spark.readStream
;.schema(staticSchema)
;.option("maxFilesPerTrigger", 1)
;.format("csv")
;.option("header", "true")
;.load("/data/retail-data/by-day/*.csv")

;;val purchaseByCustomerPerHour = streamingDataFrame
;.selectExpr(
;"CustomerId",
;"(UnitPrice * Quantity) as total_cost",
;"InvoiceDate")
;.groupBy(
;$"CustomerId", window($"InvoiceDate", "1 day"))
;.sum("total_cost")


;;define the source
(def streamingDataFrame (-> (.readStream (get-session))
                            (.schema staticSchema)
                            (.format "csv")
                            (.option "maxFilesPerTrigger" 1)
                            (.option "header" "true")
                            (.load (str (settings/get-base-path)
                                        "/data/retail-data/by-day/*.csv"))))

;;define the query on the source
(def purchaseByCustomerPerHour
  (q streamingDataFrame
     [?CustomerId ((*_ ?UnitPrice ?Quantity) ?total_cost) ?InvoiceDate]
     (groupBy ?CustomerId (functions/window ?InvoiceDate "1 day"))
     (g/sum ?total_cost)))

;;purchaseByCustomerPerHour.writeStream
;.format("memory") // memory = store in-memory table
;.queryName("customer_purchases") // the name of the in-memory table
;.outputMode("complete") // complete = all the counts should be in the table
;.start()

;;start the stream+setting options
(-> purchaseByCustomerPerHour
    .writeStream
    (.format "memory")
    (.queryName "customer_purchases")
    (.outputMode "complete")
    (.start))

(def resultTable (.table (get-session) "customer_purchases"))

(dotimes [- 5]
  (q resultTable .show)
  (Thread/sleep 1000))

;;ERROR MicroBatchExecution if SparkContext closes before finish the stream

;;----------------TODO (skipped machine learning page 46)-------------------------------

;;-----------------rdd-------------------------------------------------------------------

;;spark.sparkContext.parallelize(Seq(1, 2, 3)).toDF()

(.show (sql/seq->df [1 2 3] [["id" :long]]))

;;Complete (except machine learning)









