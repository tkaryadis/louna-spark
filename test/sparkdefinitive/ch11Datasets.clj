(ns sparkdefinitive.ch11Datasets
  (:require louna.datasets.schema
            sparkdefinitive.init-settings
            [louna.state.settings :as settings]
            clj-bean.core)
  (:use louna.datasets.column
        louna.datasets.sql-functions
        louna.q.run
        louna.datasets.datasets
        louna.datasets.udf)
  (:import [org.apache.spark.sql functions Row SparkSession Column Encoders]
           (scala Tuple2)))

(sparkdefinitive.init-settings/init)

;;---------------------------From chapter3(same code)------------------------------------------------

;;import spark.implicits._
;case class Flight(DEST_COUNTRY_NAME: String,
;                  ORIGIN_COUNTRY_NAME: String,
;                  count: BigInt)
;val flightsDF = spark.read
;  .parquet("/data/flight-data/parquet/2010-summary.parquet/")
;val flights = flightsDF.as[Flight]

;;need Java bean ,so use library to easily create one
(clj-bean.core/defbean sparkdefinitive.ch11Datasets.Flight
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

(def flightsDS (.as flightsDF (Encoders/bean sparkdefinitive.ch11Datasets.Flight)))

;;-------------------------------ch11--------------------------------------------------------------------

;;def originIsDestination(flight_row: Flight): Boolean = {
;return flight_row.ORIGIN_COUNTRY_NAME == flight_row.DEST_COUNTRY_NAME
;}

;;flights.filter(flight_row => originIsDestination(flight_row)).first()

;;
(println "Number of rows : " (.count flightsDS))

;;generic function (normal clojure function)
;;q here dont do anything so -> would be ok also

(def flightOsameD
  (q flightsDS
     (filter_ (fn [flight] (= (.getORIGIN_COUNTRY_NAME flight) (.getDEST_COUNTRY_NAME flight))))))

;;sql like
;;result remains a dataset (not converts to dataframe)
(def flightOsameD2
  (q flightsDS
     ((=_ ?ORIGIN_COUNTRY_NAME ?DEST_COUNTRY_NAME))))

(.show flightOsameD)
(.show flightOsameD2)

;;local collection , that i can local do whatever like clojure map etc
(def localSample (.take flightOsameD 1))
(def localSample2 (.take flightOsameD2 1))

(dorun (map printFlight localSample))
(dorun (map printFlight localSample2))

;;collect and reduce locally with clojure
(def flightONotsameD
  (q flightsDS
     (filter_ (fn [flight] (not= (.getORIGIN_COUNTRY_NAME flight) (.getDEST_COUNTRY_NAME flight))))
     .collect))

(dorun (map printFlight flightONotsameD))

(def totalNFlights (reduce (fn [sum flight]
                             (+ sum (.getCount flight)))
                           0
                           flightONotsameD))

(prn totalNFlights)

;;val destinations = flights.map(f => f.DEST_COUNTRY_NAME)

;;collection of strings
;;I have to specify encoder for the return type(here String)
(def destinations
  (q flightsDS
     (map_ (fn [flight] (.getDEST_COUNTRY_NAME flight)) (Encoders/STRING))
     .collect))

(dorun (map println (take 10 destinations)))

;;--------------------------Joins----------------------------------------------------

;;We can do the sql-joins like before,but result -----> DATAFRAME

;;case class FlightMetadata(count: BigInt, randomData: BigInt)
;val flightsMeta = spark.range(500).map(x => (x, scala.util.Random.nextLong))
;.withColumnRenamed("_1", "count").withColumnRenamed("_2", "randomData")
;.as[FlightMetadata]

;val flights2 = flights
;.joinWith(flightsMeta, flights.col("count") === flightsMeta.col("count"))

;;flights2.selectExpr("_1.DEST_COUNTRY_NAME")
;;flights2.take(2)

(clj-bean.core/defbean sparkdefinitive.ch11Datasets.FlightMetadata
                       [[long count]
                        [long randomData]])

;;construct a dataset with 2 columns "count"(0-500) and "randomData"(a random long)

(def flightsMetaDF0
  (q (get-session)
     (.range 500)
     (map_ (fn [n] (Tuple2. n (long (rand n)))) (Encoders/tuple (Encoders/LONG) (Encoders/LONG)))
     (rename {"_1" ?count "_2" ?randomData})))
(.show flightsMetaDF0)

;;or with udf and similar rand-int clojure function (no use of Tuple to make the 2 columns and Encoders etc...)

(defudf rLong (fn [limit] (long (rand-int limit))) 1 :long)
(def flightsMetaDF1
  (q (get-session)
     (.range 500)
     ((rLong (functions/lit 10000)) ?randomData)
     (rename {"id" ?count})))
(.show flightsMetaDF1)

;;or something like this,but rand give double 0-1
(def flightsMetaDF2
  (q (get-session)
     (.range 500)
     ((functions/rand) ?randomData)
     (rename {"id" ?count})))
(.show flightsMetaDF2)

(def flightsMetaDS (.as flightsMetaDF0 (Encoders/bean sparkdefinitive.ch11Datasets.FlightMetadata)))

;;val flights2 = flights
;.joinWith(flightsMeta, flights.col("count") === flightsMeta.col("count"))

(.show flightsDS)
(.show flightsMetaDS)

;;result=1 dataset with 2 columns _1 and _2
;;_1 column have as element a vector(the row of flightsDS)
;;_2 column =vector [join_value metadata]
;;its like we added to the join_value some metadata(if join value is the id,its like we have metadata for the id)
(def flights2
  (q flightsDS
     (.joinWith flightsMetaDS (=_ (.col flightsDS "count") (.col flightsMetaDS "count")))))

(.show flights2 40 false)



;;-----------------------------------aggregations------------------------------------------------

;;sql style aggregations works but they return dataframes
;;groupByKey produces dataset(after the count here),but JVM types + function so slower
;;groupByKey produces a KeyValueGroupedDataset
;;KeyValueGroupedDataset has methods like (flatMapGroups/mapValues/reduceGroups etc)

;;flights.groupByKey(x => x.DEST_COUNTRY_NAME).count()
(-> flightsDS
    (groupByKey (fn [flight] (.getDEST_COUNTRY_NAME flight)) (Encoders/STRING))
    .count
    (.show 40))

;;Complete except pages 211-212 (flatMapGroups/mapValues/reduceGroups examples)

