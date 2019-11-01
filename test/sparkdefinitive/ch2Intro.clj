(ns sparkdefinitive.ch2Intro
  (:require [louna.datasets.sql :as sql]
            sparkdefinitive.init-settings
            [louna.state.settings :as settings]
            [louna.datasets.grouped-datasets :as g])
  (:use louna.datasets.column
        louna.datasets.sql-functions
        louna.q.run))

(sparkdefinitive.init-settings/init)

;;val myRange = spark.range(1000).toDF("number")
(def myRange (sql/seq->df (range 10000) [["number" :long]]))

(.show myRange)
(prn (.count myRange))


;;val flightData2015 = spark
;;.read
;;.option("inferSchema", "true")
;;.option("header", "true")
;;.csv("/data/flight-data/csv/2015-summary.csv")


;;Header(3)=DEST_COUNTRY_NAME   ORIGIN_COUNTRY_NAME  count

(def flightData2015
  (-> (.read (get-session))
      (.option "header" "true")
      (.option "inferSchema" "true")
      (.csv (str (settings/get-base-path)
                 "/data/flight-data/csv/2015-summary.csv"))))

;;flightData2015.take(3)

(sql/print-rows (.take flightData2015 3))

;;flightData2015.sort("count").explain()

(q flightData2015
   (order-by ?count)
   (.explain))

;;spark.conf.set("spark.sql.shuffle.partitions", "5")

(-> (get-session)
    (.conf)
    (.set "spark.sql.shuffle.partitions" "5"))

;;flightData2015.sort("count").take(2)

(q flightData2015
   (order-by ?count)
   (.take 2)
   (sql/print-rows))




;;flightData2015.createOrReplaceTempView("flight_data_2015")

;val sqlWay = spark.sql ("" "
;SELECT DEST_COUNTRY_NAME, count(1)
;FROM flight_data_2015
;GROUP BY DEST_COUNTRY_NAME " "")

;or in scala

;;val dataFrameWay = flightData2015
;;.groupBy('DEST_COUNTRY_NAME)
;;.count()

(def lounaWay1 (q flightData2015
                 (groupBy ?DEST_COUNTRY_NAME)
                 (.count)))

(.show lounaWay1)


;;or with project to rename and keep only dest

(def lounaWay2 (q (flightData2015 ?dest)
                 (groupBy ?dest)
                 (.count)))

(.show lounaWay2)



;;sqlWay.explain
(.explain lounaWay1)
(.explain lounaWay2)

;;spark.sql("SELECT max(count) from flight_data_2015").take(1)
;;flightData2015.select(max("count")).take(1)

(q flightData2015
   [(max_ ?count)]
   (.take 1)
   (sql/print-rows))

;val maxSql = spark.sql ("" "
;SELECT DEST_COUNTRY_NAME, sum(count) as destination_total
;FROM flight_data_2015
;GROUP BY DEST_COUNTRY_NAME
;ORDER BY sum(count) DESC
;LIMIT 5 " "")

;flightData2015
;.groupBy ("DEST_COUNTRY_NAME")
;.sum ("count")
;.withColumnRenamed ("sum(count)", "destination_total")
;.sort (desc ("destination_total"))
;.limit (5)
;.show ()


(q (flightData2015 ?dest - ?count)
   (groupBy ?dest)
   (g/sum ?count)
   (rename "sum(count)" ?destTotal)
   (order-by (desc ?destTotal))
   (.limit 5)
   (.show))

;;same as  (?:sum(count) not valid clojure symbol thats why "?:sum(count)")

(q (flightData2015 ?dest - ?count)
   (groupBy ?dest)
   (g/sum ?count)
   (rename "?:sum(count)" ?destTotal)
   (order-by (desc ?destTotal))
   (.limit 5)
   (.show))

;;same as

(q (flightData2015 ?dest - ?count)
   (groupBy ?dest)
   (g/sum ?count)
   (rename {"sum(count)" ?destTotal})                       ;;if many renames
   (order-by (desc ?destTotal))
   (.limit 5)
   (.explain))

;;Complete