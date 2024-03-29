(ns sparkdefinitive.ch21Streaming
  (:require sparkdefinitive.init-settings
            [louna.state.settings :as settings]
            louna.library.scalaInterop
            clojure.set
            clojure.string
            [louna.datasets.sql :as sql]
            [louna.datasets.grouped-datasets :as g])
  (:use louna.datasets.column
        louna.datasets.sql-functions
        louna.q.run
        louna.library.util
        louna.rdds.api)
  (:import (org.apache.spark.sql.streaming Trigger)))

(sparkdefinitive.init-settings/init)

;;Make a DF from data like in the past

;;val static = spark.read.json("/data/activity-data/")
;val dataSchema = static.schema

(def staticDF (-> (get-session)
                  (.read)
                  (.json (str (settings/get-base-path) "/data/activity-data/"))))

(def dataSchema (.schema staticDF))

;(.show staticDF 5)

;(prn (.count staticDF))

;;Streaming dont do auto schema inference,so to do it i have to say it by
;;  spark.sql.streaming.schemaInference to true

;;Make a Stream from data,using the schema we already know they have

;;Stream operations
;;1)specify source+options
;;2)specify query
;;3)specify sink+options
;;4)start streaming

;;To see the temp results,we query the stream,table name=query name

;;val streaming = spark.readStream.schema(dataSchema)
;.option("maxFilesPerTrigger", 1).json("/data/activity-data")

;;we could use spark.sql.streaming.schemaInference to true also
(def readStream (-> (get-session)
                    (.readStream)
                    (.schema dataSchema)
                    (.option "maxFilesPerTrigger" 1)
                    (.json (str (settings/get-base-path) "/data/activity-data/"))))


;;trigger based on end of file,not time
;;(.option "maxFilesPerTrigger" 1)

;;Like in batch queries tranformations are lazy
;;val activityCounts = streaming.groupBy("gt").count()

;;define the query
(def queryStream (q readStream
                    (groupBy ?gt)
                    (.count)))

;;val activityQuery = activityCounts.writeStream.queryName("activity_counts")
;.format("memory").outputMode("complete")
;.start()

;;start streaming+setting options
;;driver program must not terminated before stream end

(def writeSteam (-> queryStream
                    (.writeStream)
                    (.queryName "activity_counts")        ;;random name
                    (.format "memory")                    ;;sink=memory
                    (.outputMode "complete")              ;;ovewrite old results
                    (.start)))

;;See a list of background streams
;;name(the one we given) and a UUID that spark gave
;;spark.streams.active
;(prn (-> (get-session) .streams .active))

;;activityQuery.awaitTermination()

;;block and wait for the stream
;;(.awaitTermination writeSteam)

;;get the table that the results are stored (name of the query we used)
(def resultTable (.table (get-session) "activity_counts"))

(dotimes [- 100]
  (q resultTable .show)
  (Thread/sleep 2000))



;;-------------------------bind+filter on stream------------------------


;;val simpleTransform = streaming.withColumn("stairs", expr("gt like '%stairs%'"))
;.where("stairs")
;.where("gt is not null")
;.select("gt", "model", "arrival_time", "creation_time")
;.writeStream
;.queryName("simple_transform")
;.format("memory")
;.outputMode("append")
;.start()


(q readStream
   ((includes? ?gt "stairs") ?stairs)
   ((true_? ?stairs) (not-nil_? ?gt))
   [?gt ?model ?arrival_time ?creation_time]
   (.writeStream)
   (.queryName "simple_transform")
   (.format "memory")
   (.outputMode "append")
   (.start))

(def resultTable2 (.table (get-session) "simple_transform"))

;;-----------------------------aggr-----------------------------------------------

;;val deviceModelStats = streaming.cube("gt", "model").avg()
;.drop("avg(Arrival_time)")
;.drop("avg(Creation_Time)")
;.drop("avg(Index)")
;.writeStream.queryName("device_counts").format("memory").outputMode("complete")
;.start()

(q readStream
   (sql/cube ?gt ?model)
   (g/avg)
   (drop-col "avg(Arrival_time)" "avg(Creation_Time)" "avg(Index)")
   (.writeStream)
   (.queryName "device_counts")
   (.format "memory")
   (.outputMode "complete")
   (.start))

(def resultTable3 (.table (get-session) "device_counts"))

#_(dotimes [- 5]
  (q resultTable3 .show)
  (Thread/sleep 2000))

;;------------------------------joins------------------------------------

;;here join only static+stream (spark 2.3+ will add join streams together)

;;val historicalAgg = static.groupBy("gt", "model").avg()
;val deviceModelStats = streaming.drop("Arrival_Time", "Creation_Time", "Index")
;.cube("gt", "model").avg()
;.join(historicalAgg, Seq("gt", "model"))
;.writeStream.queryName("device_counts").format("memory").outputMode("complete")
;.start()

(def historicalAgg (q staticDF
                      (groupBy ?gt ?model)
                      (g/avg)))

(.show historicalAgg)

(q staticDF
   (drop-col ?Arrival_Time ?Creation_Time ?Index)
   (sql/cube ?gt ?model)
   (g/avg)
   .show)

#_(q (historicalAgg ?gt ?model ?2:arrival "?avg(x):avg-x" "?avg(y):avg-y" "?avg(z):avg-z")
   .show)

;;join of streams not yet supported,here one was staticDF and other stream
(q readStream
   (drop-col ?Arrival_Time ?Creation_Time ?Index)
   (sql/cube ?gt ?model)
   (g/avg)
   (historicalAgg ?gt ?model "?avg(x):avg-x" "?avg(y):avg-y" "?avg(z):avg-z")
   (.writeStream)
   (.queryName "device_counts2")
   (.format "memory")
   (.outputMode "complete")
   .start)

(def resultTable4 (.table (get-session) "device_counts2"))

#_(dotimes [- 5]
    (q resultTable4 .show)
    (Thread/sleep 2000))

;;import org.apache.spark.sql.streaming.Trigger
;activityCounts.writeStream.trigger(Trigger.ProcessingTime("100 seconds"))
;.format("console").outputMode("complete").start()

;;default(not .trigger)=> many batches(when finish processing go to next)
(q queryStream
   (.writeStream)
   ;(.trigger (Trigger/ProcessingTime "10 seconds"))
   (.format "console")
   (.outputMode "complete")
   ;.start
   )

;;batch every 10 seconds (every 10 seconds write to sink,here the console)
(q queryStream
   (.writeStream)
   (.trigger (Trigger/ProcessingTime "10 seconds"))
   (.format "console")
   (.outputMode "complete")
   ;.start
   )


;;activityCounts.writeStream.trigger(Trigger.Once())
;.format("console").outputMode("complete").start()

(q queryStream
   (.writeStream)
   (.trigger (Trigger/Once))
   (.format "console")
   (.outputMode "complete")
   .start)

(Thread/sleep 1000000)

;;errors if driver exit before batch
;;completed,skipped kafka source/sink,pages 354-356
;;datasets streaming example in the ch11Datasets.clj