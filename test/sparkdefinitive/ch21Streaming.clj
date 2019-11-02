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
        louna.rdds.api))

(sparkdefinitive.init-settings/init)

;;Make a DF from data like in the past

;;val static = spark.read.json("/data/activity-data/")
;val dataSchema = static.schema

(def staticDF (-> (get-session)
                  (.read)
                  (.json (str (settings/get-base-path) "/data/activity-data/"))))

(def dataSchema (.schema staticDF))

(prn dataSchema)

(.show staticDF 5)

(prn (.count staticDF))

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

(def readStream (-> (get-session)
                    (.readStream)
                    (.schema dataSchema)
                    (.option "maxFilesPerTrigger" 1)
                    (.json (str (settings/get-base-path) "/data/activity-data/"))))


;;(.option "maxFilesPerTrigger" 1) means that every 1 file => trigger

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

(def writeSteam (-> queryStream
                    (.writeStream)
                    (.queryName "activity_counts")        ;;random name
                    (.format "memory")                    ;;sink=memory
                    (.outputMode "complete")              ;;ovewrite old results
                    (.start)
                    ))

;;streams are supposed to run forever,so they run in background
;;this will start the stream in background,and here will not run
;;at all if driver program will exit after this

;;See a list of background streams
;;name(the one we given) and a UUID that spark gave
;;spark.streams.active
;(prn (-> (get-session) .streams .active))


;;activityQuery.awaitTermination()

;;block and wait for the stream
;;(.awaitTermination writeSteam)

;;the stream is now running(suppose not await above)
;;to not terminate the drive program i can write a loop with sleep
;;also i can query the data while the stream is running

;;get the table that the results are stored (name of the query we used)
(def resultTable (.table (get-session) "activity_counts"))

(dotimes [- 5]
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

(dotimes [- 5]
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

(def historicalAgg (q staticDF (groupBy ?gt ?model) (g/avg)))

(.show historicalAgg)

(q staticDF
   (drop-col ?Arrival_Time ?Creation_Time ?Index)
   (sql/cube ?gt ?model)
   (g/avg)
   .show)

(q (historicalAgg ?gt ?model ?3:acolumn "?avg(x):avg-x" "?avg(y):avg-y" "?avg(z):avg-z")
   .show)

(q readStream
   (drop-col ?Arrival_Time ?Creation_Time ?Index)
   (sql/cube ?gt ?model)
   (g/avg)
   (historicalAgg ?gt ?model "?avg(x):avg-x" "?avg(y):avg-y" "?avg(z):avg-z")
   (.writeStream)
   (.queryName "device_counts2")
   (.format "memory")
   (.outputMode "complete")
   (.start))

(System/exit 0)

(q staticDF
   (groupBy ?gt ?model)
   (g/avg)
   [?gt ?model "?avg(x)" "avg(y)" "avg(z)"]
   (rename "avg(x)" "avg-x")
   (rename "avg(y)" "avg-y")
   (rename "avg(z)" "avg-z")
   .show)

(q readStream
   (drop-col ?Arrival_Time ?Creation_Time ?Index)
   (sql/cube ?gt ?model)
   (g/avg)
   (historicalAgg ?gt ?model "?avg(x):avg-x" "?avg(y):avg-y" "?avg(z):avg-z")
   (.writeStream)
   (.queryName "device_counts2")
   (.format "memory")
   (.outputMode "complete")
   (.start))

(def resultTable4 (.table (get-session) "device_counts2"))

(dotimes [- 5]
    (q resultTable4 .show)
    (Thread/sleep 2000))

;;errors if driver exit before batch