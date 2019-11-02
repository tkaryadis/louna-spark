(ns apps.temp
  (:require [louna.state.settings :as state]
            [louna.datasets.schema :as t]
            [louna.datasets.sql :as sql]
            louna.library.scalaInterop
            clojure.set
            clojure.string
            [louna.datasets.grouped-datasets :as g]
            [louna.state.settings :as settings])
  (:use louna.datasets.column
        louna.datasets.sql-functions
        louna.q.run
        louna.library.util
        louna.rdds.api)
  (:import (org.apache.spark.sql.types DataTypes)))


(louna.state.settings/set-local-session)
(louna.state.settings/set-log-level "ERROR")
(louna.state.settings/set-base-path "/home/white/IdeaProjects/louna-spark/")

(def staticDF (-> (get-session)
                  (.read)
                  (.json (str (settings/get-base-path) "/data/activity-data/"))))

(def dataSchema (.schema staticDF))

(def readStream (-> (get-session)
                    (.readStream)
                    (.schema dataSchema)
                    (.option "maxFilesPerTrigger" 1)
                    (.json (str (settings/get-base-path) "/data/activity-data/"))))

(def historicalAgg (q staticDF (groupBy ?gt ?model) (g/avg)))

(.show historicalAgg)

(q (historicalAgg ?gt ?model ?3:skata "?avg(x):avg-x" "?avg(y):avg-y" "?avg(z):avg-z")
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