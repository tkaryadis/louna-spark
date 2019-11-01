(ns apps.temp
  (:require [louna.state.settings :as state]
            [louna.datasets.schema :as t]
            [louna.datasets.sql :as sql]
            louna.library.scalaInterop
            clojure.set
            clojure.string)
  (:use louna.datasets.column
        louna.datasets.sql-functions
        louna.q.run
        louna.library.util
        louna.rdds.api)
  (:import (org.apache.spark.sql.types DataTypes)))


(louna.state.settings/set-local-session)
(louna.state.settings/set-log-level "ERROR")
(louna.state.settings/set-base-path "/home/white/IdeaProjects/louna/")

#_(def person (sql/seq->df [[0 "Bill Chambers" 0 (long-array [100])]
                          [1 "Matei Zaharia" 1 (long-array [500 250 100])]
                          [2 "Michael Armbrust" 1 (long-array [250 100])]]

                         [["id" :long]
                          ["name"]
                          ["graduate_program" :long]
                          ["spark_status" (t/array-type DataTypes/LongType)]]))

#_(def df (-> (.read (get-session))
            (.format "csv")
            (.option "header" "true")
            (.option "inferSchema" "true")
            (.load (str (louna.state.settings/get-base-path)
                        "/data/retail-data/by-day/2010-12-01.csv"))))

#_(.printSchema df)


#_(q  df
    ((.and (contains_? ?StockCode "DOT")
           (.or (>_ ?UnitPrice 600)
                (includes? ?Description "POSTAGE"))) ?expensive)
    ((!?expensive))
    [?expensive ?unitPrice]
    (.show 5))


#_(q (df ?Quantity:qt ?UnitPrice:price)
   ((+_ (pow (*_ ?qt ?price) 2.0) 5) ?realQt)
   (.show 2))

#_(def df1 (-> (.read (get-session))
            (.format "csv")
            (.option "header" "true")
            (.option "inferSchema" "true")
            (.load (str (louna.state.settings/get-base-path)
                        "/data/retail-data/all/*.csv"))
            (.coalesce 5)))

#_(q df1
   (groupBy ?InvoiceNo)
   (agg ((count_ ?Quantity) ?quan))
   (.show))
