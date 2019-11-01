(defproject louna-spark "0.1"
  :description "Clojure library for for Apache Spark,querying and data processing"
  :url "https://github.com/tkaryadis/louna-spark"
  :license {:name "Apache License"
            :url  "http://www.apache.org/licenses/LICENSE-2.0"}
  :dependencies [[org.clojure/clojure "1.10.0"]
                 [com.wjoel/clj-bean "0.2.1"]
                 [org.apache.spark/spark-core_2.11 "2.4.0"]   
                 [org.apache.spark/spark-sql_2.11 "2.4.0"]
                 [org.postgresql/postgresql "42.2.5"]]
  :aot [
        louna.datasets.schema
        louna.datasets.sql
        louna.datasets.udf
        louna.datasets.column
        louna.datasets.sql-functions
        louna.datasets.grouped-datasets

        louna.library.scalaInterop
        louna.library.util

        louna.q.run
        louna.q.run-util
        louna.q.transform

        louna.rdds.api
        louna.state.settings

        ;sparkdefinitive.ch2Intro
        ;sparkdefinitive.ch3IntroTools
        ;sparkdefinitive.ch4and5
        ;sparkdefinitive.ch6DataTypes
        ;sparkdefinitive.ch7Aggregations
        ;sparkdefinitive.ch8Joins
        ;sparkdefinitive.ch11Datasets
        ;sparkdefinitive.ch12RDD1
        ;sparkdefinitive.ch13RDD2
        ;sparkdefinitive.ch21Streaming

        ;sparkdefinitive.variables-constants-nested
        ;sparkdefinitive.udf
        ;sparkdefinitive.udafAvg
        ;sparkdefinitive.udaf

        ;apps.wordcount
        ;apps.temp

        ]

  :plugins [[lein-codox "0.10.7"]]
  :javac-options ["-source" "1.8" "-target" "1.8"]
  :jvm-opts ^:replace ["-server" "-Xmx2g"]
  :global-vars {*warn-on-reflection* false})
