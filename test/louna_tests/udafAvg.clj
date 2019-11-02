(ns louna-tests.udafAvg
  (:require louna.datasets.schema)
  (:import (org.apache.spark.sql.types DataTypes)
           (org.apache.spark.sql Row)))

(gen-class
  :name louna-tests.udafAvg.MyAvg
  :extends org.apache.spark.sql.expressions.UserDefinedAggregateFunction
  :state state
  :init init)


(defn -init []
  [[] {:inputSchema (louna.datasets.schema/build-schema [["inputColumn" :long]])
       :bufferSchema (louna.datasets.schema/build-schema [["sum" :long] ["count" :long]])}])


(defn -inputSchema [this]
  (get (.state this) :inputSchema))

(defn -bufferSchema [this]
  (get (.state this) :bufferSchema))

(defn -dataType [this]
  DataTypes/DoubleType)

(defn -deterministic [this]
  true)

(defn -initialize [this buffer]
  (do (.update buffer 0 0)
      (.update buffer 1 0)))


(defn -update [this buffer ^Row input]
  (if (not (.isNullAt input 0))
    (let [updatedSum (+ (.getLong buffer 0)
                        (.getLong input 0))
          updatedCount (+ (.getLong buffer 0)
                          1)]
      (do (.update buffer 0 updatedSum)
          (.update buffer 1 updatedCount)))))


(defn -merge [this buffer1 buffer2]
  (let [mergedSum (+ (.getLong buffer1 0)
                     (.getLong buffer2 0))
        mergedCount (+ (.getLong buffer1 1)
                       (.getLong buffer2 1))]
    (do (.update buffer1 0 mergedSum)
        (.update buffer1 1 mergedCount))))

(defn -evaluate [this ^Row buffer]
  (double (/ (.getLong buffer 0) (.getLong buffer 1))))