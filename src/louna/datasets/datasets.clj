(ns louna.datasets.datasets
  (:import (org.apache.spark.api.java.function FilterFunction MapFunction)))

(defrecord Filterf [f]
  FilterFunction
  (call [this x]
    (f x)))

(defrecord Mapf [f]
  MapFunction
  (call [this x]
    (f x)))

(defn filter_ [ds f]
  (.filter ds (Filterf. f)))

(defn map_ [ds f encoder]
  (.map ds (Mapf. f) encoder))

(defn groupByKey [ds f encoder]
  (.groupByKey ds (Mapf. f) encoder))