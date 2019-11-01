(ns louna.datasets.grouped-datasets
  (:import (org.apache.spark.sql Column)))


(defn sum [grouped-dataset & cols]
  (.sum grouped-dataset (into-array String (map str cols))))

(defn avg [grouped-dataset & cols]
  (.avg grouped-dataset (into-array String (map str cols))))