(ns louna.datasets.sql-functions                        ;;!!!!!!!!!!
  (:require [louna.datasets.sql :as sql])
  (:import [org.apache.spark.sql functions Column]))

;;(class object)=> which one to call
(defmulti col class)
(defmethod col org.apache.spark.sql.Column [x] x)
(defmethod col java.lang.String [x] (functions/col x))
(defmethod col clojure.lang.Symbol [x] (functions/col (str x)))

(defn struct_ [& col-names]
  (functions/struct (sql/ca col-names)))

(defn pair [& col-names]
  (functions/map (sql/ca col-names)))

(defn lit [value]
  (functions/lit value))

(defn round
  ([col1]
   (functions/round col1))
  ([col1 scale]
   (functions/round col1 scale)))

(defn lower [col1]
  (functions/lower col1))

(defn upper [col1]
  (functions/upper col1))

(defn regexp_replace [col1 pattern replacement]
  (functions/regexp_replace col1
                            pattern
                            replacement))
(defn pow [col1 col2]
  (functions/pow col1 col2))

(defn coalesce [& cols]
  (functions/coalesce (into-array Column cols)))

;;split-to-array
(defn split [col1 pattern]
  (functions/split col1 pattern))

;;-----------------------------aggr-----------------------------------
(defn count_ [col1]
  (functions/count col1))

(defn countDistinct [col1 & cols]
  (functions/countDistinct (str col1)  (into-array String (map str cols))))

(defn avg [col1]
  (functions/avg col1))

(defn mean [col1]
  (functions/mean col1))

(defn max_ [col1]
  (functions/max col1))

(defn min_ [col1]
  (functions/min col1))

(defn sum [col1]
  (functions/sum col1))