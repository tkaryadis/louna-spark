(ns louna.datasets.column
  (:require louna.datasets.schema)
  (:import (org.apache.spark.sql Column)))

(defn <_ [col1 col2]
  (.lt col1 col2))

(defn <=_ [col1 col2]
  (.leq ^Column col1 col2))


#_(defn >_ [col1 col2]
  (.gt col1 col2))

(defmacro >_ [col1 col2]
  `(.gt ~col1 ~col2))

(defn >=_ [col1 col2]
  (.geq ^Column col1 col2))

(defn =_ [col1 col2]
  (.equalTo col1 col2))

#_(defmacro =_ [col1 col2]
  `(.equalTo ~col1 ~col2))

(defn true_? [col1]
  (.equalTo col1 true))

(defn false_? [col1]
  (.equalTo col1 false))

(defn not_ [col1]
  (.equalTo col1 false))

(defn nil_? [col1]
  (.isNull col1))

(defn not-nil_? [col1]
  (.isNotNull col1))

(defn not=_ [col1 col2]
  (.notEqual col1 col2))

#_(defn *_ [col1 col2]
  (.multiply  col1 col2))

(defmacro *_ [col1 col2]
  `(.multiply  ~col1 ~col2))

(defn -_ [col1 col2]
  (.minus  col1 col2))

#_(defn +_ [col1 col2]
  (.plus  col1 col2))

(defmacro +_ [col1 col2]
  `(.plus  ~col1 ~col2))

(defn div [col1 col2]
  (.divide  col1 col2))

(defn contains_? [col1 & values]
  (.isin col1 (into-array values)))

#_(defn includes? [col1 value]
  (.contains col1 value))

(defmacro includes? [col1 value]
  `(.contains ~col1 ~value))

(defn cast_ [col1 ctype]
  (.cast col1 (ctype louna.datasets.schema/schema-types)))

(defn desc [col1]
  (.desc col1))

(defn asc [col1]
  (.asc col1))

(defn var-argsf [f & args]
  (let [first-value (f (first args) (second args))
        args (rest (rest args))]
    (loop [args args
           nested-f first-value]
      (if (empty? args)
        nested-f
        (recur (rest args) (f (first args) nested-f))))))