(ns apps.tfidf-louna
  (:require sparkdefinitive.init-settings
            [louna.state.settings :as settings]
            [louna.datasets.sql :as sql]
            louna.datasets.schema)
  (:use louna.datasets.column
        louna.datasets.sql-functions
        louna.q.run
        louna.library.util
        louna.datasets.udf)
  (:import (org.apache.spark.sql SparkSession Dataset)
           [org.apache.spark.sql functions Column DataFrameStatFunctions]
           (scala.collection.mutable WrappedArray)))

;;not good

(louna.state.settings/set-local-session)
(louna.state.settings/set-log-level "ERROR")
(louna.state.settings/set-base-path "/home/white/IdeaProjects/louna/")

(def docs (sql/seq->df [[1 "Aristotle and Plato"]
                        [2 "the athenian democracy"]
                        [3 "democrats versus republicans"]
                        [4 "democracy versus republic"]
                        [5 "Socrates Plato and Aristotle"]
                        [6 "democrary versus mediocracy"]
                        [7 "Superman versus Batman and Catwoman"]]
                       [["docid" :long]
                        ["doc"]]))

(def ndocs (.count docs))

;;array
(defn process-doc-terms [doc-array] ;;ArrayType of spark is scala.collection.mutable.WrappedArray in UDF
  (let [jarray (.array ^WrappedArray doc-array)]
    (loop [index 0]
      (if (= index (alength jarray))
        jarray
        (recur (do (aset jarray index (clojure.string/trim (clojure.string/lower-case (aget jarray index))))
                   (inc index)))))))

(defudf s-process-doc-terms process-doc-terms 1 (louna.datasets.schema/array-type :string))

(defudf get-type (fn [x] (str (type x))) 1 :string)

;;docSet=?docDTerms (not a set like it was)
(def docs-terms-meta
  (q (docs ?docid ?doc)
     ((split ?doc "\\s+") ?docArray)
     ((s-process-doc-terms ?docArray) ?docTerms)
     ((functions/array_distinct ?docTerms) ?docDTerms)      ;;array type but with distinct terms
     [?docid ?docTerms ?docDTerms]))

;;in how many documents the term appears
(defn get-df [term]
  (q docs-terms-meta
     ((functions/array_contains ?docDTerms term))
     .count))

(def get-df-mem (memoize get-df))

(defn log2 [x]
  (/ (Math/log x) (Math/log 2)))

(defn calc-tfidf [freq maxfreq]
  (reduce (fn [freq-tf [key freq]]
            (let [tf (/ freq maxfreq)
                  df (get-df-mem key)
                  idf (log2 (/ ndocs df))
                  w (* tf idf)]
              (assoc freq-tf key w)))
          {}
          freq))

(defn get-tfidf [docTerms docDTerms]
  (let [docTerms-array (.array docTerms)
        docDTerms-array (.array docDTerms)
        freq (frequencies (seq docTerms-array))
        maxfreq (apply max (vals freq))
        tfidf (calc-tfidf freq maxfreq)
        tf-idf-array (make-array Double/TYPE (alength docDTerms-array))]
    (loop [index 0]
      (if (= index (alength tf-idf-array))
        tf-idf-array
        (recur (do (aset tf-idf-array index (get tfidf (aget docDTerms-array index)))
                   (inc index)))))))

(defudf get-tfidf_ get-tfidf 2 (louna.datasets.schema/array-type :double))

(q docs-terms-meta
   ((get-tfidf_ ?docTerms ?docDTerms) ?tfidf)
   [?docid ?docDTerms ?tfidf]
   (.show false))