(ns louna.datasets.sql
  (:require louna.datasets.schema
            louna.state.settings)
  (:import (org.apache.spark.sql RowFactory)
           (org.apache.spark.api.java JavaRDD)
           [org.apache.spark.sql functions Column]))

;;---------------------------sql utils------------------------------------

(defn string->col [s]                                       ;!!!!!!!
  (if (string? s)
    (functions/col s)
    s))

(defn col->string [c]
  (if (.isInstance org.apache.spark.sql.Column c)
    (str c)
    c))

;;------------------------------------------------------------------------


(defn sa [strs]
  (let [strs (map col->string strs)]
    (into-array String strs)))

(defn ca [cols]
  (let [cols (map string->col cols)]
    (into-array Column cols)))

;;columns can be strings(auto-converted to Columns) or Columns)
(defn select [dataset & cols]
  (.select dataset (ca cols)))

(defn drop-col [dataset & cols]                        ;;!!!!!!!!!!!!!!
  (.drop dataset (sa cols)))

(defn order-by
  "DataFrame orderBy"
  [data-frame & cols]
  (.orderBy data-frame (ca cols)))

(defn groupBy
  "DataFrame group by"
  [data-frame cols]
  (.groupBy data-frame (ca cols)))

(defn cube [data-frame & cols]
  (.cube data-frame (ca cols)))


(defn join-on
  "data frame inner join use one or two column name"
  ([df1 df2 col1]
   (.join df1 df2
          (.equalTo (.col df1 col1) (.col df2 col1))))
  ([df1 df2 col1 col2]
   (.join df1 df2
          (.equalTo (.col df1 col1) (.col df2 col2))))
  ([df1 df2 col1 col2 join-type]
   (.join df1 df2
          (.equalTo (.col df1 col1) (.col df2 col2))
          join-type)))


(defn with-column-renamed
  "DataFrame with column renamed."
  [data-frame exist-name new-name]
  (.withColumnRenamed data-frame exist-name new-name))

(defn with-column [dataset new-col-name value]
  (.withColumn dataset new-col-name value))

;;--------------------Seq->Row-----------------------  !!!!!!!

(defn seq->row [seq]
  (if (not (coll? seq))
    (RowFactory/create (into-array Object [seq]))
    (RowFactory/create (into-array Object seq))))

(defn row [& values]
  (seq->row values))

(defn print-row [row]
  (println (str row)))

(defn print-rows [rows]
  (println "Row-count =" (count rows))
  (dorun (map print-row rows)))

;;-----------Nested seq->Dataframe-------!!!!!!!!!!!!

;;1 seq => 1 column
(defn seq->df [seq schema]
  (let [schema (if (vector? schema)
                 (louna.datasets.schema/build-schema schema)
                 schema)
        rows-seq (map seq->row seq)
        df (.createDataFrame (louna.state.settings/get-session) ^JavaRDD rows-seq schema)]
    df))

;;----------row-vector-----------------------


(defn row->seq [row]
  (loop [index 0
         row-seq []]
    (if (= index (.size row))
      row-seq
      (recur (inc index) (conj row-seq (.get row index))))))


(defn get-row [row index]
  (if (string? index)
    (.getAs row index)
    (.get row index)))


;;----------Add collumns names to dataset-------------- !!!

(defn add-header [dataset & col-names]
  (.toDF dataset (into-array col-names)))

;;---------get header-----------------------------------!!!

(defn get-header [df]
  (into [] (map str (.columns df))))


;;------------------------------------------------------------------------

(defn where [dataset col]
  (.filter dataset (string->col col)))