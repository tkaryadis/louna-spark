(ns louna.q.run
  (:require louna.q.transform
            louna.q.run-util
            louna.datasets.column
            louna.datasets.udf
            [louna.datasets.sql :as sql]
            louna.library.scalaInterop
            louna.datasets.sql-functions
            louna.state.settings)
  (:import (org.apache.spark.sql Column)))

(defn get-session []
  (louna.state.settings/get-session))

(defn get-context []
  (louna.state.settings/get-sc))

;;-------------------------------qmacro------------------------------------------------------------

;;q
;; 1)translates to ->
;; 2)produces spark code,for some qforms
;; 3)louna functions for other qforms that will produce spark code on runtime
(defmacro q [first-qform & rest-qforms]
  (let [first-qform (louna.q.transform/transform-first-qform first-qform)
        rest-qforms (louna.q.transform/transform-qforms rest-qforms)
        query (concat (list '->) (list first-qform) rest-qforms)
        - (prn "query" query)
        ]
    query))



;;--------------louna-functions-auto-generated-from-q-to-run-on-runtime------------------------------
;;---------------------------------------------------------------------------------------------------

(declare drop-col)
(declare where)

(defn run-constant-filters [df constant-qvars-map]
  (loop [df df
         constant-qvars-vec (into [] constant-qvars-map)]
    (if (empty? constant-qvars-vec)
      (if (and (not @louna.state.settings/keep-constants-value) (not-empty constant-qvars-map))
        (apply drop-col df (map #(subs % 1) (keys constant-qvars-map)))
        df)
      (let [[qvar value] (first constant-qvars-vec)
            qvar (subs qvar 1)
            df (where df (louna.datasets.column/=_ (org.apache.spark.sql.functions/col qvar) value))]
        (recur df (rest constant-qvars-vec))))))

(defn select [df qvars for-join?]
  (let [header (sql/get-header df)
        [constant-qvars-map qvars -] (louna.q.run-util/separate-constants-qvars header qvars)
        qvars (louna.q.run-util/add-out-of-order-vars qvars header)
        user-qvars (reduce (fn [user-qvars qvar]
                             (if (clojure.string/starts-with? qvar "?")
                               (conj user-qvars (subs qvar 1))
                               user-qvars))
                           []
                           qvars)
        svars (reduce (fn [svars index]
                        (if (not= (get qvars index) "-")
                          (conj svars (get header index))
                          svars))
                      []
                      (range (count qvars)))
        qvars (first (reduce (fn [[qvars index] qvar]
                               (cond

                                 (= qvar "-")
                                 [qvars (inc index)]

                                 (= qvar "_")
                                 [(conj qvars (get header index)) (inc index)]

                                 :else
                                 [(conj qvars (subs qvar 1)) (inc index)]))
                             [[] 0]
                             qvars))
        svars-col (map #(org.apache.spark.sql.functions/col %) svars)
        select-df (louna.q.run-util/add-header (apply (partial sql/select df) svars-col) qvars)

        filtered-df (run-constant-filters select-df constant-qvars-map)]
    (if for-join?
      [filtered-df user-qvars]
      filtered-df)))


;;thelo join mono se qvars pou dilosa else error/warning
;;bug pos na kano join metablites me perierga onomata(pou den mporoune na einai clojure symbols) opos avg(x)

(defn select-join [df1 df2 qvars]
  (let [[qvars join-type] (louna.q.run-util/get-join-type qvars)
        [df2 user-qvars] (select df2 qvars true)
        header1 (sql/get-header df1)
        header2 (sql/get-header df2)
        join-qvars (louna.q.run-util/sort-vars
                    header2
                    (into [] (clojure.set/intersection (into #{} header1) (into #{} header2))))

        join-qvars-user (clojure.set/intersection (into #{} user-qvars) (into #{} join-qvars))
        join-difference (clojure.set/difference (into #{} join-qvars) join-qvars-user)
        - (if (not (empty? join-difference))
            (cond
              (= @louna.state.settings/join-safety "warn")
              (do (println "Warning implicit join on : " (into [] join-difference)))

              (= @louna.state.settings/join-safety "error")
              (do (println "Error implicit join on : " (into [] join-difference))
                  (System/exit 0))
              :else nil))]
    (.join df1 df2 (louna.library.scalaInterop/clj->scala join-qvars) join-type)))


(defn bind [dataset value new-col-name]
  (sql/with-column dataset new-col-name value))

(defn sql-select [dataset cols]
  (apply (partial sql/select dataset) cols))

(defn where [dataset col]
  (sql/where dataset col))

(defn agg [dataset agg-args]
  (.agg dataset (first agg-args) (into-array Column (rest agg-args))))


;;--------------------------louna-functions-user-can-use---------------------------------------------
;;---------------------------------------------------------------------------------------------------

(defn sa [& strs]
  (sql/sa strs))

(defn ca [& cols]
  (sql/ca cols))

(defn drop-col [dataset & cols]                        ;;!!!!!!!!!!!!!!
  (apply (partial sql/drop-col dataset) cols))

(defn order-by [data-frame & cols]
  (apply (partial sql/order-by data-frame) cols))

(defn rename
  ([data-frame exist-name new-name]
   (sql/with-column-renamed data-frame (str exist-name) (str new-name)))
  ([data-frame names-map]
   (loop [names-keys (keys names-map)
          data-frame data-frame]
     (if (empty? names-keys)
       data-frame
       (let [k (first names-keys)
             v (get names-map k)]
         (recur (rest names-keys) (rename data-frame k v)))))))


(defn groupBy [dataset & cols]
  (sql/groupBy dataset cols))

(defn union [df1 df2]
  (.union df1 df2))

(defn crossJoin [df1 df2]
  (.crossJoin df1 df2))

(defn drop-nulls
  ([dataset]
   (let [na (.na dataset)]
     (.drop na)))
  ([dataset arg1]
   (let [na (.na dataset)]
     (.drop na arg1)))
  ([dataset arg1 arg2]
   (let [na (.na dataset)]
     (.drop na arg1 arg2))))

(defn fill-rows
  ([dataset arg1]
   (let [na (.na dataset)]
     (.fill na arg1)))
  ([dataset arg1 arg2]
   (let [na (.na dataset)]
     (.fill na arg1 arg2))))

(defn replace-rows
  ([dataset arg1 arg2]
   (let [na (.na dataset)]
     (.replace na arg1 arg2))))