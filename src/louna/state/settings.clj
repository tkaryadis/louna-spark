(ns louna.state.settings
  (:import (org.apache.spark.sql SparkSession)
           (org.apache.spark.api.java JavaSparkContext)
           (org.apache.spark SparkConf)))

;;The user sets the session so louna knows where to find the spark session object
(def session (atom nil))

(defn get-session []
  @session)

(defn set-session [s]
  (reset! session s))

(defn close-session []
  (.cloneSession @session))

;;default settings for a local session,so we dont do that everytime
;;creates a local session and set the session atom to that
(defn set-local-session []
  (let [conf (-> (SparkConf.)
                 (.setMaster "local[*]") ;;
                 (.setAppName "local-app")
                 (.set "spark.sql.shuffle.partitions" "5") ;;200 default(too slow for local)
                 )
        local-session (-> (SparkSession/builder)
                          (.config conf)
                          (.getOrCreate))]
    (do (reset! session local-session)
        nil)))

(defn get-sc []
  (-> (get-session)
      (.sparkContext)
      (JavaSparkContext/fromSparkContext)))

(defn set-log-level [level]
  (.setLogLevel (get-sc) level))


;;Louna joins if columns same name,so no header with same name columns
;;but to be more safe forces optionally the user to use names for the join
;;variables for example ?myvar not _ ,if join on _ variable => warning/error
;;1)join          "ignore"
;;2)join but warn "warn"
;;3)throw error   "error"

(def join-safety (atom "ignore"))

(defn set-join-safety [safety]
  (reset! join-safety safety))


;;to reduce the path size,to the data,if all data in in one directory
;;base path can be the common part of the path
(def base-path (atom ""))

(defn set-base-path [path]
  (reset! base-path path))

(defn get-base-path []
  @base-path)

;;if dataframe schema is  name,lastname
;;(df ?name "kariadis")
;;if keep constants is true we keep the ?lastname on the result header
;;if not we only filter and remove the column
(def keep-constants-value (atom true))

(defn keep-constants [v]
  (reset! keep-constants-value v))

;;random names for the udfs,the user dont use any of those,its for internal use
(def udf-names-counter (atom 0))

(defn new-udf-name []
  (let [n (swap! udf-names-counter inc)]
    (str "udf" n)))

(def db-url (atom "jdbc:postgresql://localhost/white?user=user&password=pass"))
