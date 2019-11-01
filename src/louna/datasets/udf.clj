(ns louna.datasets.udf
  (:require louna.state.settings
            [louna.datasets.sql :as sql])
  (:import (org.apache.spark.sql.api.java UDF1 UDF0 UDF2 UDF3 UDF4)
           [org.apache.spark.sql functions]))

;;---------UDF---------------------------------

(defrecord UF0 [f]
  UDF0
  (call [this]
    (f)))

(defrecord UF1 [f]
  UDF1
  (call [this x]
    (f x)))

(defrecord UF2 [f]
  UDF2
  (call [this x y]
    (f x y)))

(defrecord UF3 [f]
  UDF3
  (call [this x y z]
    (f x y z)))

(defrecord UF4 [f]
  UDF4
  (call [this x y z w]
    (f x y z w)))


(defn get-udf-class [f nargs]
  (cond
    (= nargs 0)
    (UF0. f)

    (= nargs 1)
    (UF1. f)

    (= nargs 2)
    (UF2. f)

    (= nargs 3)
    (UF3. f)

    (= nargs 4)
    (UF4. f)

    :else
    (do (prn "unknown arity") (System/exit 0))))

(defn call-udf [name & col-names]
  (functions/callUDF name (sql/ca col-names)))

(defn register-udf [f nargs rtype]
  (let [rtype (if (keyword? rtype)
                (get  louna.datasets.schema/schema-types rtype)
                rtype)
        new-udf (get-udf-class f nargs)
        name (louna.state.settings/new-udf-name)
        - (->  (louna.state.settings/get-session)
               (.udf)
               (.register name new-udf rtype))]
    (partial call-udf name)))

(defmacro defudf [arg1 arg2 arg3 arg4]
  (if (vector? arg3)
    ;;define "spark" function
    (let [fname arg1
          rtype arg2
          args arg3
          body arg4]
      `(def ~fname (register-udf (fn ~args ~body) ~(count args) ~rtype)))
    ;;use already defined clojure function
    (let [fname arg1
          f arg2
          nargs arg3
          rtype arg4]
      `(def ~fname (register-udf ~f ~nargs ~rtype)))))

(defn register-udaf [udaf-object]
  (let [name (louna.state.settings/new-udf-name)
        - (.register (.udf (louna.state.settings/get-session))
                     name
                     udaf-object)]
    (partial louna.datasets.udf/call-udf name)))

(defmacro defudaf [fname udaf-object]
  `(def ~fname (register-udaf ~udaf-object)))

