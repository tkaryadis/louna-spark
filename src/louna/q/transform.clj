(ns louna.q.transform
  (:require clojure.set
            louna.library.util)
  (:use louna.datasets.column
        louna.datasets.sql-functions)
  (:import (org.apache.spark.sql Column)))

(defn nested-query? [qform]
  (and (coll? qform)
       (coll? (first qform))
       (= (first (first qform)) 'q)))

;;(df_name ...)  or if local  (:df_name ....)
;;df_name must be dataset or unresolved symbol(if local)
(defn df-qform? [qform]
  (or (nested-query? qform)
      (and (coll? qform)
           (not (coll? (first qform)))                      ;;not filter/bind
           (or (keyword? (first qform))                     ;;maybe local var for df
               (try (if (instance? org.apache.spark.sql.Dataset (eval (first qform))) ;;global as df
                      true
                      false)
                    (catch Exception e false))))))

;;(() () ... ())
(defn filter-qform? [qform]
  (and (list? qform)
       (apply (louna.library.util/macrof and) (map list? qform))))

;;(() ?qvar)
(defn bind-qform? [qform]
  (and (coll? qform)
       (= (count qform) 2)
       (coll? (first qform))
       (or (clojure.string/starts-with? (str (second qform)) "?")
           (= (str (second qform)) "-"))))

;;[....]
(defn select-qform? [qform]
  (vector? qform))

;;(agg ...)
(defn agg-qform? [query]
  (and (coll? query)
       (= (first query) 'agg)))


;;array => first=dataset or first=undefened
;;bind = first=col second=variable/variables
;;else filtro
(defn get-qform-type [qform]
  (cond
    (df-qform? qform)     0
    (filter-qform? qform) 1
    (bind-qform? qform)   2
    (select-qform? qform) 3
    (agg-qform? qform)    4
    :else                 5))              ;;spark code

(defn qvar->col [qvar]
  (cond

    ;;"?:sum(avc(x))" or "?:dest" (not needed the " in the second)
    (clojure.string/starts-with? qvar "\"?:")
    (let [qvar (subs qvar 3 (dec (.length qvar)))]
      (str "\"" qvar "\""))

    ;;case "?sum(avg(x))"
    (clojure.string/starts-with? qvar "\"?")
    (let [qvar (subs qvar 2 (dec (.length qvar)))]
      (str "(org.apache.spark.sql.functions/col \"" qvar "\")"))

    ;;case ?:dest
    (clojure.string/starts-with? qvar "?:")
    (let [qvar (subs qvar 2)]
      (str "\"" qvar "\""))

    ;;case ?dest
    :else
    (let [qvar (subs qvar 1)]
      (str "(org.apache.spark.sql.functions/col \"" qvar "\")"))))

;;qvars if inside "" can be whatever
;;if ?varname then varname must be words or digits
(defn remove-qmark [code]
  (let [code (str code)

        code (clojure.string/replace
               code
               #"\"\?:[^\"]+\""
               (fn [qvar]
                 (qvar->col qvar)))

        code (clojure.string/replace
               code
               #"\"\?[^:\"]+\""
               (fn [qvar]
                 (qvar->col qvar)))

        code (clojure.string/replace
               code
               #"\?:[\w-_]+"
               (fn [qvar]
                 (qvar->col qvar)))


        code (clojure.string/replace
               code
               #"\?[\w-_]+"
               (fn [qvar]
                 (qvar->col qvar)))]
    (read-string code)))

(defn unnamed-qvar? [qvar]
  (let [qvar (str qvar)]
    (or (= qvar "_")
        (= qvar "__")
        (= qvar "-")
        (= qvar "--"))))

(defn qvar? [qvar]
  (clojure.string/starts-with? (str qvar) "?"))

(defn qvars-to-strs [qvars]
  (into [] (map (fn [qvar]
                  (if (or (qvar? qvar) (unnamed-qvar? qvar)) ;;if not a variable symbol
                    (str qvar)
                    qvar))
                qvars)))

(defn tranform-select-args [qform]
  (into [] (map (fn [select-arg]
                  (cond

                    (bind-qform? select-arg)
                    (let [code (str (first select-arg))
                          code (remove-qmark code)
                          bind-var (subs (str (second select-arg)) 1)]
                      (list (symbol ".alias") code bind-var))

                    (or (= select-arg '*) (= select-arg "*") (= select-arg '__))
                    (list 'org.apache.spark.sql.functions/expr "*")

                    ;;qvar as string
                    (string? select-arg)
                    (qvar->col select-arg)

                    ;;?qvar or code with ?qvars (column functions)
                    :else
                    (remove-qmark select-arg)))
                qform)))

(defn tranform-agg-args [qform]
  (let [agg-args (rest qform)
        agg-args (map (fn [agg-arg]
                        (if (louna.q.transform/bind-qform? agg-arg)
                          (let [code (str (first agg-arg))
                                code (louna.q.transform/remove-qmark code)
                                bind-var (subs (str (second agg-arg)) 1)]
                            `(.alias ~code ~bind-var))
                          (louna.q.transform/remove-qmark agg-arg)))
                      agg-args)
        qvars (into [] agg-args)]
    qvars))

(defn add-filter-boolean-functions [qform]
  (let [qform (map (fn [filter-form]
                     (let [fst (first filter-form)]
                       (cond
                         (qvar? fst)
                         `(true_? ~fst)

                         (clojure.string/starts-with? (str fst) "!?")
                         `(false_? ~(symbol (subs (str fst) 1)))

                         :else
                         filter-form)))
                   qform)
        qform (into [] qform)]
    qform))

(defn transform-qforms [qforms]
  (loop [qforms qforms
         final-qforms []]
    (if (empty? qforms)
      final-qforms
      (let [qform (first qforms)
            qform-type (get-qform-type qform)]

        (cond

          ;;(df ?...)
          (= qform-type 0)
          (let [df-var (first qform)
                df-var (if (keyword? df-var)
                         (symbol (name df-var))
                         df-var)
                qvars (qvars-to-strs (rest qform))]
            (recur
              (rest qforms)
              (conj final-qforms `(louna.q.run/select-join ~df-var ~qvars))))

          ;;filter
          (= qform-type 1)
          (let [qform (add-filter-boolean-functions qform)
                qform (remove-qmark qform)
                final-qforms (reduce (fn [final-qforms filter-code]
                                   (conj final-qforms `(louna.q.run/where ~filter-code)))
                                 final-qforms
                                 qform)]
            (recur (rest qforms) final-qforms))

          ;;bind
          (= qform-type 2)
          (let [code (str (first qform))
                code (remove-qmark code)
                bind-var (subs (str (second qform)) 1)]
            (recur
              (rest qforms)
              (conj final-qforms `(louna.q.run/bind ~code ~bind-var))))

          ;;sql type select [...]
          (= qform-type 3)
          (let [select-args (tranform-select-args qform)]
            (recur
              (rest qforms)
              (conj final-qforms `(louna.q.run/sql-select ~select-args))))

          ;;(agg ...)
          (= qform-type 4)
          (let [aggr-args (tranform-agg-args qform)]
            (recur
              (rest qforms)
              (conj final-qforms `(louna.q.run/agg ~aggr-args))))


          ;;spark code
          ;(= qform-type 5)
          :else
          (let [spark-code (remove-qmark qform)]
            (recur
              (rest qforms)
              (conj final-qforms spark-code))))))))

(defn transform-first-qform [first-qform]
  (if (louna.q.transform/df-qform? first-qform) ;;can only be df query or spark code
    (let [df-var (first first-qform)
          df-var (if (keyword? df-var)
                   (symbol (name df-var))
                   df-var)
          qvars (qvars-to-strs (rest first-qform))]
      `(louna.q.run/select ~df-var ~qvars false))
    (remove-qmark first-qform)))   