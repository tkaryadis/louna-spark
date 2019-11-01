(ns louna.library.util
  (:import (java.util Properties)))

(defn ^Properties as-properties
  "Convert any seq of pairs to a java.utils.Properties instance.
   Uses as-str to convert both keys and values into strings."
  {:tag Properties}
  [m]
  (let [p (Properties.)]
    (doseq [[k v] m]
      (.setProperty p (str k) (str v)))
    p))

(defmacro macrof [macro]
  `(fn [& args#] (eval (cons '~macro args#))))

(defmacro var-args [f & args]
  (let [first-value (list f (first args) (second args))
        args (rest (rest args))]
    (loop [args args
           nested-f first-value]
      (if (empty? args)
        nested-f
        (recur (rest args) (list f (first args) nested-f))))))