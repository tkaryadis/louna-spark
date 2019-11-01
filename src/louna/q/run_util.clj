(ns louna.q.run-util)

(defn add-header [dataset col-names]
  (.toDF dataset (into-array col-names)))

(defn add-indexed-qvars [qvars header indexed-qvars]
  (reduce (fn [qvars indexed-qvar]
            (let [parts (clojure.string/split indexed-qvar #":")
                  qvar-index (read-string (subs (first parts) 1))
                  qvar-index (if (number? qvar-index)
                               (dec qvar-index)
                               (.indexOf header (str qvar-index)))
                  qvar-name (str "?" (second parts))]
              (assoc qvars qvar-index qvar-name)))
          qvars
          indexed-qvars))

(defn add-named-qvars [qvars header named-qvars]
  (reduce (fn [qvars indexed-qvar]
            (let [parts (clojure.string/split indexed-qvar #":")
                  name-on-header (subs (first parts) 1)
                  new-name (second parts)
                  named-qvar-index (.indexOf header name-on-header)]
              (assoc qvars named-qvar-index (str "?" new-name))))
          qvars
          named-qvars))

(defn separate-constants-qvars [header qvars]
  (reduce (fn [[temp-map qvars index] qvar]
            (if (or (louna.q.transform/unnamed-qvar? qvar)
                    (clojure.string/starts-with? qvar "?"))
              [temp-map (conj qvars qvar) (inc index)]
              (let [header-var (str "?" (get header index))]
                [(assoc temp-map header-var qvar)
                 (conj qvars header-var)
                 (inc index)])))
          [{} [] 0]
          qvars))

;;default is not add missing
(defn add-out-of-order-vars [qvars header]
  (let [last-qvar (peek qvars)
        add-missing? (= last-qvar "__")
        qvars (if (or (= last-qvar "__") (= last-qvar "--"))
                (pop qvars)
                qvars)

        ;;?:name ?name:name1 ?index:name
        out-of-order-qvars (filter (fn [qvar] (re-find #"\?.*:" qvar)) qvars)
        ;;?:name ?name:name1
        named-qvars (filter (fn [qvar] (re-find #"\?\D.*:" qvar)) out-of-order-qvars)
        ;;?index:name
        indexed-qvars (filter (fn [qvar] (re-find #"\?\d+:" qvar)) out-of-order-qvars)

        ;;?qvar
        qvars (filter (fn [qvar] (nil? (re-find #"\?.*:" qvar))) qvars)
        qvars (if add-missing?
                (into [] (concat qvars (take (- (count header) (count qvars)) (repeat "_"))))
                (into [] (concat qvars (take (- (count header) (count qvars)) (repeat "-")))))
        qvars (add-indexed-qvars qvars header indexed-qvars)
        qvars (add-named-qvars qvars header named-qvars)
        ]
    qvars))

(defn get-var-position [sorted-vars var-name]
  (loop [index 0]
    (let [cur-var (get sorted-vars index)]
      (if (> index (count sorted-vars))
        (do (prn "Var" var-name " Not found in " sorted-vars) (System/exit 0))
        (if (= cur-var var-name)
          index
          (recur (inc index)))))))

(defn sort-vars [sorted-vars vars]
  (into [] (sort-by (partial get-var-position sorted-vars)
                    vars)))

;;join type is always in the end for example  (mydf ?x ?y :outer)
(defn get-join-type [qvars]
  (if (keyword? (peek qvars))
    [(pop qvars) (name (peek qvars))]
    [qvars "inner"]))