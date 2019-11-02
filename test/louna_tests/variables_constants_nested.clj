(ns louna-tests.variables-constants-nested
  (:require sparkdefinitive.init-settings
            [louna.state.settings :as settings])
  (:use louna.datasets.column
        louna.datasets.sql-functions
        louna.q.run))

(settings/set-local-session)
(settings/set-log-level "ERROR")
(settings/set-base-path "/home/white/IdeaProjects/louna-spark/")

;;--------------------------Load test data--------------------------------

(def df
  (-> (.read (get-session))
      (.format "csv")
      (.option "header" "true")
      (.option "inferSchema" "true")
      (.load (str (settings/get-base-path)
                  "/data/retail-data/by-day/2010-12-01.csv"))))

;;(.printSchema df)

;;-----------------variables in filters/binds------------------------------


(q  df
    ((contains_? ?StockCode "DOT") (.or (>_ ?UnitPrice 600)
                                      (includes? ?Description "POSTAGE")))
    ((lit true) ?expensive)
    [?expensive ?unitPrice]
    (.show 5))

(defn fq [stock-code unit-price true-var show-number]
  (q  df
      ((contains_? ?StockCode stock-code) (.or (>_ ?UnitPrice unit-price)
                                               (includes? ?Description "POSTAGE")))
      ((lit true-var) ?expensive)
      [?expensive ?unitPrice]
      (.show show-number)))

(fq "DOT" 600 true 5)

;;----------------------constants/variables in predicates-----------------------------------

;;if false => drop the column of the constant (filter+drop),else only filter
(louna.state.settings/keep-constants false)

(q  (df ?InvoiceNo 21777 - - - - ?CustomerID)
    (.show 20))

(louna.state.settings/keep-constants true)

(def myStockCode 21777)

(q  (df ?InvoiceNo myStockCode - - - - ?CustomerID)
    (.show 20))

(let [myStockCode1 21777]
  (q  (df ?InvoiceNo myStockCode1 - - - - ?CustomerID)
      (.show 20)))

;;--------------------------local df-------------------------------------------------------

;;if local a : is added
(let [df1 (-> (.read (get-session))
              (.format "csv")
              (.option "header" "true")
              (.option "inferSchema" "true")
              (.load (str (settings/get-base-path)
                          "/data/retail-data/by-day/2010-12-01.csv")))]
  (q (:df1 ?InvoiceNo)
     .show))


;;-----------------------nested queries--------------------------------------------------

(q df
   (.show 2))

;;==

(q (q df)
   (.show 2))


;;== (join the table to its self)

(q df
   ((q df) _)
   (.show 2))

;;==

(q df
   ((q (q df)) __)
   (.show 2))
