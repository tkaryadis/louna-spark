(ns louna-tests.udf
  (:require [louna.state.settings :as settings]
            [louna.datasets.sql :as sql]
            [louna.datasets.schema :as t]
            [louna.state.settings :as settings])
  (:use louna.datasets.column
        louna.datasets.sql-functions
        louna.q.run
        louna.datasets.udf)
  (:import (org.apache.spark.sql.types DataTypes)))

(settings/set-local-session)
(settings/set-log-level "ERROR")
(settings/set-base-path "/home/white/IdeaProjects/louna-spark/")

;;---------------------------Schema--------------------------------------
;;-----------------------------------------------------------------------

(def df (-> (.read (get-session))
            (.format "json")
            (.load (str (settings/get-base-path)
                        "/data/flight-data/json/2015-summary.json"))))

(prn (str (.schema df)))

;;-------------------------UDF-------------------------------------

;;Ftiaxno mia sinartisi
;;Spark tin kani serialize(bits),kai tin stelni se ola ta nodes
;;Ean i sinartisi einai JVM sinartisi,tote mikro to kostos se perfomance
;;(arkei na min xrisimopoio poli ton garbage collector?)

;;Stin python einai poli pio argo omos giati trexo idiko python process
;;se kathe node,kai epidi kano ta data serialize oste i python na ta
;;katanoisi(genika to spark den trexi pote python,otan exo build in
;; sinartisis,xristis grafi python alla scala trexi kanonika,alla edo
;; nomizo oti trexi ontos i python)

;;Return types = einai aparetiti,kai spark epitrepoi kapoious sigekrimenous
;;tipots mono nomizo(ean prokipsi alos tipos=>spark epistrefi null)

;;se aplo benchmark,einai 10% pio arges

;;UDF without fn macros

;;works re-run (without re-compile)
(defudf myudf1 (fn [x] (* x 2)) 1 :long)

(q df
   ((myudf1 ?count) ?x2)
   (.show 5))

;;works re-run(without re-compile)
(defudf myudf2 :long [x]
        (* x 2))
(q df
   ((myudf2 ?count) ?x2)
   (.show 5))


;;-----------------join+filter+bind with udfs-----------------------

(def person (sql/seq->df [[0 "Bill Chambers" 0 (long-array [100])]
                          [1 "Matei Zaharia" 1 (long-array [500 250 100])]
                          [2 "Michael Armbrust" 1 (long-array [250 100])]]

                         [["id" :long]
                          ["name"]
                          ["graduate_program" :long]
                          ["spark_status" (t/array-type DataTypes/LongType)]]))

(def graduateProgram (sql/seq->df [[0 "Masters" "School of Information" "UC Berkeley"]
                                   [2 "Masters" "EECS" "UC Berkeley"]
                                   [1 "Ph.D." "EECS" "UC Berkeley"]]
                                  [["id" :long]
                                   ["degree"]
                                   ["department"]
                                   ["school"]]))

(defudf b1 :long [x]
          (if (= x 1) 10 x))

(defudf f1 :boolean [x y]
          (= x y))

(defn f2 [x]
  (* x 2))

(defudf b2 f2 1 :long)

(q (person ?id ?name ?cid ?status)
   ((b1 ?cid) ?pid2)
   ((b2 ?cid) ?pid3)
   (graduateProgram ?cid ?degree ?dep ?school)
   ((f1 ?cid ?id))
   .show)