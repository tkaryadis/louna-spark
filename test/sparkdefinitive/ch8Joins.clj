(ns sparkdefinitive.ch8Joins
  (:require [louna.state.settings :as settings]
            sparkdefinitive.init-settings
            [louna.datasets.schema :as t]
            [louna.datasets.sql :as sql]
            louna.library.scalaInterop
            clojure.set
            clojure.string
            [louna.state.settings :as state])
  (:use louna.datasets.column
        louna.datasets.sql-functions
        louna.q.run
        louna.library.util)
  (:import (org.apache.spark.sql.types DataTypes)
           [org.apache.spark.sql functions Column]))


(sparkdefinitive.init-settings/init)

;;-------------------------Join types-------------------------------

;(.join df1 df2 (join_columns) join-type)

;;Join-types (joinMatch=same values on join attributes)
;;1)inner
;;  if join match => join
;;  header= header1+header2
;;2)outer (== "full" =="full_outer")
;;  if joinMatch => join
;;  else join but add nulls as values
;;  header= header1+header2
;;3)left_outer (== left)
;;  if joinMatch=> join
;;  else join add nulls,but ONLY from the left
;;  header= header1+header2
;;4)right_outer (== right)
;;  if joinMatch=> join
;;  else join add nulls,but ONLY from the left
;;  header= header1+header2
;;5)left_semi
;;  keep the rows of the left that "would" join,but dont do join
;;  header= header_left
;;6)left_anti
;;  keep the rows of the that "wouldnt" join,but dont do any join
;;  header=header_left
;;7)cross
;;  for every row of the left, make a join with all rows of the right
;;  (we don't care about joinMatch,every row joions with every row)
;;  header=header1+header2  , size=n*m

(def df1 (sql/seq->df [[1 "A1"]
                       [2 "A2"]
                       [3 "A3"]
                       [4 "A4"]]
                      [["id" :long]
                       ["value"]]))

(def df2 (sql/seq->df [[3 "A3"]
                       [4 "A4"]
                       [4 "A4_1"]
                       [5 "A5"]
                       [6 "A6"]]
                      [["id" :long]
                       ["value"]]))

(def join-types ["inner" "outer" "full" "full_outer"
                 "left" "left_outer" "right"
                 "right_outer" "left_semi" "left_anti"])

(def join-types-k (map keyword join-types))

(doseq [join-type join-types-k]
  (q (df1 ?id ?v1)
     (df2 ?id ?v2 join-type)
     (order-by ?id)
     .show))

;------------------------------------------------------------------------------

;;val person = Seq(
;    (0, "Bill Chambers", 0, Seq(100)),
;    (1, "Matei Zaharia", 1, Seq(500, 250, 100)),
;    (2, "Michael Armbrust", 1, Seq(250, 100)))
;  .toDF("id", "name", "graduate_program", "spark_status")
;val graduateProgram = Seq(
;    (0, "Masters", "School of Information", "UC Berkeley"),
;    (2, "Masters", "EECS", "UC Berkeley"),
;    (1, "Ph.D.", "EECS", "UC Berkeley"))
;  .toDF("id", "degree", "department", "school")
;val sparkStatus = Seq(
;    (500, "Vice President"),
;    (250, "PMC Member"),
;    (100, "Contributor"))
;  .toDF("id", "status")

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

(def sparkStatus (sql/seq->df [[500 "Vice President"]
                               [250 "PMC Member"]
                               [100, "Contributor"]]
                              [["id" :long]
                               ["status"]]))

(println (sql/get-header person))
(println (sql/get-header graduateProgram))
(println (sql/get-header sparkStatus))

;;Headers with same columns names,is confusing
;;In Louna this is prevented before happening,because same column name => join
;;In louna if you want to join name the variable,for safety dont use _ for it
;;For protection from accidental joins,louna has 3 levels of join safety
;; "error"  exception if join in variables i haven't named(didnt use the ?qvar notation)
;; "warn"   warn but join if join in variables i haven't named
;; "ignore"

(state/set-join-safety "warn")

;;[id name graduate_program spark_status]  person
;;[id degree department school]            graduateProgram
;;[id status]                              sparkStatus

;;val joinExpression = person.col("graduate_program") === graduateProgram.col("id")
;;person.join(graduateProgram, joinExpression).show()
;;= person.join(graduateProgram, joinExpression, "inner").show()

;;In graduateProgram join at ?gid don't cause an error/warning,because i named it
;;when i name a variable its like approving the possible join
(q (person _ _ ?gid)
   (graduateProgram ?gid __)
   .show)

;;==

(q (person _ _ ?gid)
   (graduateProgram ?gid __ :inner)
   .show)

;;person.join(graduateProgram, joinExpression, "outer").show()

(q (person _ _ ?gid)
   (graduateProgram ?gid __ :outer)
   .show)

;;graduateProgram.join(person, joinExpression, "left_outer").show()

(q (person _ _ ?gid)
   (graduateProgram ?gid __ :left_outer)
   .show)

;;graduateProgram.join(person, joinExpression, "right_outer").show()

(q (person _ _ ?gid)
   (graduateProgram ?gid __ :right_outer)
   .show)

;;graduateProgram.join(person, joinExpression, "left_semi").show()

(q (person _ _ ?gid)
   (graduateProgram ?gid __ :left_semi)
   .show)


;;---------------Union(simply add the rows of df2 "below",no join,they must have same schema)------------------

;;val gradProgram2 = graduateProgram.union(Seq(
;    (0, "Masters", "Duplicated Row", "Duplicated School")).toDF())

(def gradProgram2
  (q graduateProgram
     (union (sql/seq->df [[0 "Masters" "Duplicated Row" "Duplicated School"]]
                         (.schema graduateProgram)))))

(.show gradProgram2)

;;gradProgram2.join(person, joinExpression, "left_semi").show()
;;graduateProgram.join(person, joinExpression, "left_anti").show()

(q (gradProgram2 ?gid __)
   (person _ _ ?gid :left_semi)
   .show)

(q (graduateProgram ?gid __)
   (person _ _ ?gid :left_anti)
   .show)

;;person.crossJoin(graduateProgram).show()

(q person
   (crossJoin graduateProgram)
   .show)

;;person.withColumnRenamed("id", "personId")
;  .join(sparkStatus, expr("array_contains(spark_status, id)")).show()

(q (person ?personId __)
   (sparkStatus __)
   ((functions/array_contains ?spark_status ?id))
   .show)

;;catalyst auto-optimize it,the filter runs at join time
;;all the join expressions in louna are used in filters,except the = which is the default
(q (person ?personId __)
   (sparkStatus __)
   ((functions/array_contains ?spark_status ?id))
   (.explain))

(q  person
    (rename ?id ?personId)
    (.join sparkStatus (functions/expr "array_contains(spark_status, id)"))
    .explain)

;;val gradProgramDupe = graduateProgram.withColumnRenamed("id", "graduate_program")
;val joinExpr = gradProgramDupe.col("graduate_program") === person.col(
;  "graduate_program")
;;person.join(gradProgramDupe, joinExpr).show()

;;2 columns graduate program,one belongs to person and the other to gradProgramDupe
(def gradProgramDupe (q graduateProgram (rename ?id ?graduate_program)))
(q person
   (.join gradProgramDupe (=_ (.col gradProgramDupe ?:graduate_program)
                              (.col person ?:graduate_program)))
   .show)

;;In louna you can do that rename with simpler code
;;louna joins always returns 1 column on join attributes
;;here in person we dont name the variable that will be joined,so in "error" mode exception would occur
;;Error implicit join on :  [graduate_program],in "warn" warning would have being printed
(q (graduateProgram ?graduate_program __)
   (person __)
   .show)

;;this would work without any error or warning,because in person i named the join-var
(q (graduateProgram ?graduate_program __)
   (person _ _ ?graduate_program)
   .show)

;;TODO
;;page 156 2 examples + theory of How Spark Performs Joins

;;Completed
;;(skipped some Handling Duplicate Column Names,in louna this is prevented before happening)