(ns sparkdefinitive.ch9sqlDB
  (:require sparkdefinitive.init-settings
            [louna.state.settings :as settings]
            [louna.datasets.sql :as sql]
            louna.library.gen-util
            [louna.db.run-sql :as run-sql])
  (:use louna.datasets.column
        louna.datasets.sql-functions
        louna.q.run
        louna.library.util
        louna.rdds.api)
  (:import (org.apache.spark.sql SparkSession)))

(sparkdefinitive.init-settings/init)

;;here postgresql will be used



;(def db-url (atom "jdbc:postgresql://localhost/white?user=white&password=death"))

;;Install postgresql,create a user(here white with pass death),
;;and a db named white (here on locahost default port)


;;(def connection (DriverManager/getConnection "jdbc:postgresql://localhost/white?user=white&password=death"))

;;(prn (.isClosed connection))
;;(.close connection)
;;(prn (.isClosed connection))

;;--------------------------------Connect with jdbc-------------------------------------------
;;(for not spark related things JDBC or with some sql abstraction library)
;;(when we mix spark df(read or write) we do it using spark libraries,not just jdbc)

(run-sql/update-db  "DROP TABLE IF EXISTS persons")

;;--------------------------------create an empty table(just schema with no rows)-------------
;;its not necessary both ovewrite and append would create the table if it didnt exist

(def properties (louna.library.util/as-properties {"driver" "org.postgresql.Driver"
                                                       "user" "white"
                                                       "password" "death"
                                                       "table" "myDF"}))


(def persons-init (sql/seq->df []
                               [["id" :long]
                                ["name"]
                                ["lastname"]
                                ["postal-code" :long]]))


(-> persons-init
    (.write)
    (.mode "overwrite")                                     ;;or append (ovewrite create new table)
    (.jdbc "jdbc:postgresql://localhost/"
           "persons"                                         ;;the table name in the DB
           properties))

;;--------------------------------overwrite a table-------------------------------------------

(def persons (sql/seq->df [[0 "takis" "kariadis" 17567]
                           [1 "takis" "petrou" 17562]
                           [2 "kostas" "iliadis" 17564]]
                          [["id" :long]
                           ["name"]
                           ["lastname"]
                           ["postal-code" :long]]))

(-> persons
    (.write)
    (.mode "overwrite")                                     ;;or append (ovewrite create new table)
    (.jdbc "jdbc:postgresql://localhost/"
           "persons"                                         ;;the table name in the DB
           properties))

;;-----------------------------append to a table---------------------------------------------

(def persons1 (sql/seq->df [[3 "mpampis" "mpampoudis" 17642]]
                           [["id" :long]
                            ["name"]
                            ["lastname"]
                            ["postal-code" :long]]))

(-> persons1
    (.write)
    (.mode "append")                                     ;;or append (append the rows in the existing table)
    (.jdbc "jdbc:postgresql://localhost/"
           "persons"                                         ;;the table name in the DB
           properties))



;;--------------------------------Load a table to a df------------------------------------

;;;;\dt on psql to see the user tables+info
(def personsDF (-> ^SparkSession (get-session)
                   (.read)
                   (.format "jdbc")
                   (.option "driver" "org.postgresql.Driver")
                   (.option "url" "jdbc:postgresql://localhost/") ;;/ in the end is important
                   (.option "user" "white")
                   (.option "password" "death")
                   (.option "dbtable" "persons") ;;"schema.tablename" here "public.persons" works also
                   (.load)))

(.show personsDF)

;;-----------------------filter before loading(auto-push down filters)--------------------------

(q personsDF
   ((=_ ?name "takis"))
   (.show))

;;PushedFilters: [*IsNotNull(color), *EqualTo(color,blue)]
(q personsDF
   ((=_ ?name "takis"))
   (.explain))

;;Query parts Pushdown
;;load is lazy,show forces it to be retrieved
;;When we query a database spark tries to make the database do as much as possible,so the db-result
;;that spark will process even fether is small
;;Pushdown
;;  1)projects
;;  2)filters
;;  ...

;;------------------------------------Query pushdown---------------------------------------------

;;Spark has limitations,for example cannot traslate all its functions to sql functions to be run
;;on every database,in case that we need max perfomance we push down the whole query
;;this way everything in the query is done directly by the database,using the syntax of sql of the target db

;;AS is always required for the result name
;;to distinct nomizo oxi pushdown automata ean to grapso san spark query(oxi opos edo pou einai pushdown query)

(def pushdownQuery "(SELECT DISTINCT(name) FROM persons) AS distinct_names")

(def pushedDF (-> ^SparkSession (get-session)
                  (.read)
                  (.format "jdbc")
                  (.option "driver" "org.postgresql.Driver")
                  (.option "url" "jdbc:postgresql://localhost/") ;;/ in the end is important
                  (.option "user" "white")
                  (.option "password" "death")
                  (.option "dbtable" pushdownQuery)
                  (.load)))

(.show pushedDF)