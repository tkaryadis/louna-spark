(ns louna.db.run-sql
  (:require louna.state.settings)
  (:import (java.sql DriverManager)))

(defn update-db [ sql-query]
  (with-open [connection (DriverManager/getConnection @louna.state.settings/db-url)]
    (try (do (.executeUpdate (.createStatement connection) sql-query)
             (.close connection)
             true)
         (catch Exception e
           (println (.toString e))
           false))))