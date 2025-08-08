(ns sql2dsql.sql-parser
  (:require [clojure.data.json :as json]
            [clojure.pprint :as p]
            [sql2dsql.transpiler :refer [stmt->dsql]])
  (:import (com.example.pgquery PgQuery)))

(defrecord SqlParser [parser-fn])

(defn make-sql-parser [native-lib-path]
  (let [pg-query (PgQuery/load native-lib-path)
        parser-fn (fn [sql]
                    (when (nil? sql)
                      (throw (IllegalArgumentException. "SQL string cannot be null")))
                    (when (empty? (str sql))
                      (throw (IllegalArgumentException. "SQL string cannot be empty")))
                    (let [result (.pg_query_parse pg-query (str sql))]
                      (if-let [error (.-error result)]
                        (throw (ex-info (.-message error) {:sql sql}))
                        (json/read-str (.-parse_tree result) :key-fn keyword))))]
    (->SqlParser parser-fn)))

(defn process-stmt [stmt pretty-print? params]
  (try
    (if pretty-print?
      (binding [*print-meta* true]
        (with-out-str (p/pprint (stmt->dsql stmt {:params params}))))
      (stmt->dsql stmt {:params params}))
    (catch Exception e
      (str "Error processing statement: " stmt " with params: " params " Error: " (.getMessage e)))))

(defn sql->dsql
  ([parser sql] (sql->dsql parser sql []))
  ([parser sql params] (sql->dsql parser sql params false))
  ([^SqlParser parser sql params pretty-print?]
   (if-let [parse-fn (:parser-fn parser)]
     (let [parsed-result (parse-fn sql)]
       (mapv #(process-stmt (:stmt %) pretty-print? params) (:stmts parsed-result)))
     (throw (Exception. "Parser is not initialized")))))

(defprotocol ISqlParser
  (parse [this sql])
  (to-dsql [this sql] [this sql params] [this sql params pretty-print?]))

(extend-protocol ISqlParser
  SqlParser
  (parse [this sql]
    ((:parser-fn this) sql))
  (to-dsql
    ([this sql] (to-dsql this sql []))
    ([this sql params] (to-dsql this sql params false))
    ([this sql params pretty-print?]
     (let [parsed-result (parse this sql)]
       (mapv #(process-stmt (:stmt %) pretty-print? params) (:stmts parsed-result))))))