(ns user
  (:require [sql2dsql.transpiler :as transpiler])
  (:import
   (sql2dsql.pgquery PgQueryLibInterface)))

(def native-lib (PgQueryLibInterface/load "libpg_query.dylib"))
(def parse-sql (transpiler/make-parser native-lib))
(def ->dsql (transpiler/make native-lib))


(parse-sql "select * from patient where id = '1'")
(->dsql "select * from patient where id = '1'")
