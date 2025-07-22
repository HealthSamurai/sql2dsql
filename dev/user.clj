(ns user
  (:require [sql2dsql.core :refer [->dsql parse-sql]]))

(parse-sql "select * from patient where id = '1'")
(->dsql "select * from patient where id = '1'")
