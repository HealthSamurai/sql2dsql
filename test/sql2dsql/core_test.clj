(ns sql2dsql.core-test
  (:require
    [clojure.test :refer :all]
    [sql2dsql.core :refer [->dsql]]))

(defn parse [sql & params]
  (try
    (-> (apply ->dsql (cons sql params))
        first)
    (catch Exception e
      (println (str "Error parsing SQL: " sql))
      (println (str "Exception: " (.getMessage e)))
      nil)))

(defmacro test-sql
  ([num sql expected]
   `(test-sql ~num ~sql [] ~expected))
  ([num sql params expected]
   `(testing (str "Test " ~num ": " ~sql)
      (let [result# (apply parse ~sql ~params)
            passed?# (= result# ~expected)]
        (do-report
          {:type (if passed?# :pass :fail)
           :message (str "\nTest " ~num " failed!"
                         "\nExpected: " ~expected
                         "\nActual:   " result#)
           :expected ~expected
           :actual result#})))))

(defmacro test-sql-meta
  [num sql params expected]
  `(testing (str "Test " ~num ": " ~sql)
     (let [result# (apply parse ~sql ~params)
           passed?# (= result# ~expected)
           meta-actual# (meta (:select result#))
           meta-expected# (meta (:select ~expected))]
       (do-report
         {:type (if passed?# :pass :fail)
          :message (str "\nTest " ~num " failed!"
                        "\nExpected: " ~expected
                        "\nActual:   " result#)
          :expected ~expected
          :actual result#})
       (when passed?#
         (let [meta-passed?# (= meta-actual# meta-expected#)]
           (do-report
             {:type (if meta-passed?# :pass :fail)
              :message (str "\nTest " ~num " metadata failed!"
                            "\nExpected meta: " meta-expected#
                            "\nActual meta:   " meta-actual#)
              :expected meta-expected#
              :actual meta-actual#}))))))

(deftest select-tests
  (testing "Basic SELECT queries"

    (test-sql 1
              "SELECT * FROM \"user\" WHERE \"user\".id = $1 LIMIT 100;"
              ["u-1"]
              {:select :*
               :from :user
               :where ^:pg/op [:= :user.id [:pg/param "u-1"]]
               :limit 100})

    (test-sql 2
              "SELECT * FROM \"user\" WHERE \"user\".id = $1 LIMIT 100"
              ["u'-1"]
              {:select :*
               :from :user
               :where ^:pg/op [:= :user.id [:pg/param "u'-1"]]
               :limit 100})

    (test-sql 3
              "SELECT a, b, c FROM \"user\""
              {:select {:a :a, :b :b, :c :c}
               :from :user})

    (test-sql 4
              "SELECT a, $1 b, c FROM \"user\""
              ["deleted"]
              {:select {:a :a
                        :b [:pg/param "deleted"]
                        :c :c}
               :from :user})

    (test-sql 5
              "SELECT a, $1 b, $2 c FROM \"user\""
              ["deleted" 9]
              {:select {:a :a
                        :b [:pg/param "deleted"]
                        :c [:pg/param 9]}
               :from :user})

    (test-sql 6
              "SELECT a, CURRENT_TIMESTAMP b, $1 c FROM \"user\""
              [9]
              {:select {:a :a
                        :b :CURRENT_TIMESTAMP
                        :c [:pg/param 9]}
               :from :user}))

  (testing "GROUP BY queries"

    (test-sql 7
              "SELECT * FROM patient GROUP BY name, same LIMIT 10"
              {:select :*
               :from :patient
               :group-by {:name :name, :same :same}
               :limit 10}))

  (testing "JOIN queries"

    (test-sql 8
              "SELECT count(*) FROM dft"
              []
              {:select {:count [:pg/count*]}
               :from :dft})

    (test-sql 9
              "SELECT count(*) as count FROM dft
               LEFT JOIN document d ON dft.id = d.resource ->> 'caseNumber' AND d.resource ->> 'name' = $1
               WHERE d.id is NULL"
              ["front"]
              {:select {:count [:pg/count*]}
               :from :dft
               :left-join {:d {:table :document
                               :on [:and
                                    ^:pg/op [:= :dft.id [:jsonb/->> :d.resource :caseNumber]]
                                    ^:pg/op [:= [:jsonb/->> :d.resource :name] [:pg/param "front"]]]}}
               :where ^:pg/op [:is :d.id nil]})
    )

  (testing "EXPLAIN queries"

    (test-sql 10
              "EXPLAIN ANALYZE
               SELECT count(*) as count
               FROM dft
               LEFT JOIN document d ON dft.id = d.resource ->> 'caseNumber' AND d.resource ->> 'name' = $1
               WHERE d.id is NULL"
              ["front"]
              {:explain {:analyze true}
               :select {:count [:pg/count*]}
               :from :dft
               :left-join {:d {:table :document
                               :on [:and
                                    ^:pg/op [:= :dft.id [:jsonb/->> :d.resource :caseNumber]]
                                    ^:pg/op [:= [:jsonb/->> :d.resource :name] [:pg/param "front"]]]}}
               :where ^:pg/op [:is :d.id nil]}))

  (testing "Complex WHERE clauses"

    (test-sql 11
              "SELECT id, resource
               FROM healthcareservices
               WHERE (
                 resource #>> '{name}' ILIKE $1
                 OR resource #>> '{type,0,coding,0,code}' ILIKE $1
                 OR resource #>> '{type,0,coding,1,code}' ILIKE $1)
               AND (
                 resource #>> '{name}' ILIKE $2 OR
                 resource #>> '{type,0,coding,0,code}' ILIKE $2 OR
                 resource #>> '{type,0,coding,1,code}' ILIKE $2
               )
               ORDER BY id"
              ["a" "b"]
              {:select {:id :id, :resource :resource}
               :from :healthcareservices
               :where [:and
                       [:or
                        [:ilike [:jsonb/#>> :resource [:name]] [:pg/param "a"]]
                        [:ilike [:jsonb/#>> :resource [:type 0 :coding 0 :code]] [:pg/param "a"]]
                        [:ilike [:jsonb/#>> :resource [:type 0 :coding 1 :code]] [:pg/param "a"]]]
                       [:or
                        [:ilike [:jsonb/#>> :resource [:name]] [:pg/param "b"]]
                        [:ilike [:jsonb/#>> :resource [:type 0 :coding 0 :code]] [:pg/param "b"]]
                        [:ilike [:jsonb/#>> :resource [:type 0 :coding 1 :code]] [:pg/param "b"]]]]
               :order-by :id})

    (test-sql 12
              "SELECT count(*) FROM oru WHERE ( resource #>> '{message,datetime}' )::timestamp > now() - interval '1 week' and id ILIKE $1"
              ["%Z%.CV"]
              {:select {:count [:pg/count*]}
               :from :oru
               :where [:and
                       [:>
                        [:pg/cast [:jsonb/#>> :resource [:message :datetime]] :pg_catalog.timestamp]
                        [:- [:now] [:pg/cast [:pg/sql "'1 week'"] :pg_catalog.interval]]]
                       [:ilike :id [:pg/param "%Z%.CV"]]]}))

  (testing "Function calls"

    (test-sql 13
              "SELECT p.resource || jsonb_build_object('id', p.id) as pr, resource || jsonb_build_object('id', id) as resource
               FROM oru
               LEFT JOIN practitioner p ON practitioner.id = p.resource #>> '{\"patient_group\", \"order_group\", 0, order, requester, provider, 0, identifier, value}'
               LEFT JOIN organization org ON organization.id = p.resource #>> '{\"patient_group\", \"order_group\", 0, order, contact, phone, 0, phone}'
               WHERE id ILIKE $1
               ORDER BY id
               LIMIT 5"
              ["%Z38886%"]
              {:select {:pr [:|| :p.resource ^:pg/fn [:jsonb_build_object [:pg/sql "'id'"] :p.id]]
                        :resource [:|| :resource ^:pg/fn [:jsonb_build_object [:pg/sql "'id'"] :id]]}
               :left-join {:org {:table :organization
                                 :on [:=
                                      :organization.id
                                      [:jsonb/#>> :p.resource [:patient_group :order_group 0 :order :contact :phone 0 :phone]]]}
                           :p {:table :practitioner
                               :on [:=
                                    :practitioner.id
                                    [:jsonb/#>>
                                     :p.resource
                                     [:patient_group :order_group 0 :order :requester :provider 0 :identifier :value]]]}}
               :from :oru
               :where [:ilike :id [:pg/param "%Z38886%"]]
               :order-by :id
               :limit 5})

    (test-sql 14
              "SELECT AVG(salary) as avg_sal FROM abc"
              {:select {:avg_sal [:avg :salary]}
               :from :abc}))

  (testing "DISTINCT queries"

    (test-sql 15
              "SELECT DISTINCT test FROM best"
              {:select-distinct {:test :test}
               :from :best})

    (test-sql 16
              "SELECT DISTINCT id as id , resource as resource , txid as txid FROM best"
              {:select-distinct {:id :id
                                 :resource :resource
                                 :txid :txid}
               :from :best}))

  (testing "DISTINCT ON queries"

    (test-sql-meta 17
                   "SELECT DISTINCT ON ( id , txid ) id as id , resource as resource , txid as txid FROM best"
                   []
                   {:select ^{:pg/projection {:distinct-on [:id :txid]}}
                            {:id :id
                             :resource :resource
                             :txid :txid}
                    :from :best})

    (test-sql-meta 18
                   "SELECT DISTINCT ON ( id ) id as id , resource as resource , txid as txid FROM best"
                   []
                   {:select ^{:pg/projection {:distinct-on [:id]}}
                            {:id :id
                             :resource :resource
                             :txid :txid}
                    :from :best})

    (test-sql-meta 19
                   "SELECT DISTINCT ON ( ( resource #>> '{id}' ) ) id as id , resource as resource , txid as txid FROM best"
                   []
                   {:select ^{:pg/projection {:distinct-on [[:jsonb/#>> :resource [:id]]]}}
                            {:id :id
                             :resource :resource
                             :txid :txid}
                    :from :best}))

  (testing "SELECT ALL"

    (test-sql 20
              "SELECT ALL id as id , resource as resource , txid as txid FROM best"
              {:select {:id :id
                        :resource :resource
                        :txid :txid}
               :from :best}))

  (testing "GROUP BY with DISTINCT"

    (test-sql 21
              "SELECT department_id, COUNT(DISTINCT job_title) as count_job_titles FROM employee GROUP BY department_id"
              {:select {:department_id :department_id
                        :count_job_titles [:count [:distinct [:pg/columns :job_title]]]}
               :from :employee
               :group-by {:department_id :department_id}})

    (test-sql 22
              "SELECT department_id, COUNT(DISTINCT job_title) FROM employee GROUP BY department_id"
              {:select {:department_id :department_id
                        :count [:count [:distinct [:pg/columns :job_title]]]}
               :from :employee
               :group-by {:department_id :department_id}}))

  (testing "VALUES clause"

    (test-sql 23
              "(VALUES (1, 'Alice'), (2, 'Grandma'), (3, 'Bob'))"
              {:ql/type :pg/values
               :keys [:k1 :k2]
               :values [{:k1 1 :k2 [:pg/sql "'Alice'"]}
                        {:k1 2 :k2 [:pg/sql "'Grandma'"]}
                        {:k1 3 :k2 [:pg/sql "'Bob'"]}]}))

  (testing "FETCH FIRST"

    (test-sql 24
              "SELECT * FROM employees FETCH FIRST 5 ROWS ONLY"
              {:select :*
               :from :employees
               :limit 5}))

  (testing "WITH clause (CTEs)"

    (test-sql 25
              "WITH recent_hires AS ( SELECT * FROM employees WHERE hire_date > CURRENT_DATE - INTERVAL '30 days')
               SELECT * FROM recent_hires"
              {:ql/type :pg/cte
               :with {:recent_hires {:select :*
                                     :from :employees
                                     :where [:> :hire_date
                                             [:- :CURRENT_DATE [:pg/cast [:pg/sql "'30 days'"] :pg_catalog.interval]]]}}
               :select {:select :*
                        :from :recent_hires}})

    (test-sql 26
              "WITH dept_avg AS (
                 SELECT department_id as dept_id, AVG(salary) as avg_sal
                 FROM employee
                 GROUP BY department_id
               ),
               high_earners AS (
                 SELECT *
                 FROM employee
                 WHERE salary > (SELECT AVG(salary) FROM dept_avg WHERE dept_avg.dept_id = employee.department_id)
               )
               SELECT * FROM high_earners"
              {:ql/type :pg/cte
               :with {:dept_avg
                      {:select
                       {:dept_id :department_id
                        :avg_sal ^:pg/fn [:avg :salary]}
                       :from :employee
                       :group-by {:department_id :department_id}}
                      :high_earners {:select :*
                                     :from :employee
                                     :where [:> :salary {:ql/type :pg/sub-select
                                                         :select ^:pg/fn [:avg :salary]
                                                         :from :dept_avg
                                                         :where [:= :dept_avg.dept_id :employee.department_id]}]}}
               :select {:select :*
                        :from :high_earners}}))

  (testing "Set operations"

    (test-sql 27
              "SELECT name FROM employees UNION SELECT name FROM contractors"
              {:select {:name :name}
               :from :employees
               :union {:contractors {:ql/type :pg/sub-select
                                     :select {:name :name}
                                     :from :contractors}}})

    (test-sql 28
              "SELECT name FROM employees INTERSECT SELECT name FROM contractors"
              {:select {:name :name}
               :from :employees
               :intersect {:contractors {:ql/type :pg/sub-select
                                         :select {:name :name}
                                         :from :contractors}}})

    (test-sql 29
              "SELECT name FROM employees EXCEPT SELECT name FROM contractors"
              {:select {:name :name}
               :from :employees
               :except {:contractors {:ql/type :pg/sub-select
                                      :select {:name :name}
                                      :from :contractors}}})

    (test-sql 30
              "SELECT name FROM employees UNION ALL SELECT name FROM contractors"
              {:select {:name :name}
               :from :employees
               :union-all {:contractors {:ql/type :pg/sub-select
                                         :select {:name :name}
                                         :from :contractors}}}))

  (testing "Additional edge cases"
    (test-sql 31
              "SELECT COUNT(*) as total, MAX(salary) as max_sal, MIN(salary) as min_sal FROM employees"
              {:select {:total [:pg/count*]
                        :max_sal [:max :salary]
                        :min_sal [:min :salary]}
               :from :employees})

    (test-sql 32
              "SELECT id, name AS employee_name FROM users WHERE active = true"
              {:select {:id :id, :employee_name :name}
               :from :users
               :where [:= :active true]})

    (test-sql 33
              "SELECT * FROM employees WHERE department_id IN (1, 2, 3)"
              {:select :*
               :from :employees
               :where [:in :department_id [:pg/list 1 2 3]]})

    (test-sql 34
              "SELECT * FROM employees WHERE name LIKE 'John%'"
              {:select :*
               :from :employees
               :where [:like :name :John%]})

    (test-sql 35
              "SELECT CAST(salary AS TEXT) as salary_text FROM employees"
              {:select {:salary_text [:pg/cast :salary :text]}
               :from :employees})

    (test-sql 36
              "SELECT EXISTS(SELECT a as a FROM employees WHERE salary > 100000) as has_high_earners"
              {:select {:has_high_earners [:exists {:ql/type :pg/sub-select
                                                    :select {:a :a}
                                                    :from :employees
                                                    :where [:> :salary 100000]}]}})))

(deftest create-table-tests
  (testing "CREATE TABLE queries"
    (test-sql 0
              "CREATE TABLE \"MyTable\" ( \"a\" integer )"
              {:ql/type :pg/create-table, :table-name :MyTable, :columns {:a ["pg_catalog.int4"]}})

    (test-sql 1
              "CREATE TABLE projects (project_id INT PRIMARY KEY,\n    start_date DATE,\n    end_date DATE\n)"
              {:ql/type :pg/create-table
               :table-name :projects
               :columns {:project_id ["pg_catalog.int4" "PRIMARY KEY"],
                         :start_date ["date"],
                         :end_date ["date"]}})

    (test-sql 2
              "CREATE TABLE IF NOT EXISTS mytable
              ( \"id\" text PRIMARY KEY , \"filelds\" jsonb , \"match_tags\" text[] , \"dedup_tags\" text[] )"
              {:ql/type :pg/create-table
               :table-name :mytable
               :if-not-exists true
               :columns {:id ["text" "PRIMARY KEY"],
                         :filelds ["jsonb"],
                         :match_tags ["text[]"],
                         :dedup_tags ["text[]"]}})

    (test-sql 3
              "CREATE TABLE IF NOT EXISTS patient_000 partition of patient ( PRIMARY KEY (\"id\", \"partition\"))
              for values from (0) to (1001) partition by range ( partition )"
              {:ql/type :pg/create-table,
               :table-name :patient_000,
               :constraint {:primary-key [:id :partition]}
               :if-not-exists true,
               :partition-of "patient",
               :partition-by {:method :range,
                              :expr :partition},
               :for {:from 0, :to 1001}})

    (test-sql 4
              "CREATE TABLE mytable ( PRIMARY KEY (\"id\", \"partition\") )"
              {:ql/type :pg/create-table,
               :table-name :mytable,
               :constraint {:primary-key [:id :partition]}})

    (test-sql 5
              "CREATE TABLE mytable
              ( \"id\" uuid , \"partition\" int , \"resource\" jsonb , PRIMARY KEY (\"id\", \"partition\") )"
              {:ql/type :pg/create-table,
               :table-name :mytable,
               :constraint {:primary-key [:id :partition]}
               :columns {:id ["uuid"],
                         :partition ["pg_catalog.int4"],
                         :resource ["jsonb"]}})

    (test-sql 6
              "CREATE TABLE mytable
              ( \"id\" uuid not null , \"version\" uuid not null ,
              \"cts\" timestamptz not null DEFAULT current_timestamp ,
              \"ts\" timestamptz not null DEFAULT current_timestamp ,
              \"status\" resource_status not null , \"partition\" int not null ,
              \"resource\" jsonb not null )"
              {:ql/type :pg/create-table,
               :table-name :mytable,
               :columns {:id ["uuid" "NOT NULL"],
                         :version ["uuid" "NOT NULL"],
                         :cts ["timestamptz" "NOT NULL" :DEFAULT :CURRENT_TIMESTAMP],
                         :ts ["timestamptz" "NOT NULL" :DEFAULT :CURRENT_TIMESTAMP],
                         :status ["resource_status" "NOT NULL"],
                         :partition ["pg_catalog.int4" "NOT NULL"],
                         :resource ["jsonb" "NOT NULL"]}})

    (test-sql 7
              "CREATE UNLOGGED TABLE IF NOT EXISTS mytable
              ( \"id\" text PRIMARY KEY , \"filelds\" jsonb , \"match_tags\" text[] , \"dedup_tags\" text[] )"
              {:ql/type :pg/create-table,
               :table-name :mytable,
               :if-not-exists true,
               :unlogged true,
               :columns {:id ["text" "PRIMARY KEY"],
                         :filelds ["jsonb"],
                         :match_tags ["text[]"],
                         :dedup_tags ["text[]"]}})

    (test-sql 8
              "CREATE TABLE mytable ( \"a\" integer NOT NULL DEFAULT 8 )"
              {:ql/type :pg/create-table
               :table-name :mytable
               :columns {:a ["pg_catalog.int4" "NOT NULL" :DEFAULT 8]}})

    (test-sql 9
              "CREATE TABLE IF NOT EXISTS part partition of whole for values from (0) to (400) partition by range ( partition )"
              {:ql/type :pg/create-table
               :table-name :part
               :if-not-exists true
               :partition-of "whole"
               :partition-by {:method :range :expr :partition}
               :for {:from 0 :to 400}})

    (test-sql 10
              "CREATE EXTENSION jsonknife"
              {:ql/type :pg/create-extension
               :name :jsonknife})

    (test-sql 11
              "CREATE EXTENSION IF NOT EXISTS jsonknife SCHEMA ext"
              {:ql/type :pg/create-extension
               :name :jsonknife
               :schema "ext"
               :if-not-exists true})

    (test-sql 12
              "CREATE TABLE table1 AS SELECT 1"
              {:ql/type :pg/create-table-as
               :table :table1
               :select {:ql/type :pg/select
                        :select 1}})
    (test-sql 13
              "CREATE UNIQUE INDEX IF NOT EXISTS sdl_src_dst ON sdl_src_dst ( ( src ) , ( dst ) )"
              {:ql/type :pg/index
               :index   :sdl_src_dst
               :on      :sdl_src_dst
               :if-not-exists true
               :unique  true
               :using :btree,
               :expr    [:src :dst]})

    (test-sql 14
              "CREATE INDEX IF NOT EXISTS users_id_idx ON users USING GIN ( ( resource-> 'a' ) , ( resource->'b' ) ) WHERE users.status = $1"
              ["active"]
              {:ql/type :pg/index,
               :index :users_id_idx,
               :on :users,
               :if-not-exists true,
               :using :gin,
               :expr [[:-> :resource :a] [:-> :resource :b]],
               :where [:= :users.status [:pg/param "active"]]})))

(deftest create-table-edge-cases
  (testing "CREATE TABLE edge cases"
    ;; Test 15: Multiple constraints on single column
    (test-sql 15
              "CREATE TABLE users ( id SERIAL PRIMARY KEY UNIQUE NOT NULL )"
              {:ql/type :pg/create-table
               :table-name :users
               :columns {:id ["serial" "PRIMARY KEY" "UNIQUE" "NOT NULL"]}})

    ;; Test 18: Column with custom type and array dimensions
    (test-sql 16
              "CREATE TABLE matrix ( data FLOAT8[3][3] )"
              {:ql/type :pg/create-table
               :table-name :matrix
               :columns {:data ["float8[3][3]"]}})

    ;; Test 19: TEMPORARY table
    (test-sql 17
              "CREATE TEMPORARY TABLE temp_data ( session_id TEXT )"
              {:ql/type :pg/create-table
               :table-name :temp_data
               :temporary true
               :columns {:session_id ["text"]}})

    ;; Test 21: Complex DEFAULT with function call
    (test-sql 18
              "CREATE TABLE logs ( created_at TIMESTAMPTZ DEFAULT NOW(), id UUID DEFAULT gen_random_uuid() )"
              {:ql/type :pg/create-table
               :table-name :logs
               :columns {:created_at ["timestamptz" :DEFAULT [:now]]
                         :id ["uuid" :DEFAULT [:gen_random_uuid]]}})

    ;; Test 22: SERIAL and BIGSERIAL types
    (test-sql 19
              "CREATE TABLE sequences ( small_id SERIAL, big_id BIGSERIAL )"
              {:ql/type :pg/create-table
               :table-name :sequences
               :columns {:small_id ["serial"]
                         :big_id ["bigserial"]}})

    ;; Test 24: ENUM type usage
    (test-sql 20
              "CREATE TABLE orders ( status order_status DEFAULT 'pending' )"
              {:ql/type :pg/create-table
               :table-name :orders
               :columns {:status ["order_status" :DEFAULT :pending]}})

    ;; Test 27: Column with COLLATE
    (test-sql 21
              "CREATE TABLE texts ( content TEXT COLLATE \"en_US.UTF-8\" )"
              {:ql/type :pg/create-table
               :table-name :texts
               :columns {:content ["text" "COLLATE" "en_US.UTF-8"]}})))


(deftest create-index-edge-cases
  (testing "CREATE INDEX edge cases"

    ;; Test 31: Partial index with complex WHERE clause
    (test-sql 22
              "CREATE INDEX active_users_idx ON users (last_login) WHERE status = 'active' AND last_login > '2023-01-01'"
              {:ql/type :pg/index
               :index :active_users_idx
               :on :users
               :using :btree
               :expr [:last_login]
               :where [:and
                       [:= :status :active]
                       [:> :last_login :2023-01-01]]})

    ;; Test 32: Expression index with function
    (test-sql 23
              "CREATE INDEX lower_email_idx ON users (LOWER(email))"
              {:ql/type :pg/index
               :index :lower_email_idx
               :on :users
               :using :btree
               :expr [[:pg/call :lower :email]]})

    ;; Test 36: GiST index for geometric data
    (test-sql 24
              "CREATE INDEX spatial_idx ON locations USING GIST (coordinates)"
              {:ql/type :pg/index
               :index :spatial_idx
               :on :locations
               :using :gist
               :expr [:coordinates]})))

(deftest create-extension-edge-cases
  (testing "CREATE EXTENSION edge cases"

    ;; Test 38: Extension with version
    (test-sql 25
              "CREATE EXTENSION postgis VERSION '3.1.0'"
              {:ql/type :pg/create-extension
               :name :postgis
               :version "3.1.0"})

    ;; Test 40: Extension with CASCADE
    (test-sql 26
              "CREATE EXTENSION IF NOT EXISTS postgis CASCADE"
              {:ql/type :pg/create-extension
               :name :postgis
               :if-not-exists true
               :cascade true})))

(deftest insert-tests
  (testing "Basic INSERT queries"

    (test-sql 1
              "INSERT INTO patient (id, gender) VALUES ('id-123', 'male')"
              {:ql/type :pg/insert
               :into :patient
               :value {:id :id-123
                       :gender :male}})

    (test-sql 2
              "INSERT INTO patient (id, gender) VALUES ($1, $2)"
              ["id-123" "male"]
              {:ql/type :pg/insert
               :into :patient
               :value {:id [:pg/param "id-123"]
                       :gender [:pg/param "male"]}})

    (test-sql 3
              "INSERT INTO patient AS p (id, gender) VALUES ($1, $2)"
              ["id-123" "male"]
              {:ql/type :pg/insert
               :into :patient
               :as :p
               :value {:id [:pg/param "id-123"]
                       :gender [:pg/param "male"]}})

    (test-sql 4
              "INSERT INTO patient (id, gender) VALUES ($1, $2) RETURNING *"
              ["id-123" "male"]
              {:ql/type :pg/insert
               :into :patient
               :value {:id [:pg/param "id-123"]
                       :gender [:pg/param "male"]}
               :returning [:pg/columns :*]})

    (test-sql 5
              "INSERT INTO patient AS p (id, gender) VALUES ($1, $2) RETURNING id, gender"
              ["id-123" "male"]
              {:ql/type :pg/insert
               :into :patient
               :as :p
               :value {:id [:pg/param "id-123"]
                       :gender [:pg/param "male"]}
               :returning [:pg/columns :id :gender]})

    (test-sql 6.1
              "INSERT INTO patient AS p (id, gender)
              VALUES ($1, $2) RETURNING jsonb_strip_nulls(CAST(row_to_json(p.*) AS jsonb)) AS row"
              ["id-123" "male"]
              {:ql/type :pg/insert
               :into :patient
               :as :p
               :value {:id [:pg/param "id-123"]
                       :gender [:pg/param "male"]}
               :returning [:as ^:pg/fn [:jsonb_strip_nulls [:pg/cast ^:pg/fn[:row_to_json :p.] :jsonb]] :row]})

    (test-sql 6.2
              "INSERT INTO patient AS p ( \"id\", \"gender\" ) VALUES ( $1 , $2 ) RETURNING jsonb_strip_nulls( ( row_to_json( p.* ) )::jsonb )"
              ["id" "male"]
              {:ql/type :pg/insert
               :into :patient
               :as :p
               :value {:id [:pg/param "id"]
                       :gender [:pg/param "male"]}
               :returning ^:pg/fn [:jsonb_strip_nulls [:pg/cast ^:pg/fn [:row_to_json :p.] :jsonb]]})

    (test-sql 7
              "INSERT INTO healthplan (a, b, c) VALUES (1, 'x', $1) RETURNING *"
              ["str"]
              {:ql/type :pg/insert
               :into :healthplan
               :value {:a 1
                       :b :x
                       :c [:pg/param "str"]}
               :returning [:pg/columns :*]})

    (test-sql 8
              "INSERT INTO healthplan (a, b, c, birthDate) VALUES (1, 'x', $1, $2) RETURNING *"
              ["str" "1991-11-08"]
              {:ql/type :pg/insert
               :into :healthplan
               :value {:a 1, :b :x, :c [:pg/param "str"], :birthdate [:pg/param "1991-11-08"]}
               :returning [:pg/columns :*]}))


  (testing "INSERT with ON CONFLICT"

    (test-sql 9
              "INSERT INTO healthplan (a, b, c)
              VALUES (1, 'x', $1) ON CONFLICT (id) DO UPDATE SET a = excluded.a WHERE 1 = 2 RETURNING *"
              ["str"]
              {:ql/type :pg/insert
               :into :healthplan
               :value {:a 1
                       :b :x
                       :c [:pg/param "str"]}
               :on-conflict {:on [:id]
                             :do {:set {:a :excluded.a}
                                  :where [:= 1 2]}}
               :returning [:pg/columns :*]})

    (test-sql 10
              "INSERT INTO mytable (id, name) VALUES ($1, $2) ON CONFLICT (id) DO NOTHING"
              ["123" "test"]
              {:ql/type :pg/insert
               :into :mytable
               :value {:id [:pg/param "123"]
                       :name [:pg/param "test"]}
               :on-conflict {:on [:id]
                             :do :nothing}}))

  (testing "INSERT SELECT queries"

    (test-sql 11
              "INSERT INTO mytable SELECT * FROM t"
              {:ql/type :pg/insert-select
               :into :mytable
               :select {:select :*
                        :from :t}})

    (test-sql 12
              "INSERT INTO mytable (a, b, z) SELECT 'a' AS a, b AS b, z AS z FROM t"
              {:ql/type :pg/insert-select
               :into :mytable
               :select {:select {:a :a
                                 :b :b
                                 :z :z}
                        :from :t}})

    (test-sql 13
              "INSERT INTO mytable (a, b, z) SELECT $1 AS a, b AS b, z AS z FROM t"
              ["a"]
              {:ql/type :pg/insert-select
               :into :mytable
               :select {:select {:a [:pg/param "a"]
                                 :b :b
                                 :z :z}
                        :from :t}})

    (test-sql 14
              "INSERT INTO mytable (a, b, z)
              SELECT $1 AS a, b AS b, z AS z FROM t ON CONFLICT (id) DO UPDATE SET a = excluded.a WHERE 1 = 2"
              ["a"]
              {:ql/type :pg/insert-select
               :into :mytable
               :select {:select {:a [:pg/param "a"]
                                 :b :b
                                 :z :z}
                        :from :t}
               :on-conflict {:on [:id]
                             :do {:set {:a :excluded.a}
                                  :where [:= 1 2]}}})

    (test-sql 15
              "INSERT INTO mytable (a, b, z) SELECT $1 AS a, b AS b, z AS z FROM t ON CONFLICT (id) DO NOTHING"
              ["a"]
              {:ql/type :pg/insert-select
               :into :mytable
               :select {:select {:a [:pg/param "a"]
                                 :b :b
                                 :z :z}
                        :from :t}
               :on-conflict {:on [:id]
                             :do :nothing}})

    (test-sql 16
              "INSERT INTO mytable (a, b, z) SELECT $1 AS a, b AS b, z AS z FROM t RETURNING *"
              ["a"]
              {:ql/type :pg/insert-select
               :into :mytable
               :select {:select {:a [:pg/param "a"]
                                 :b :b
                                 :z :z}
                        :from :t}
               :returning [:pg/columns :*]}))

  (testing "INSERT with multiple values"

    (test-sql 17
              "INSERT INTO conceptmaprule (id, txid, resource, status) VALUES (1, NULL, $1, $2), (2, NULL, NULL, $3) RETURNING *"
              ["1" "ready" "failure"]
              {:ql/type :pg/insert-many
               :into :conceptmaprule
               :values {:keys [:id :txid :resource :status]
                        :values [{:id 1
                                  :txid nil
                                  :resource [:pg/param "1"]
                                  :status [:pg/param "ready"]}
                                 {:id 2
                                  :txid nil
                                  :resource nil
                                  :status [:pg/param "failure"]}]}
               :returning [:pg/columns :*]})

    (test-sql 18
              "INSERT INTO users (id, name, active) VALUES (1, 'Alice', true), (2, 'Bob', false)"
              {:ql/type :pg/insert-many
               :into :users
               :values {:keys [:id :name :active]
                        :values [{:id 1
                                  :name :Alice
                                  :active true}
                                 {:id 2
                                  :name :Bob
                                  :active false}]}}))

  (testing "Complex INSERT queries"

    (test-sql 19
              "INSERT INTO patient (id, resource) VALUES ($1, jsonb_build_object('name', $2, 'gender', $3))"
              ["id-123" "John Doe" "male"]
              {:ql/type :pg/insert
               :into :patient
               :value {:id [:pg/param "id-123"]
                       :resource ^:pg/fn[:jsonb_build_object
                                         [:pg/sql "'name'"]
                                         [:pg/param "John Doe"]
                                         [:pg/sql "'gender'"]
                                         [:pg/param "male"]]}})

    (test-sql 20
              "INSERT INTO logs (id, created_at) VALUES (gen_random_uuid(), NOW())"
              {:ql/type :pg/insert
               :into :logs
               :value {:id [:gen_random_uuid]
                       :created_at [:now]}})

    (test-sql 21
              "INSERT INTO healthplan (id, resource) VALUES ($1, resource || jsonb_build_object('status', $2))"
              ["hp-1" "active"]
              {:ql/type :pg/insert
               :into :healthplan
               :value {:id [:pg/param "hp-1"]
                       :resource [:|| :resource ^:pg/fn[:jsonb_build_object
                                                        [:pg/sql "'status'"]
                                                        [:pg/param "active"]]]}})

    (test-sql 22
              "INSERT INTO \"MyTable\" (\"id\", \"name\") VALUES ($1, $2)"
              ["123" "test"]
              {:ql/type :pg/insert
               :into :MyTable
               :value {:id [:pg/param "123"]
                       :name [:pg/param "test"]}})

    (test-sql 23
              "INSERT INTO patient (id, metadata) VALUES ($1, $2::jsonb)"
              ["p-1" "{\"foo\":\"bar\"}"]
              {:ql/type :pg/insert
               :into :patient
               :value {:id [:pg/param "p-1"]
                       :metadata [:pg/cast [:pg/param "{\"foo\":\"bar\"}"] :jsonb]}})))

(deftest insert-edge-case-tests
  (testing "INSERT edge cases"

    (test-sql 24
              "INSERT INTO users (id, name, created_at) VALUES ($1, $2, DEFAULT)"
              ["123" "Alice"]
              {:ql/type :pg/insert
               :into :users
               :value {:id [:pg/param "123"]
                       :name [:pg/param "Alice"]
                       :created_at :DEFAULT}})

    (test-sql 25
              "INSERT INTO logs (id, created_at, updated_at) VALUES (DEFAULT, DEFAULT, DEFAULT)"
              {:ql/type :pg/insert
               :into :logs
               :value {:id :DEFAULT
                       :created_at :DEFAULT
                       :updated_at :DEFAULT}})

    (test-sql 26
              "INSERT INTO events (id, name, event_time) VALUES ($1, $2, CURRENT_TIMESTAMP)"
              ["evt-1" "Login"]
              {:ql/type :pg/insert
               :into :events
               :value {:id [:pg/param "evt-1"]
                       :name [:pg/param "Login"]
                       :event_time :CURRENT_TIMESTAMP}})

    (test-sql 27
              "INSERT INTO schedules (id, schedule_date, schedule_time) VALUES ($1, CURRENT_DATE, CURRENT_TIME)"
              ["sch-1"]
              {:ql/type :pg/insert
               :into :schedules
               :value {:id [:pg/param "sch-1"]
                       :schedule_date :CURRENT_DATE
                       :schedule_time :CURRENT_TIME}})

    (test-sql 28
              "INSERT INTO patients (id, name, middle_name, deceased_date) VALUES ($1, $2, NULL, NULL)"
              ["p-1" "John Doe"]
              {:ql/type :pg/insert
               :into :patients
               :value {:id [:pg/param "p-1"]
                       :name [:pg/param "John Doe"]
                       :middle_name nil
                       :deceased_date nil}})

    (test-sql 29
              "INSERT INTO tags_table (id, tags) VALUES ($1, ARRAY['tag1', 'tag2', 'tag3'])"
              ["123"]
              {:ql/type :pg/insert
               :into :tags_table
               :value {:id [:pg/param "123"]
                       :tags [:pg/array [:tag1 :tag2 :tag3]]}})

    (test-sql 30
              "INSERT INTO documents (id, data) VALUES ($1, jsonb_build_object('items', jsonb_build_array($2, $3, $4)))"
              ["doc-1" "item1" "item2" "item3"]
              {:ql/type :pg/insert
               :into :documents
               :value {:id [:pg/param "doc-1"]
                       :data ^:pg/fn[:jsonb_build_object
                                     [:pg/sql "'items'"]
                                     ^:pg/fn[:jsonb_build_array
                                             [:pg/param "item1"]
                                             [:pg/param "item2"]
                                             [:pg/param "item3"]]]}})

    (test-sql 31
              "INSERT INTO summary (id, total_count) VALUES ($1, (SELECT COUNT(*) FROM users WHERE active = true))"
              ["sum-1"]
              {:ql/type :pg/insert
               :into :summary
               :value {:id [:pg/param "sum-1"]
                       :total_count {:ql/type :pg/sub-select
                                     :select [:pg/count*]
                                     :from :users
                                     :where [:= :active true]}}})

    (test-sql 32
              "INSERT INTO user_roles (user_id, role_id, assigned_at) VALUES ($1, $2, $3) ON CONFLICT (user_id, role_id) DO UPDATE SET assigned_at = EXCLUDED.assigned_at"
              ["u-1" "r-1" "2023-01-01"]
              {:ql/type :pg/insert
               :into :user_roles
               :value {:user_id [:pg/param "u-1"]
                       :role_id [:pg/param "r-1"]
                       :assigned_at [:pg/param "2023-01-01"]}
               :on-conflict {:on [:user_id :role_id]
                             :do {:set {:assigned_at :excluded.assigned_at}}}})

    (test-sql 33
              "INSERT INTO \"user\" (\"id\", \"from\", \"to\", \"select\") VALUES ($1, $2, $3, $4)"
              ["123" "NYC" "LA" "first"]
              {:ql/type :pg/insert
               :into :user
               :value {:id [:pg/param "123"]
                       :from [:pg/param "NYC"]
                       :to [:pg/param "LA"]
                       :select [:pg/param "first"]}})

    (test-sql 34
              "INSERT INTO public.users (id, name) VALUES ($1, $2)"
              ["u-1" "Alice"]
              {:ql/type :pg/insert
               :into :public.users
               :value {:id [:pg/param "u-1"]
                       :name [:pg/param "Alice"]}})

    (test-sql 35
              "INSERT INTO schedules (id, name, duration) VALUES ($1, $2, INTERVAL '1 hour 30 minutes')"
              ["sch-1" "Meeting"]
              {:ql/type :pg/insert
               :into :schedules
               :value {:id [:pg/param "sch-1"]
                       :name [:pg/param "Meeting"]
                       :duration [:pg/cast [:pg/sql "'1 hour 30 minutes'"] :pg_catalog.interval]}})

    (test-sql 36
              "INSERT INTO feature_flags (id, name, enabled) VALUES ($1, $2, $3 = 'true')"
              ["ff-1" "new_ui" "true"]
              {:ql/type :pg/insert
               :into :feature_flags
               :value {:id [:pg/param "ff-1"]
                       :name [:pg/param "new_ui"]
                       :enabled [:= [:pg/param "true"] :true]}})

    (test-sql 37
              "INSERT INTO users (id, name, status) VALUES ($1, $2, CASE WHEN $3 > 18 THEN 'adult' ELSE 'minor' END)"
              ["u-1" "John" 25]
              {:ql/type :pg/insert
               :into :users
               :value {:id [:pg/param "u-1"]
                       :name [:pg/param "John"]
                       :status [:cond
                                [:> [:pg/param 25] 18] :adult
                                :minor]}})

    (test-sql 38
              "INSERT INTO products (id, price, tax, total) VALUES ($1, $2, $3, $2 * (1 + $3))"
              ["prod-1" 100 0.1]
              {:ql/type :pg/insert
               :into :products
               :value {:id [:pg/param "prod-1"]
                       :price [:pg/param 100]
                       :tax [:pg/param 0.1]
                       :total [:* [:pg/param 100] [:+ 1 [:pg/param 0.1]]]}})

    (test-sql 39
              "INSERT INTO sessions (id, user_id, token) VALUES (uuid_generate_v4(), $1, encode(gen_random_bytes(32), 'hex'))"
              ["user-123"]
              {:ql/type :pg/insert
               :into :sessions
               :value {:id ^:pg/fn [:uuid_generate_v4]
                       :user_id [:pg/param "user-123"]
                       :token ^:pg/fn [:encode ^:pg/fn [:gen_random_bytes 32] [:pg/sql "'hex'"]]}})

    (test-sql 40
              "INSERT INTO inventory (product_id, quantity, updated_at) VALUES ($1, $2, NOW()) ON CONFLICT (product_id) DO UPDATE SET quantity = inventory.quantity + EXCLUDED.quantity, updated_at = NOW() WHERE inventory.quantity + EXCLUDED.quantity > 0"
              ["prod-1" 5]
              {:ql/type :pg/insert
               :into :inventory
               :value {:product_id [:pg/param "prod-1"]
                       :quantity [:pg/param 5]
                       :updated_at [:now]}
               :on-conflict {:on [:product_id]
                             :do {:set {:quantity [:+ :inventory.quantity :excluded.quantity]
                                        :updated_at [:now]}
                                  :where [:> [:+ :inventory.quantity :excluded.quantity] 0]}}})

    (test-sql 41
              "INSERT INTO big_table (col1, col2, col3, col4, col5, col6, col7, col8, col9, col10) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)"
              ["v1" "v2" "v3" "v4" "v5" "v6" "v7" "v8" "v9" "v10"]
              {:ql/type :pg/insert
               :into :big_table
               :value {:col1 [:pg/param "v1"]
                       :col2 [:pg/param "v2"]
                       :col3 [:pg/param "v3"]
                       :col4 [:pg/param "v4"]
                       :col5 [:pg/param "v5"]
                       :col6 [:pg/param "v6"]
                       :col7 [:pg/param "v7"]
                       :col8 [:pg/param "v8"]
                       :col9 [:pg/param "v9"]
                       :col10 [:pg/param "v10"]}}))

(testing "INSERT with WITH clause"
  (test-sql 42
            "WITH active_users AS (SELECT id FROM users WHERE active = true) INSERT INTO user_stats (user_id, calculated_at) SELECT id as user_id, NOW() as calculated_at FROM active_users"
            {:ql/type :pg/cte
             :with {:active_users {:select {:id :id}
                                   :from :users
                                   :where [:= :active true]}}
             :insert {:ql/type :pg/insert-select
                      :into :user_stats
                      :select {:select {:user_id :id
                                        :calculated_at [:now]}
                               :from :active_users}}})))

(deftest update-tests
  (testing "Basic UPDATE queries"

    (test-sql 1
              "UPDATE healthplan SET a = tbl.b FROM some_table tbl WHERE healthplan.id = $1"
              ["ups"]
              {:ql/type :pg/update
               :update :healthplan
               :set {:a :tbl.b}
               :from {:tbl :some_table}
               :where [:= :healthplan.id [:pg/param "ups"]]})

    (test-sql 2
              "UPDATE healthplan SET a = tbl.b FROM some_table tbl WHERE healthplan.id = $1 RETURNING *"
              ["ups"]
              {:ql/type :pg/update
               :update :healthplan
               :set {:a :tbl.b}
               :from {:tbl :some_table}
               :where [:= :healthplan.id [:pg/param "ups"]]
               :returning [:pg/columns :*]})

    (test-sql 3
              "UPDATE healthplan SET resource = resource || jsonb_build_object('status', $1) WHERE id = $2"
              ["some-val" "some-id"]
              {:ql/type :pg/update
               :update :healthplan
               :set {:resource ^:pg/op[:|| :resource ^:pg/fn[:jsonb_build_object [:pg/sql "'status'"] [:pg/param "some-val"]]]}
               :where ^:pg/op[:= :id [:pg/param "some-id"]]})

    (test-sql 4
              "UPDATE ORU SET resource = resource || jsonb_build_object('status', $1) WHERE id = $2"
              ["some-val" "some-id"]
              {:ql/type :pg/update
               :update :oru
               :set {:resource ^:pg/op[:|| :resource ^:pg/fn[:jsonb_build_object [:pg/sql "'status'"] [:pg/param "some-val"]]]}
               :where ^:pg/op[:= :id [:pg/param "some-id"]]})

    (test-sql 5
              "UPDATE AmdExport SET resource = resource || jsonb_build_object('status', $1) WHERE resource->'status' IS NULL"
              ["pending"]
              {:ql/type :pg/update
               :update :amdexport
               :set {:resource ^:pg/op[:|| :resource ^:pg/fn[:jsonb_build_object [:pg/sql "'status'"] [:pg/param "pending"]]]}
               :where ^:pg/op[:is [:-> :resource :status] nil]})

    (test-sql 6
              "UPDATE Document SET resource = resource || jsonb_build_object('caseNumber', $1) WHERE id = $2"
              ["new-case-id" "doc-id"]
              {:ql/type :pg/update
               :update :document
               :set {:resource ^:pg/op[:|| :resource ^:pg/fn[:jsonb_build_object [:pg/sql "'caseNumber'"] [:pg/param "new-case-id"]]]}
               :where ^:pg/op[:= :id [:pg/param "doc-id"]]})

    (test-sql 7
              "UPDATE BillingCase SET resource = resource || jsonb_build_object('report_no', id || $1) WHERE id ILIKE $2"
              [".CV" "%Z%"]
              {:ql/type :pg/update
               :update :billingcase
               :set {:resource ^:pg/op[:|| :resource ^:pg/fn[:jsonb_build_object [:pg/sql "'report_no'"] ^:pg/op[:|| :id [:pg/param ".CV"]]]]}
               :where [:ilike :id [:pg/param "%Z%"]]}))

  (testing "Complex UPDATE queries"

    (test-sql 8
              "UPDATE xinvoice SET resource = resource || jsonb_build_object('status', $1, 'history', resource #>> '{history}' || jsonb_build_array(jsonb_build_object('status', $2, 'user', jsonb_build_object('id', $3), 'date', now()))) RETURNING id"
              ["sent" "sent" "id"]
              {:ql/type :pg/update
               :update :xinvoice
               :set {:resource ^:pg/op[:|| :resource
                                       ^:pg/fn[:jsonb_build_object
                                               [:pg/sql "'status'"] [:pg/param "sent"]
                                               [:pg/sql "'history'"] ^:pg/op[:|| [:jsonb/#>> :resource [:history]]
                                                                             ^:pg/fn[:jsonb_build_array
                                                                                     ^:pg/fn[:jsonb_build_object
                                                                                             [:pg/sql "'status'"] [:pg/param "sent"]
                                                                                             [:pg/sql "'user'"] ^:pg/fn[:jsonb_build_object [:pg/sql "'id'"] [:pg/param "id"]]
                                                                                             [:pg/sql "'date'"] [:now]]]]]]}
               :returning [:pg/columns :id]})

    (test-sql 9
              "UPDATE healthplan
               SET resource = resource || jsonb_build_object('identifier', jsonb_build_array(
                 jsonb_build_object('value', resource #>> '{\"clearing_house\",\"advance_md\",\"payer-id\"}', 'system', $1),
                 jsonb_build_object('value', resource #>> '{\"clearing_house\",\"change_healthcare\",\"payer-id\"}', 'system', $2),
                 jsonb_build_object('value', resource #>> '{\"clearing_house\",omega,\"payer-id\"}', 'system', $3)))
               WHERE resource -> 'clearing_house' IS NOT NULL"
              ["amd" "change_healthcare" "omega"]
              {:ql/type :pg/update
               :update :healthplan
               :set {:resource ^:pg/op[:|| :resource
                                       ^:pg/fn[:jsonb_build_object
                                               [:pg/sql "'identifier'"]
                                               ^:pg/fn[:jsonb_build_array
                                                       ^:pg/fn[:jsonb_build_object
                                                               [:pg/sql "'value'"] [:jsonb/#>> :resource [:clearing_house :advance_md :payer-id]]
                                                               [:pg/sql "'system'"] [:pg/param "amd"]]
                                                       ^:pg/fn[:jsonb_build_object
                                                               [:pg/sql "'value'"] [:jsonb/#>> :resource [:clearing_house :change_healthcare :payer-id]]
                                                               [:pg/sql "'system'"] [:pg/param "change_healthcare"]]
                                                       ^:pg/fn[:jsonb_build_object
                                                               [:pg/sql "'value'"] [:jsonb/#>> :resource [:clearing_house :omega :payer-id]]
                                                               [:pg/sql "'system'"] [:pg/param "omega"]]]]]}
               :where ^:pg/op[:is [:-> :resource :clearing_house] [:pg/sql "NOT NULL"]]}))

  (testing "UPDATE with multiple columns"

    (test-sql 10
              "UPDATE employees SET name = $1, salary = $2, updated_at = NOW() WHERE id = $3"
              ["John Doe" 75000 "emp-123"]
              {:ql/type :pg/update
               :update :employees
               :set {:name [:pg/param "John Doe"]
                     :salary [:pg/param 75000]
                     :updated_at [:now]}
               :where [:= :id [:pg/param "emp-123"]]})

    (test-sql 11
              "UPDATE users SET status = 'active', verified = true WHERE email = $1"
              ["user@example.com"]
              {:ql/type :pg/update
               :update :users
               :set {:status :active
                     :verified true}
               :where [:= :email [:pg/param "user@example.com"]]})

    (test-sql 12
              "UPDATE inventory SET quantity = quantity + $1 WHERE product_id = $2 AND quantity + $1 > 0"
              [10 "prod-456"]
              {:ql/type :pg/update
               :update :inventory
               :set {:quantity [:+ :quantity [:pg/param 10]]}
               :where [:and
                       [:= :product_id [:pg/param "prod-456"]]
                       [:> [:+ :quantity [:pg/param 10]] 0]]}))

  (testing "UPDATE with subqueries"

    (test-sql 13
              "UPDATE products SET price = (SELECT AVG(price) FROM products WHERE category = 'electronics') WHERE category = 'electronics' AND price IS NULL"
              {:ql/type :pg/update
               :update :products
               :set {:price {:ql/type :pg/sub-select
                             :select [:avg :price]
                             :from :products
                             :where [:= :category :electronics]}}
               :where [:and
                       [:= :category :electronics]
                       [:is :price nil]]})

    (test-sql 14
              "UPDATE users SET last_order_date = (SELECT MAX(created_at) FROM orders WHERE orders.user_id = users.id) WHERE active = true"
              {:ql/type :pg/update
               :update :users
               :set {:last_order_date {:ql/type :pg/sub-select
                                       :select [:max :created_at]
                                       :from :orders
                                       :where [:= :orders.user_id :users.id]}}
               :where [:= :active true]}))

  (testing "UPDATE with CASE expressions"

    (test-sql 15
              "UPDATE products SET status = CASE WHEN quantity > 100 THEN 'in_stock' WHEN quantity > 0 THEN 'low_stock' ELSE 'out_of_stock' END"
              {:ql/type :pg/update
               :update :products
               :set {:status [:cond
                              [:> :quantity 100] :in_stock
                              [:> :quantity 0] :low_stock
                              :out_of_stock]}})

    (test-sql 16
              "UPDATE employees SET bonus = CASE department WHEN 'sales' THEN salary * 0.1 WHEN 'engineering' THEN salary * 0.15 ELSE salary * 0.05 END"
              {:ql/type :pg/update
               :update :employees
               :set {:bonus [:case :department
                             :sales [:* :salary 0.1]
                             :engineering [:* :salary 0.15]
                             [:* :salary 0.05]]}})

    (test-sql 17
              "UPDATE orders SET discount = CASE WHEN total > 1000 THEN 0.2 WHEN total > 500 THEN 0.1 ELSE 0 END WHERE created_at > $1"
              ["2023-01-01"]
              {:ql/type :pg/update
               :update :orders
               :set {:discount [:cond
                                [:> :total 1000] 0.2
                                [:> :total 500] 0.1
                                0]}
               :where [:> :created_at [:pg/param "2023-01-01"]]}))

  (testing "UPDATE with JSONB operations"

    (test-sql 18
              "UPDATE users SET preferences = jsonb_set(preferences, '{notifications,email}', $1) WHERE id = $2"
              ["true" "user-123"]
              {:ql/type :pg/update
               :update :users
               :set {:preferences [:pg/jsonb_set :preferences [:notifications :email] [:pg/param "true"]]}
               :where [:= :id [:pg/param "user-123"]]})

    (test-sql 19
              "UPDATE products SET metadata = metadata || $1::jsonb WHERE id = $2"
              ["{\"featured\":true}" "prod-789"]
              {:ql/type :pg/update
               :update :products
               :set {:metadata [:|| :metadata [:pg/cast [:pg/param "{\"featured\":true}"] :jsonb]]}
               :where [:= :id [:pg/param "prod-789"]]})

    (test-sql 20
              "UPDATE settings SET config = jsonb_strip_nulls(config || jsonb_build_object('theme', $1, 'language', $2)) WHERE user_id = $3"
              ["dark" "en" "user-456"]
              {:ql/type :pg/update
               :update :settings
               :set {:config ^:pg/fn[:jsonb_strip_nulls
                              [:|| :config
                               ^:pg/fn[:jsonb_build_object
                                       [:pg/sql "'theme'"] [:pg/param "dark"]
                                       [:pg/sql "'language'"] [:pg/param "en"]]]]}
               :where [:= :user_id [:pg/param "user-456"]]}))

  (testing "UPDATE with complex WHERE clauses"

    (test-sql 21
              "UPDATE tasks SET status = 'overdue' WHERE due_date < CURRENT_DATE AND status NOT IN ('completed', 'cancelled')"
              {:ql/type :pg/update
               :update :tasks
               :set {:status :overdue}
               :where [:and
                       [:< :due_date :CURRENT_DATE]
                       [:not-in :status [:pg/list :completed :cancelled]]]})

    (test-sql 22
              "UPDATE employees SET department_id = $1 WHERE department_id IN (SELECT id FROM departments WHERE name = $2)"
              [10 "Old Department"]
              {:ql/type :pg/update
               :update :employees
               :set {:department_id [:pg/param 10]}
               :where [:in :department_id
                       {:ql/type :pg/sub-select
                        :select :id
                        :from :departments
                        :where [:= :name [:pg/param "Old Department"]]}]})

    (test-sql 23
              "UPDATE products SET price = price * 0.9 WHERE category_id = $1 AND (price > 100 OR quantity < 10)"
              [5]
              {:ql/type :pg/update
               :update :products
               :set {:price [:* :price 0.9]}
               :where [:and
                       [:= :category_id [:pg/param 5]]
                       [:or
                        [:> :price 100]
                        [:< :quantity 10]]]}))

  (testing "UPDATE with table aliases and schema"

    (test-sql 24
              "UPDATE public.users SET modified_at = NOW() WHERE id = $1"
              ["user-999"]
              {:ql/type :pg/update
               :update :public.users
               :set {:modified_at [:now]}
               :where [:= :id [:pg/param "user-999"]]})

    (test-sql 25
              "UPDATE \"MyTable\" SET \"MyColumn\" = $1 WHERE \"ID\" = $2"
              ["new value" 123]
              {:ql/type :pg/update
               :update :MyTable
               :set {:MyColumn [:pg/param "new value"]}
               :where [:= :ID [:pg/param 123]]})))

(deftest update-edge-case-tests
  (testing "UPDATE with DEFAULT values"

    (test-sql 26
              "UPDATE users SET created_at = DEFAULT, status = DEFAULT WHERE id = $1"
              ["user-123"]
              {:ql/type :pg/update
               :update :users
               :set {:created_at :DEFAULT
                     :status :DEFAULT}
               :where [:= :id [:pg/param "user-123"]]})

    (test-sql 27
              "UPDATE products SET price = DEFAULT, updated_at = CURRENT_TIMESTAMP WHERE sku = $1"
              ["SKU-789"]
              {:ql/type :pg/update
               :update :products
               :set {:price :DEFAULT
                     :updated_at :CURRENT_TIMESTAMP}
               :where [:= :sku [:pg/param "SKU-789"]]}))

  (testing "UPDATE with array operations"

    (test-sql 28
              "UPDATE users SET tags = ARRAY['admin', 'verified'] WHERE id = $1"
              ["user-456"]
              {:ql/type :pg/update
               :update :users
               :set {:tags [:pg/array [:admin :verified]]}
               :where [:= :id [:pg/param "user-456"]]})

    (test-sql 29
              "UPDATE products SET categories = categories || ARRAY[$1, $2] WHERE id = $3"
              ["electronics" "featured" "prod-123"]
              {:ql/type :pg/update
               :update :products
               :set {:categories [:|| :categories [:pg/array [[:pg/param "electronics"] [:pg/param "featured"]]]]}
               :where [:= :id [:pg/param "prod-123"]]}))

  (testing "UPDATE with mathematical operations"

    (test-sql 30
              "UPDATE accounts SET balance = balance - $1, updated_at = NOW() WHERE id = $2 AND balance >= $1"
              [100.50 "acc-789"]
              {:ql/type :pg/update
               :update :accounts
               :set {:balance [:- :balance [:pg/param 100.50]]
                     :updated_at [:now]}
               :where [:and
                       [:= :id [:pg/param "acc-789"]]
                       [:>= :balance [:pg/param 100.50]]]})

    (test-sql 31
              "UPDATE statistics SET views = views + 1, rating = (rating * review_count + $1) / (review_count + 1), review_count = review_count + 1 WHERE id = $2"
              [4.5 "stat-123"]
              {:ql/type :pg/update
               :update :statistics
               :set {:views [:+ :views 1]
                     :rating [:/ [:+ [:* :rating :review_count] [:pg/param 4.5]]
                              [:+ :review_count 1]]
                     :review_count [:+ :review_count 1]}
               :where [:= :id [:pg/param "stat-123"]]})

    (test-sql 32
              "UPDATE inventory SET quantity = GREATEST(0, quantity - $1) WHERE product_id = $2"
              [5 "prod-999"]
              {:ql/type :pg/update
               :update :inventory
               :set {:quantity [:greatest 0 [:- :quantity [:pg/param 5]]]}
               :where [:= :product_id [:pg/param "prod-999"]]}))

  (testing "UPDATE with complex JSONB path operations"

    (test-sql 33
              "UPDATE users SET preferences = jsonb_set(preferences, '{notifications,channels}', preferences->'notifications'->'channels' || jsonb_build_array($1)) WHERE id = $2"
              ["sms" "user-789"]
              {:ql/type :pg/update
               :update :users
               :set {:preferences [:pg/jsonb_set :preferences [:notifications :channels] [:||
                                                                                          [:-> [:-> :preferences [:pg/sql "'notifications'"]] [:pg/sql "'channels'"]]
                                                                                          ^:pg/fn[:jsonb_build_array [:pg/param "sms"]]]]}
               :where [:= :id [:pg/param "user-789"]]})

    (test-sql 34
              "UPDATE products SET attributes = jsonb_set(attributes, '{dimensions}', jsonb_build_object('width', $1, 'height', $2, 'depth', $3)) WHERE sku = $4"
              [10 20 5 "SKU-123"]
              {:ql/type :pg/update
               :update :products
               :set {:attributes [:pg/jsonb_set :attributes [:dimensions]
                                  ^:pg/fn[:jsonb_build_object
                                          [:pg/sql "'width'"] [:pg/param 10]
                                          [:pg/sql "'height'"] [:pg/param 20]
                                          [:pg/sql "'depth'"] [:pg/param 5]]]}
               :where [:= :sku [:pg/param "SKU-123"]]}))

  (testing "UPDATE with string operations"

    (test-sql 35
              "UPDATE users SET email = LOWER(email), username = UPPER(username) WHERE id = $1"
              ["user-123"]
              {:ql/type :pg/update
               :update :users
               :set {:email ^:pg/fn[:lower :email]
                     :username ^:pg/fn[:upper :username]}
               :where [:= :id [:pg/param "user-123"]]})

    (test-sql 36
              "UPDATE products SET description = CONCAT(name, ' - ', category, ' (', brand, ')') WHERE description IS NULL"
              {:ql/type :pg/update
               :update :products
               :set {:description ^:pg/fn[:concat :name [:pg/sql "' - '"] :category [:pg/sql "' ('"] :brand [:pg/sql "')'"]]}
               :where [:is :description nil]})

    (test-sql 37
              "UPDATE articles SET slug = REGEXP_REPLACE(LOWER(title), '[^a-z0-9]+', '-', 'g') WHERE slug IS NULL"
              {:ql/type :pg/update
               :update :articles
               :set {:slug ^:pg/fn[:regexp_replace ^:pg/fn[:lower :title] [:pg/sql "'[^a-z0-9]+'"] [:pg/sql "'-'"] [:pg/sql "'g'"]]}
               :where [:is :slug nil]}))

  (testing "UPDATE with date/time operations"

    (test-sql 38
              "UPDATE subscriptions SET expires_at = expires_at + INTERVAL '1 month' WHERE id = $1 AND auto_renew = true"
              ["sub-123"]
              {:ql/type :pg/update
               :update :subscriptions
               :set {:expires_at [:+ :expires_at [:pg/cast [:pg/sql "'1 month'"] :pg_catalog.interval]]}
               :where [:and
                       [:= :id [:pg/param "sub-123"]]
                       [:= :auto_renew true]]})

    (test-sql 39
              "UPDATE events SET scheduled_date = date_trunc('day', scheduled_date) + INTERVAL '18 hours' WHERE event_type = $1"
              ["webinar"]
              {:ql/type :pg/update
               :update :events
               :set {:scheduled_date [:+ ^:pg/fn[:date_trunc [:pg/sql "'day'"] :scheduled_date]
                                      [:pg/cast [:pg/sql "'18 hours'"] :pg_catalog.interval]]}
               :where [:= :event_type [:pg/param "webinar"]]})

    (test-sql 40
              "UPDATE tasks SET reminder_at = due_date - INTERVAL '2 days' WHERE reminder_at IS NULL AND due_date > NOW()"
              {:ql/type :pg/update
               :update :tasks
               :set {:reminder_at [:- :due_date [:pg/cast [:pg/sql "'2 days'"] :pg_catalog.interval]]}
               :where [:and
                       [:is :reminder_at nil]
                       [:> :due_date [:now]]]}))

  (testing "UPDATE with EXISTS and NOT EXISTS"

    (test-sql 41
              "UPDATE users SET active = false WHERE NOT EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id AND orders.created_at > NOW() - INTERVAL '6 months')"
              {:ql/type :pg/update
               :update :users
               :set {:active false}
               :where [:not [:exists {:ql/type :pg/sub-select
                                      :select 1
                                      :from :orders
                                      :where [:and
                                              [:= :orders.user_id :users.id] [:> :orders.created_at [:- [:now] [:pg/cast [:pg/sql "'6 months'"] :pg_catalog.interval]]]]}]]}))

  (testing "UPDATE with CTEs"

    (test-sql 42
              "WITH active_users AS (SELECT id FROM users WHERE last_login > NOW() - INTERVAL '30 days') UPDATE user_stats SET status = 'active' WHERE user_id IN (SELECT id FROM active_users)"
              {:ql/type :pg/cte
               :with {:active_users {:select {:id :id}
                                     :from :users
                                     :where [:> :last_login [:- [:now] [:pg/cast [:pg/sql "'30 days'"] :pg_catalog.interval]]]}}
               :select {:ql/type :pg/update
                        :update :user_stats
                        :set {:status :active}
                        :where [:in :user_id {:ql/type :pg/sub-select
                                              :select :id
                                              :from :active_users}]}})

    (test-sql 43
              "WITH avg_prices AS (SELECT category_id, AVG(price) as avg_price FROM products GROUP BY category_id) UPDATE products SET price = ap.avg_price * 0.9 FROM avg_prices ap WHERE products.category_id = ap.category_id AND products.price IS NULL"
              {:ql/type :pg/cte
               :with {:avg_prices {:select {:category_id :category_id
                                            :avg_price [:avg :price]}
                                   :from :products
                                   :group-by {:category_id :category_id}}}
               :select {:ql/type :pg/update
                        :update :products
                        :set {:price [:* :ap.avg_price 0.9]}
                        :from {:ap :avg_prices}
                        :where [:and
                                [:= :products.category_id :ap.category_id]
                                [:is :products.price nil]]}}))

    (testing "UPDATE with type casting edge cases"

      (test-sql 44
                "UPDATE events SET metadata = metadata || ($1::text)::jsonb WHERE id = $2"
                ["{\"source\":\"api\"}" "evt-123"]
                {:ql/type :pg/update
                 :update :events
                 :set {:metadata [:|| :metadata [:pg/cast [:pg/cast [:pg/param "{\"source\":\"api\"}"] :text] :jsonb]]}
                 :where [:= :id [:pg/param "evt-123"]]}))

    (testing "UPDATE with NULL handling"

      (test-sql 45
                "UPDATE orders SET discount = COALESCE(discount, 0) + $1 WHERE id = $2"
                [5 "ord-123"]
                {:ql/type :pg/update
                 :update :orders
                 :set {:discount [:+ [:pg/coalesce :discount 0] [:pg/param 5]]}
                 :where [:= :id [:pg/param "ord-123"]]}))

    (testing "UPDATE with RETURNING complex expressions"

      (test-sql 46
                "UPDATE inventory SET quantity = quantity - $1 WHERE product_id = $2 RETURNING product_id, quantity, quantity < 10 as low_stock"
                [5 "prod-789"]
                {:ql/type :pg/update
                 :update :inventory
                 :set {:quantity [:- :quantity [:pg/param 5]]}
                 :where [:= :product_id [:pg/param "prod-789"]]
                 :returning [:pg/columns :product_id :quantity [:as [:< :quantity 10] :low_stock]]})

      (test-sql 47
                "UPDATE users SET credits = credits + $1 WHERE id = $2 RETURNING id, credits, CASE WHEN credits >= 1000 THEN 'premium' WHEN credits >= 100 THEN 'standard' ELSE 'basic' END as tier"
                [50 "user-123"]
                {:ql/type :pg/update
                 :update :users
                 :set {:credits [:+ :credits [:pg/param 50]]}
                 :where [:= :id [:pg/param "user-123"]]
                 :returning [:pg/columns :id :credits
                             [:as [:cond
                                   [:>= :credits 1000] :premium
                                   [:>= :credits 100] :standard
                                   :basic] :tier]]}))

    (testing "UPDATE with quoted identifiers and reserved words"

      (test-sql 49
                "UPDATE \"user\" SET \"from\" = $1, \"to\" = $2, \"select\" = $3 WHERE \"user\" = $4"
                ["NYC" "LA" "first-class" "john"]
                {:ql/type :pg/update
                 :update :user
                 :set {:from [:pg/param "NYC"]
                       :to [:pg/param "LA"]
                       :select [:pg/param "first-class"]}
                 :where [:= :user [:pg/param "john"]]})

      (test-sql 50
                "UPDATE \"order\" SET \"group\" = $1, \"having\" = $2 WHERE \"where\" = $3"
                ["A" "premium" "US"]
                {:ql/type :pg/update
                 :update :order
                 :set {:group [:pg/param "A"]
                       :having [:pg/param "premium"]}
                 :where [:= :where [:pg/param "US"]]}))

    (testing "UPDATE with boolean and comparison operations"

      (test-sql 51
                "UPDATE products SET on_sale = (discount > 0 AND price < 100), featured = NOT hidden WHERE category_id = $1"
                [5]
                {:ql/type :pg/update
                 :update :products
                 :set {:on_sale [:and [:> :discount 0] [:< :price 100]]
                       :featured [:not :hidden]}
                 :where [:= :category_id [:pg/param 5]]})

      (test-sql 52
                "UPDATE users SET verified = email IS NOT NULL AND email_confirmed = true AND phone IS NOT NULL WHERE created_at < $1"
                ["2023-01-01"]
                {:ql/type :pg/update
                 :update :users
                 :set {:verified [:and [:is :email [:pg/sql "NOT NULL"]] [:= :email_confirmed true] [:is :phone [:pg/sql "NOT NULL"]]]}
                 :where [:< :created_at [:pg/param "2023-01-01"]]})))