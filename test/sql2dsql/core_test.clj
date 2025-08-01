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
              {:select {:has_high_earners
                        {:ql/type :pg/sub-select,
                         :select {:a :a},
                         :from :employees,
                         :where [:> :salary 100000]}}})))

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

    ;; Test 61: INSERT with DEFAULT values
    (test-sql 61
              "INSERT INTO users (id, name, created_at) VALUES ($1, $2, DEFAULT)"
              ["123" "Alice"]
              {:ql/type :pg/insert
               :into :users
               :value {:id [:pg/param "123"]
                       :name [:pg/param "Alice"]
                       :created_at :DEFAULT}})

    ;; Test 62: INSERT with multiple DEFAULT values
    (test-sql 62
              "INSERT INTO logs (id, created_at, updated_at) VALUES (DEFAULT, DEFAULT, DEFAULT)"
              {:ql/type :pg/insert
               :into :logs
               :value {:id :DEFAULT
                       :created_at :DEFAULT
                       :updated_at :DEFAULT}})

    ;; Test 63: INSERT with CURRENT_TIMESTAMP
    (test-sql 63
              "INSERT INTO events (id, name, event_time) VALUES ($1, $2, CURRENT_TIMESTAMP)"
              ["evt-1" "Login"]
              {:ql/type :pg/insert
               :into :events
               :value {:id [:pg/param "evt-1"]
                       :name [:pg/param "Login"]
                       :event_time :CURRENT_TIMESTAMP}})

    ;; Test 64: INSERT with CURRENT_DATE and CURRENT_TIME
    (test-sql 64
              "INSERT INTO schedules (id, schedule_date, schedule_time) VALUES ($1, CURRENT_DATE, CURRENT_TIME)"
              ["sch-1"]
              {:ql/type :pg/insert
               :into :schedules
               :value {:id [:pg/param "sch-1"]
                       :schedule_date :CURRENT_DATE
                       :schedule_time :CURRENT_TIME}})

    ;; Test 65: INSERT with NULL values
    (test-sql 65
              "INSERT INTO patients (id, name, middle_name, deceased_date) VALUES ($1, $2, NULL, NULL)"
              ["p-1" "John Doe"]
              {:ql/type :pg/insert
               :into :patients
               :value {:id [:pg/param "p-1"]
                       :name [:pg/param "John Doe"]
                       :middle_name nil
                       :deceased_date nil}})

    ;; Test 66: INSERT with array literals
    (test-sql 66
              "INSERT INTO tags_table (id, tags) VALUES ($1, ARRAY['tag1', 'tag2', 'tag3'])"
              ["123"]
              {:ql/type :pg/insert
               :into :tags_table
               :value {:id [:pg/param "123"]
                       :tags [:pg/array [:tag1 :tag2 :tag3]]}})

    ;; Test 68: INSERT with complex JSONB operations
    (test-sql 68
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

    ;; Test 69: INSERT with nested subquery in value
    (test-sql 69
              "INSERT INTO summary (id, total_count) VALUES ($1, (SELECT COUNT(*) FROM users WHERE active = true))"
              ["sum-1"]
              {:ql/type :pg/insert
               :into :summary
               :value {:id [:pg/param "sum-1"]
                       :total_count {:ql/type :pg/sub-select
                                     :select [:pg/count*]
                                     :from :users
                                     :where [:= :active true]}}})

    ;; Test 70: INSERT with ON CONFLICT on multiple columns
    (test-sql 70
              "INSERT INTO user_roles (user_id, role_id, assigned_at) VALUES ($1, $2, $3) ON CONFLICT (user_id, role_id) DO UPDATE SET assigned_at = EXCLUDED.assigned_at"
              ["u-1" "r-1" "2023-01-01"]
              {:ql/type :pg/insert
               :into :user_roles
               :value {:user_id [:pg/param "u-1"]
                       :role_id [:pg/param "r-1"]
                       :assigned_at [:pg/param "2023-01-01"]}
               :on-conflict {:on [:user_id :role_id]
                             :do {:set {:assigned_at :excluded.assigned_at}}}})

    ;; Test 73: INSERT with quoted column names
    (test-sql 73
              "INSERT INTO \"user\" (\"id\", \"from\", \"to\", \"select\") VALUES ($1, $2, $3, $4)"
              ["123" "NYC" "LA" "first"]
              {:ql/type :pg/insert
               :into :user
               :value {:id [:pg/param "123"]
                       :from [:pg/param "NYC"]
                       :to [:pg/param "LA"]
                       :select [:pg/param "first"]}})

    ;; Test 74: INSERT with schema-qualified table
    (test-sql 74
              "INSERT INTO public.users (id, name) VALUES ($1, $2)"
              ["u-1" "Alice"]
              {:ql/type :pg/insert
               :into :public.users
               :value {:id [:pg/param "u-1"]
                       :name [:pg/param "Alice"]}})

    ;; Test 76: INSERT with interval type
    (test-sql 76
              "INSERT INTO schedules (id, name, duration) VALUES ($1, $2, INTERVAL '1 hour 30 minutes')"
              ["sch-1" "Meeting"]
              {:ql/type :pg/insert
               :into :schedules
               :value {:id [:pg/param "sch-1"]
                       :name [:pg/param "Meeting"]
                       :duration [:pg/cast [:pg/sql "'1 hour 30 minutes'"] :pg_catalog.interval]}})

    ;; Test 77: INSERT with boolean expressions
    (test-sql 77
              "INSERT INTO feature_flags (id, name, enabled) VALUES ($1, $2, $3 = 'true')"
              ["ff-1" "new_ui" "true"]
              {:ql/type :pg/insert
               :into :feature_flags
               :value {:id [:pg/param "ff-1"]
                       :name [:pg/param "new_ui"]
                       :enabled [:= [:pg/param "true"] :true]}})

    ;; Test 78: INSERT with CASE expression
    (test-sql 78
              "INSERT INTO users (id, name, status) VALUES ($1, $2, CASE WHEN $3 > 18 THEN 'adult' ELSE 'minor' END)"
              ["u-1" "John" 25]
              {:ql/type :pg/insert
               :into :users
               :value {:id [:pg/param "u-1"]
                       :name [:pg/param "John"]
                       :status [:cond
                                [:> [:pg/param 25] 18] :adult
                                :minor]}})

    ;; Test 81: INSERT with computed column using other columns
    (test-sql 81
              "INSERT INTO products (id, price, tax, total) VALUES ($1, $2, $3, $2 * (1 + $3))"
              ["prod-1" 100 0.1]
              {:ql/type :pg/insert
               :into :products
               :value {:id [:pg/param "prod-1"]
                       :price [:pg/param 100]
                       :tax [:pg/param 0.1]
                       :total [:* [:pg/param 100] [:+ 1 [:pg/param 0.1]]]}})

    ;; Test 82: INSERT with uuid generation
    (test-sql 82
              "INSERT INTO sessions (id, user_id, token) VALUES (uuid_generate_v4(), $1, encode(gen_random_bytes(32), 'hex'))"
              ["user-123"]
              {:ql/type :pg/insert
               :into :sessions
               :value {:id ^:pg/fn [:uuid_generate_v4]
                       :user_id [:pg/param "user-123"]
                       :token ^:pg/fn [:encode ^:pg/fn [:gen_random_bytes 32] [:pg/sql "'hex'"]]}})

    ;; Test 83: INSERT with ON CONFLICT with WHERE clause in DO UPDATE
    (test-sql 83
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

    ;; Test 85: INSERT with very long column list
    (test-sql 85
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
                       :col10 [:pg/param "v10"]}})))