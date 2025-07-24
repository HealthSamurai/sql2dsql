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
      (let [result# (apply parse ~sql ~params)]
        (is (= result# ~expected)
            (str "\nTest " ~num " failed!"
                 "\nExpected: " ~expected
                 "\nActual:   " result#))))))

(defmacro test-sql-meta
  [num sql params expected]
  `(testing (str "Test " ~num ": " ~sql)
     (let [result# (apply parse ~sql ~params)
           meta-actual# (meta (:select result#))
           meta-expected# (meta (:select ~expected))]
       (is (= result# ~expected)
           (str "\nTest " ~num " failed!"
                "\nExpected: " ~expected
                "\nActual:   " result#))
       (when (= result# ~expected)
         (is (= meta-actual# meta-expected#)
             (str "\nTest " ~num " metadata failed!"
                  "\nExpected meta: " meta-expected#
                  "\nActual meta:   " meta-actual#))))))

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
              "SELECT count(*) as count FROM dft
               LEFT JOIN document d ON dft.id = d.resource ->> 'caseNumber' AND d.resource ->> 'name' = $1
               WHERE d.id is NULL"
              ["front"]
              {:select {:count [:pg/count*]}
               :from :dft
               :left-join {:d {:table :document
                               :on [:and
                                    ^:pg/op [:= :dft.id [:jsonb/->> :d.resource "caseNumber"]]
                                    ^:pg/op [:= [:jsonb/->> :d.resource "name"] [:pg/param "front"]]]}}
               :where ^:pg/op [:is :d.id nil]}))

  (testing "EXPLAIN queries"

    (test-sql 9
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
                                    ^:pg/op [:= :dft.id [:jsonb/->> :d.resource "caseNumber"]]
                                    ^:pg/op [:= [:jsonb/->> :d.resource "name"] [:pg/param "front"]]]}}
               :where ^:pg/op [:is :d.id nil]}))

  (testing "Complex WHERE clauses"

    (test-sql 10
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

    (test-sql 11
              "SELECT count(*) FROM oru WHERE ( resource #>> '{message,datetime}' )::timestamp > now() - interval '1 week' and id ILIKE $1"
              ["%Z%.CV"]
              {:select {:count [:pg/count*]}
               :from :oru
               :where [:and
                       [:>
                        [:pg/cast [:jsonb/#>> :resource [:message :datetime]] :pg_catalog.timestamp]
                        [:- [:now] [:pg/cast "1 week" :pg_catalog.interval]]]
                       [:ilike :id [:pg/param "%Z%.CV"]]]}))

  (testing "Function calls"

    (test-sql 12
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

    (test-sql 13
              "SELECT AVG(salary) as avg_sal FROM abc"
              {:select {:avg_sal [:avg :salary]}
               :from :abc}))

  (testing "DISTINCT queries"

    (test-sql 14
              "SELECT DISTINCT test FROM best"
              {:select-distinct {:test :test}
               :from :best})

    (test-sql 15
              "SELECT DISTINCT id as id , resource as resource , txid as txid FROM best"
              {:select-distinct {:id :id
                                 :resource :resource
                                 :txid :txid}
               :from :best}))

  (testing "DISTINCT ON queries"

    (test-sql-meta 16
                   "SELECT DISTINCT ON ( id , txid ) id as id , resource as resource , txid as txid FROM best"
                   []
                   {:select ^{:pg/projection {:distinct-on [:id :txid]}}
                            {:id :id
                             :resource :resource
                             :txid :txid}
                    :from :best})

    (test-sql-meta 17
                   "SELECT DISTINCT ON ( id ) id as id , resource as resource , txid as txid FROM best"
                   []
                   {:select ^{:pg/projection {:distinct-on [:id]}}
                            {:id :id
                             :resource :resource
                             :txid :txid}
                    :from :best})

    (test-sql-meta 18
                   "SELECT DISTINCT ON ( ( resource #>> '{id}' ) ) id as id , resource as resource , txid as txid FROM best"
                   []
                   {:select ^{:pg/projection {:distinct-on [[:jsonb/#>> :resource [:id]]]}}
                            {:id :id
                             :resource :resource
                             :txid :txid}
                    :from :best}))

  (testing "SELECT ALL"

    (test-sql 19
              "SELECT ALL id as id , resource as resource , txid as txid FROM best"
              {:select {:id :id
                        :resource :resource
                        :txid :txid}
               :from :best}))

  (testing "GROUP BY with DISTINCT"

    (test-sql 20
              "SELECT department_id, COUNT(DISTINCT job_title) as count_job_titles FROM employee GROUP BY department_id"
              {:select {:department_id :department_id
                        :count_job_titles [:count [:distinct [:pg/columns :job_title]]]}
               :from :employee
               :group-by {:department_id :department_id}})

    (test-sql 21
              "SELECT department_id, COUNT(DISTINCT job_title) FROM employee GROUP BY department_id"
              {:select {:department_id :department_id
                        :count [:count [:distinct [:pg/columns :job_title]]]}
               :from :employee
               :group-by {:department_id :department_id}}))

  (testing "VALUES clause"

    (test-sql 22
              "(VALUES (1, 'Alice'), (2, 'Grandma'), (3, 'Bob'))"
              {:ql/type :pg/values
               :keys [:k1 :k2]
               :values [{:k1 1 :k2 [:pg/sql "'Alice'"]}
                        {:k1 2 :k2 [:pg/sql "'Grandma'"]}
                        {:k1 3 :k2 [:pg/sql "'Bob'"]}]}))

  (testing "FETCH FIRST"

    (test-sql 23
              "SELECT * FROM employees FETCH FIRST 5 ROWS ONLY"
              {:select :*
               :from :employees
               :limit 5}))

  (testing "WITH clause (CTEs)"

    (test-sql 24
              "WITH recent_hires AS ( SELECT * FROM employees WHERE hire_date > CURRENT_DATE - INTERVAL '30 days')
               SELECT * FROM recent_hires"
              {:ql/type :pg/cte
               :with {:recent_hires {:select :*
                                     :from :employees
                                     :where [:> :hire_date
                                             [:- :CURRENT_DATE [:pg/cast [:pg/sql "'30 days'"] :pg_catalog.interval]]]}}
               :select {:select :*
                        :from :recent_hires}})

    (test-sql 25
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
               :with {:dept_avg {:select {:dept_id :department_id
                                          :avg_sal [:avg :salary]}
                                 :from :employee
                                 :group-by {:dept_id :department_id}}
                      :high_earners {:select :*
                                     :from :employee
                                     :where [:> :salary
                                             {:ql/type :pg/sub-select
                                              :select [:avg :salary]
                                              :from :dept_avg
                                              :where [:= :dept_avg.dept_id :employee.department_id]}]}}
               :select {:select :*
                        :from :high_earners}}))

  (testing "Set operations"

    (test-sql 26
              "SELECT name FROM employees UNION SELECT name FROM contractors"
              {:select {:name :name}
               :from :employees
               :union {:contractors {:ql/type :pg/sub-select
                                     :select {:name :name}
                                     :from :contractors}}})

    (test-sql 27
              "SELECT name FROM employees INTERSECT SELECT name FROM contractors"
              {:select {:name :name}
               :from :employees
               :intersect {:contractors {:ql/type :pg/sub-select
                                         :select {:name :name}
                                         :from :contractors}}})

    (test-sql 28
              "SELECT name FROM employees EXCEPT SELECT name FROM contractors"
              {:select {:name :name}
               :from :employees
               :except {:contractors {:ql/type :pg/sub-select
                                      :select {:name :name}
                                      :from :contractors}}})

    (test-sql 29
              "SELECT name FROM employees UNION ALL SELECT name FROM contractors"
              {:select {:name :name}
               :from :employees
               :union-all {:contractors {:ql/type :pg/sub-select
                                         :select {:name :name}
                                         :from :contractors}}})))