(ns sql2dsql.core
  (:require [cheshire.core :as json]
            [ring.adapter.jetty :refer [run-jetty]]
            [compojure.core :refer :all]
            [compojure.route :as route]
            [ring.middleware.defaults :refer [wrap-defaults site-defaults]]
            [sql2dsql.transpiler :refer [->dsql]]))

(defn parse [sql & params]
  (-> (apply ->dsql (cons sql params)) str))

(defroutes app-routes
           (GET "/" []
             {:status 200
              :headers {"Content-Type" "text/html"}
              :body (slurp "resources/public/index.html")})

           (POST "/to_dsql" request
             (try
                 {:status 200 :body (parse (-> request :body slurp (json/parse-string true)))}
               (catch Exception e
                 {:status 400 :body (str "Error: " (.getMessage e))})))

           (route/resources "/")

           (route/not-found {:status 404 :body "Error: Route not found"}))

(def app
  (wrap-defaults app-routes
                 (assoc-in site-defaults [:security :anti-forgery] false)))

(defn -main []
  (run-jetty app {:port 3000 :join? false}))
