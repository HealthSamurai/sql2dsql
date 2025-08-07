(ns sql2dsql.core
  (:require [cheshire.core :as json]
            [org.httpkit.server :as httpkit]
            [compojure.core :refer :all]
            [compojure.route :as route]
            [sql2dsql.transpiler :refer [->dsql make-parser parse-sql]]
            [ring.middleware.defaults :refer [wrap-defaults site-defaults]])
  (:gen-class))

(defroutes app-routes
           (GET "/" []
             {:status 200
              :headers {"Content-Type" "text/html"}
              :body (slurp "resources/public/index.html")})

           (POST "/to-dsql" request
             (try
                 {:status 200 :body (->dsql (-> request :body slurp (json/parse-string true)))}
               (catch Exception e
                 {:status 400 :body (str "Error: " (.getMessage e))})))

           (route/resources "/")

           (route/not-found (fn [request]
                                {:status 404
                                 :body (str "Error: Route not found - " (:uri request))})))

(def app
  (wrap-defaults app-routes
                 (assoc-in site-defaults [:security :anti-forgery] false)))

(defn -main [& args]
  (println "Starting server on port 3000...")
  (let [lib-path (or (first args) "libpg_query.dylib")]
    (println "Using library path:" lib-path)
    (reset! parse-sql (make-parser lib-path))
    (httpkit/run-server app {:port 3000 :join? false})))
