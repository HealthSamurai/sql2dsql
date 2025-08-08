(ns sql2dsql.core
  (:require [cheshire.core :as json]
            [org.httpkit.server :as httpkit]
            [compojure.core :refer :all]
            [compojure.route :as route]
            [sql2dsql.sql-parser :refer [make-sql-parser sql->dsql]]
            [ring.middleware.defaults :refer [wrap-defaults site-defaults]])
  (:gen-class))

(defn make-routes [parser]
  "Create routes with parser dependency injected"
  (routes
    (GET "/" []
      {:status 200
       :headers {"Content-Type" "text/html"}
       :body (slurp "resources/public/index.html")})

    (POST "/to-dsql" request
      (try
        (let [sql (-> request :body slurp (json/parse-string true))
              result (sql->dsql parser sql [] true)]
          {:status 200
           :headers {"Content-Type" "application/json"}
           :body result})
        (catch Exception e
          {:status 400
           :headers {"Content-Type" "application/json"}
           :body (.getMessage e)})))

    (route/resources "/")

    (route/not-found (fn [request]
                       {:status 404
                        :headers {"Content-Type" "application/json"}
                        :body (json/generate-string
                                {:error (str "Route not found - " (:uri request))})}))))

(defn make-app [parser]
  "Create the app with parser dependency"
  (wrap-defaults (make-routes parser)
                 (assoc-in site-defaults [:security :anti-forgery] false)))

(defn -main [& args]
  (println "Starting server on port 3000...")
  (let [lib-path (or (first args) "libpg_query.dylib")]
    (println "Using library path:" lib-path)
    (try
      (let [parser (make-sql-parser lib-path)
            app (make-app parser)]
        (println "Parser initialized successfully")
        (httpkit/run-server app {:port 3000 :join? false})
        (println "Server started on port 3000"))
      (catch Exception e
        (println "Failed to initialize parser:" (.getMessage e))
        (System/exit 1)))))