(ns redis.client.codec
  (:require [gloss.core :as gloss]
            [gloss.io :as io]
            [manifold.stream :as s]))

(def codec
  (gloss/compile-frame
   (gloss/string :ascii :delimiters ["\r\n"])
   identity
   not-empty))

(defn duplex
  [tcp]
  (let [out (s/stream)]
    (s/connect (s/map #(io/encode codec %) out) tcp)
    (s/splice out (io/decode-stream tcp codec))))

(defn read-frame!
  [{:keys [conn] :as _client}]
  @(s/take! conn))

(defn load-frame!
  [{{:keys [decode]} :protocol :as client}]
  (when-let [frame (read-frame! client)]
    (prn frame)
    (decode client frame)))

(comment
  (require '[clojure.edn :as edn])
  (require '[dev.utils :as utils])
  (require '[manifold.stream :as s])

  (def port 12323)

  (defn handler
    [f]
    (fn [s _info]
      (s/connect (s/map f s) s)))

  (defn test-handler
    [input]
    (prn input)
    input)

  (def protocol
    (gloss/compile-frame
     (gloss/finite-frame :uint32 (gloss/string :utf-8))
     pr-str
     edn/read-string))

  (def server (utils/start-server (gloss/string :ascii :delimiters ["\r\n"]) (handler test-handler) port))
  (def client @(utils/client resp2 "localhost" 6379))

  @(s/put! client "+OK\r\n")
  @(s/take! client)

  (.close server)

  nil)
