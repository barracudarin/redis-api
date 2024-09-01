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
