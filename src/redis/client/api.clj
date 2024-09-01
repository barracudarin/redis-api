(ns redis.client.api
  (:require [aleph.tcp :as tcp]
            [clojure.string :as str]
            [manifold.deferred :as d]
            [manifold.stream :as s]
            [redis.client.codec :as codec]
            [redis.client.protocol :as p]
            [redis.commands :as c]))

(defn client
  [{:keys [host port protocol]
    :or {protocol 3}}]
  {:protocol (p/versions protocol)
   :conn @(d/chain (tcp/client {:host host, :port port}) codec/duplex)})

(defn send!
  [{:keys [conn] {:keys [encode]} :protocol} ops]
  @(s/put! conn (str/join (map (comp encode c/build) ops))))

(defn receive!
  [client]
  (codec/load-frame! client))
