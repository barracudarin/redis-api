(ns redis.client.protocol
  (:require [redis.client.protocol.resp2 :as resp2]
            [redis.client.protocol.resp3 :as resp3]))

(def versions
  (into {} (map (juxt :version identity)) [resp2/resp2 resp3/resp3]))
