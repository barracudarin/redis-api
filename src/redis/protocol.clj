(ns redis.protocol
  (:require [clojure.string :as str]
            [gloss.core :as gloss]))

(defprotocol Serializer
  (-serialize [this]))

(extend-protocol Serializer
  String
  (-serialize [this]
    (format "$%s\r\n%s\r\n" (count this) this))

  clojure.lang.Keyword
  (-serialize [this]
    (-serialize (subs (str this) 1)))

  clojure.lang.PersistentVector
  (-serialize [this]
    (format "*%s\r\n%s" (count this) (apply str (map -serialize this))))

  clojure.lang.LazySeq
  (-serialize [this]
    (format "*%s\r\n%s" (count this) (apply str (map -serialize this))))

  Long
  (-serialize [this]
    (format ":%s\r\n" (pr-str this)))

  nil
  (-serialize [this]
    "_\r\n")

  Boolean
  (-serialize [this]
    (format "#%s\r\n" (if this "t" "f")))

  java.util.Map
  (-serialize [this]
    (let [entries (mapcat identity this)]
      (format "%%%s\r\n%s" (count this) (apply str (map -serialize entries)))))

  clojure.lang.PersistentHashSet
  (-serialize [this]
    (format "~%s\r\n%s" (count this) (apply str (map -serialize this)))))

(def codec
  (gloss/compile-frame (gloss/string :ascii :delimiters ["\r\n"]) identity not-empty))

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
