(ns redis.client.protocol.resp2
  (:require [redis.client.protocol.shared :as shared])
  (:import [clojure.lang Keyword]))

(defprotocol RespVersion2
  (-write [frame]))

(defn read-bulk-string
  [client value]
  (let [length (parse-long value)]
    (if (neg? length)
      nil
      (shared/read-bulk-string client value))))

(defn read-list
  [client value]
  (let [length (parse-long value)]
    (if (neg? length)
      nil
      (shared/read-list client value))))

(defn write-nil
  [_]
  "$-1\r\n")

(extend nil            RespVersion2 {:-write write-nil})
(extend String         RespVersion2 {:-write shared/write-bulk-string})
(extend Number         RespVersion2 {:-write shared/write-number})
(extend Keyword        RespVersion2 {:-write (partial shared/write-keyword -write)})
(extend java.util.List RespVersion2 {:-write (partial shared/write-seqable "*" -write)})

(def readers
  {\+ shared/read-simple-string
   \- shared/read-error
   \: shared/read-number
   \$ read-bulk-string
   \* read-list})

(def resp2
  {:version 2
   :encode -write
   :decode (shared/decode-fn readers)})
