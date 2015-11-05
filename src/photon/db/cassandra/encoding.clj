(ns photon.db.cassandra.encoding
  (:require [clojure.java.io :as io]
            [taoensso.nippy :as nippy]
            [pjson.core :as pjson]
            [cheshire.core :as json])
  (:import (java.nio Buffer ByteBuffer)
           (java.nio.charset Charset)
           (java.io PushbackReader)
           (java.util Arrays)))

(def charset (Charset/forName "ISO-8859-1"))

(defn encode [^String str]
  (ByteBuffer/wrap (.getBytes str charset)))

(defn decode [^Buffer b]
  (String. (.array b) charset))

(defn byte-output-stream [v i bb]
  (proxy [java.io.OutputStream] []
    (close []
      (let [new-bb (ByteBuffer/allocateDirect @i)]
        (dorun (map #(.put new-bb (nth % 0) (nth % 1) (nth % 2))
                    (persistent! @v)))
        (.flip new-bb)
        (reset! bb new-bb)))
    (write [#^bytes b ^Integer off ^Integer len]
      (let [ve @v]
        (reset! v (conj! ve [(Arrays/copyOf b len) off len])))
      (swap! i + len))))

(defn byte-input-stream [^ByteBuffer bb]
  (proxy [java.io.InputStream] []
    (available [] (.remaining bb))
    (read
      ([] (if (.hasRemaining bb) (.get bb) -1))
      ([#^bytes b ^Integer off ^Integer len]
       (let [c (min (.remaining bb) len)]
         (if (= 0 c)
           -1
           (do (.get bb b off c) c)))))))

(defn clj-encode-edn-stream [item]
  (let [bb (atom nil)
        the-boss (byte-output-stream (atom (transient [])) (atom 0) bb)]
    (with-open [w (io/writer the-boss)]
      (binding [*out* w]
        (pr item)))
    @bb))

(defn clj-decode-edn-stream [data]
  (with-open [r (PushbackReader. (io/reader (byte-input-stream data)) 8192)]
    (read r)))

(defn clj-encode-json-stream [item]
  (let [bb (atom nil)
        the-boss (byte-output-stream (atom (transient [])) (atom 0) bb)]
    (with-open [w (io/writer the-boss)]
      (json/generate-stream item w))
    @bb))

(defn clj-decode-json-stream [data]
  (with-open [r (io/reader (byte-input-stream data))]
    (json/parse-stream r true)))

(defn clj-encode-nippy [item]
  (ByteBuffer/wrap (nippy/freeze item)))
(defn clj-decode-nippy [data]
  (nippy/thaw (.array data)))

(defn clj-encode-pjson [item]
  (encode (pjson/write-str item)))
(defn clj-decode-pjson [data]
  (let [item (decode data)]
    (if (= "{}" item)
     {}
     (pjson/read-str item))))

(defn clj-encode-edn [item]
  (encode (pr-str item)))
(defn clj-decode-edn [data]
  (let [item (decode data)]
    (read-string item)))

(defn clj-encode-json [item]
  (encode (json/generate-string item)))
(defn clj-decode-json [data]
  (let [item (decode data)]
    (json/parse-string item true)))

(defn remaining [^Buffer b] (.remaining b))

(defn get-from-buffer [^ByteBuffer b #^bytes bb] (.get b bb))

(def clj-encode-smile json/generate-smile)

(defn clj-decode-smile [data]
  (let [bb (byte-array (remaining data))]
    (get-from-buffer data bb)
    (json/parse-smile bb true)))

