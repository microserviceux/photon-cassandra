(ns photon.db.cassandra.encoding
  (:require [clojure.java.io :as io]
            [taoensso.nippy :as nippy]
            [pjson.core :as pjson]
            [cheshire.core :as json])
  (:import (java.nio CharBuffer Buffer ByteBuffer)
           (java.io PushbackReader)
           (java.util Arrays)))

(defn buffer->string [^CharBuffer cb] (.toString cb))

(defn encode [^String str]
  (let [l (.length str)
        bb (ByteBuffer/allocateDirect l)]
    (loop [i 0]
      (when (< i l)
        (.put bb i (.getAt str i))
        (recur (inc i))))))

(defn decode [^Buffer b]
  (String. (.array b) 0))

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

(defn clj-encode-edn [item]
  (encode (pr-str item)))
(defn clj-decode-edn [data]
  (let [item (decode data)]
    (read-string (buffer->string item))))

(defn clj-encode-json [item]
  (encode (json/generate-string item)))
(defn clj-decode-json [data]
  (let [item (decode data)]
    (json/parse-string (buffer->string item) true)))

(defn remaining [^Buffer b] (.remaining b))

(defn get-from-buffer [^ByteBuffer b #^bytes bb] (.get b bb))

(def clj-encode-smile json/generate-smile)

(defn clj-decode-smile [data]
  (let [bb (byte-array (remaining data))]
    (get-from-buffer data bb)
    (json/parse-smile bb true)))

