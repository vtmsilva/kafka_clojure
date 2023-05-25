(ns treinamento.core
  (:import
   [java.util Properties]
   [org.apache.kafka.common.serialization Serdes Serde Serializer Deserializer StringSerializer StringDeserializer]
   [org.apache.kafka.streams KafkaStreams StreamsConfig Topology]
   [org.apache.kafka.streams.processor Processor ProcessorSupplier To])
  (:require
   [taoensso.timbre :as log]
   [clojure.data.json :as json])
  (:gen-class))

(deftype Desserializador []
  Deserializer
  (close [_])
  (configure [_ configs isKey])
  (deserialize [_ topic data]
    (json/read-str (.deserialize (StringDeserializer.) topic data) :key-fn keyword)))

(deftype Serializador []
  Serializer
  (close [_])
  (configure [_ configs isKey])
  (serialize [_ topic data]
    (.serialize (StringSerializer.) topic (json/write-str data))))

(deftype Serde-personalizado []
  Serde
  (close [_])
  (configure [_ configs isKey])
  (deserializer [_] (Desserializador.))
  (serializer [_] (Serializador.)))

(def props
  (doto (Properties.)
    (.putAll
     {StreamsConfig/APPLICATION_ID_CONFIG             "trt-topology"
      StreamsConfig/BOOTSTRAP_SERVERS_CONFIG          "host.docker.internal:9094"
      StreamsConfig/DEFAULT_KEY_SERDE_CLASS_CONFIG     (.getClass (Serdes/String))
      StreamsConfig/DEFAULT_VALUE_SERDE_CLASS_CONFIG   (.getClass (Serde-personalizado.))})))

(deftype Processador [^:volatile-mutable context]
  Processor
  (close [_])
  (init [_ c]
    (set! context c))
  (process [_ k msg]
    (case (.topic context)
      "comando.radar"
      (when (= (:status msg) "pendente")
        (if (<= (:velocidade msg) 100)
          (do
            (log/info "VEICULO LIBERADO " msg)
            (.forward context (:placa msg) (assoc msg :status "executado") (To/child "cmd-radar")))
          (do
            (log/info "VEICULO MULTADO, MSG ENCAMINHADA PARA O TÓPICO multa")
            (.forward context (:placa msg) msg (To/child "cmd-multa")))))

      "multa"
      (when (= (:status msg) "pendente")
        (cond
          (<= (:velocidade msg) 110)
          (do
            (log/info "MULTA " (assoc msg :status "executado" :multa 150.00 :pontos 5))
            (.forward context k (assoc msg :status "executado" :multa 150.00 :pontos 5) (To/child "cmd-radar"))
            (.forward context k (assoc msg :status "executado" :multa 150.00 :pontos 5) (To/child "cmd-relatorio")))
          (<= (:velocidade msg) 130)
          (do
            (log/info "MULTA " (assoc msg :status "executado" :multa 250.00 :pontos 7))
            (.forward context k (assoc msg :status "executado" :multa 250.00 :pontos 7) (To/child "cmd-radar"))
            (.forward context k (assoc msg :status "executado" :multa 250.00 :pontos 7) (To/child "cmd-relatorio")))
          (> (:velocidade msg) 130)
          (do
            (log/info "MULTA " (assoc msg :status "executado" :multa 300.00 :pontos 12))
            (.forward context k (assoc msg :status "executado" :multa 300.00 :pontos 12) (To/child "cmd-radar"))
            (.forward context k (assoc msg :status "executado" :multa 300.00 :pontos 12) (To/child "cmd-relatorio")))))

      "relatorio"
      (spit "relatorio.txt" (str k ": " msg "\n") :append true)

      nil)))

(deftype Suplier-processador []
  ProcessorSupplier
  (get [_]
    (Processador. nil)))

(defn topology
  []
  (doto (Topology.)
    (.addSource     "cmd-in"                              (into-array String ["comando.radar" "multa" "relatorio"]))
    (.addProcessor  "processador" (Suplier-processador.)  (into-array String ["cmd-in"]))
    (.addSink       "cmd-radar" "comando.radar"                      (into-array String ["processador"]))
    (.addSink       "cmd-multa" "multa"                              (into-array String ["processador"]))
    (.addSink       "cmd-relatorio" "relatorio"                      (into-array String ["processador"]))))

(defn -main [& args]
  (.start (KafkaStreams. (topology) props)))
