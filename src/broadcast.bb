#!/usr/bin/env bb
(ns broadcast
  (:require
   [common.core :refer :all]))

(def node-id (atom ""))
(def message-id (atom 0))
(def seen (atom #{}))
(def topology (atom {}))

(defn next-message-id
  []
  (swap! message-id inc))

(defn process-request
  [input]
  (let [body (:body input)
        r-body {:msg_id (next-message-id)
                :in_reply_to (:msg_id body)}]
    (printerr {:input input})
    (case (:type body)
      "init"
      (do
        (reset! node-id (:node_id body))
        (reply @node-id
               (:src input)
               (assoc r-body :type "init_ok")))
      "broadcast"
      (do
        (swap! seen conj (:message body))
        (conj
         (mapv
          #(reply @node-id
                  %
                  {:type "broadcast_int"
                   :msg_id (next-message-id)
                   :known [@node-id]
                   :message (:message body)})
          (get @topology (keyword @node-id) []))
         (reply @node-id
                (:src input)
                (assoc r-body
                       :type "broadcast_ok"))))
      "broadcast_int"
      (let [{:keys [:known :message]} body
            known (set known)
            neighbors (-> @topology
                          (get (keyword @node-id) [])
                          (->> (filter #(not (known %)))))]
        (swap! seen conj message)
        (mapv
          #(reply @node-id
                  %
                  {:type "broadcast_int"
                   :msg_id (next-message-id)
                   :known (conj known @node-id)
                   :message (:message body)})
          neighbors))
      "read"
      (reply @node-id
             (:src input)
             (assoc r-body
                    :type "read_ok"
                    :messages @seen))
      "topology"
      (do
        (reset! topology (:topology body))
        (reply @node-id
               (:src input)
               (assoc r-body
                      :type "topology_ok"))))))


(defn -main
  "Read transactions from stdin and send output to stdout"
  [& args]
  (process-stdin #(-> %
                      parse-json
                      process-request
                      generate-json
                      printout)))


(-main)
