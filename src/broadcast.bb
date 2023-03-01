#!/usr/bin/env bb
(ns broadcast
  (:require
   [common.core :refer :all]))

(def node-id (atom ""))
(def message-id (atom 0))
(def seen (atom #{}))
(def topology (atom {}))
(def pending
  "{broadcast-id {:msg msg
                  :to #{node-id}}}"
  (atom {}))

(defn next-message-id
  []
  (swap! message-id inc))

(defn retry-pending-loop
  []
  (future
   (loop []
     (doseq [{:keys [:msg :to]} (vals @pending)
             dest to]
       (send! (reply @node-id
                     dest
                     (assoc msg
                            :msg_id (next-message-id)))))
     (Thread/sleep 1000)
     (recur)
     #_(->> @pending
            vals
            (mapcat (fn [{:keys [:msg :to] :as all}]
                      (mapv #(reply @node-id
                                    %
                                    (assoc msg
                                           :msg_id (next-message-id)
                                           :retry true))
                            to)))))))

(defn process-request
  [input]
  (let [body (:body input)
        src (:src input)
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
      (let [broadcast-id (:msg_id body)
            neighbors (set (get @topology (keyword @node-id) []))
            message (:message body)
            broadcast-int-msg {:type "broadcast_int"
                               :broadcast_id broadcast-id
                               :known [@node-id]
                               :message message}]
        (swap! seen conj message)
        (when (seq neighbors)
          (swap! pending assoc broadcast-id {:to neighbors
                                             :msg broadcast-int-msg}))
        (conj
         (mapv
          #(reply @node-id
                  %
                  (assoc broadcast-int-msg
                         :msg_id (next-message-id)))
          neighbors)
         (reply @node-id
                src
                (assoc r-body
                       :type "broadcast_ok"))))
      "broadcast_int"
      (let [{:keys [:known :message :broadcast_id]} body
            message-to-save (dissoc body :retry)
            known (set known)
            neighbors (-> @topology
                          (get (keyword @node-id) [])
                          (->> (filter #(not (known %))))
                          set)]
        (printerr "broadcast int >>>>>>>>>>> " {:pending @pending
                                                :body body
                                                :neighbors neighbors})
        (swap! seen conj message)
        (when (seq neighbors)
          (swap! pending assoc broadcast_id {:to neighbors
                                             :msg message-to-save}))
        (conj
         (mapv
          #(reply @node-id
                  %
                  (assoc message-to-save
                         :msg_id (next-message-id)
                         :known (conj known @node-id)))
          neighbors)
         (reply @node-id
                src
                (assoc r-body
                       :type "broadcast_int_ok"
                       :broadcast_id broadcast_id
                       :msg_id (next-message-id)))))
      "broadcast_int_ok"
      (let [{:keys [:broadcast_id]} body]
        (printerr "broadcast int ok >>>>>>>>>>> " {:pending @pending
                                                   :body body})
        (swap! pending (fn [pending broadcast-id src]
                         (let [{:keys [:to]} (get pending broadcast-id)
                               remaining (disj to src)]
                           (if (seq remaining)
                             (assoc-in pending [broadcast-id :to] remaining)
                             (dissoc pending broadcast-id))))
               broadcast_id src)
        [])
      "read"
      (reply @node-id
             src
             (assoc r-body
                    :type "read_ok"
                    :messages @seen))
      "topology"
      (do
        (reset! topology (:topology body))
        (reply @node-id
               src
               (assoc r-body
                      :type "topology_ok"))))))


(defn -main
  "Read transactions from stdin and send output to stdout"
  [& args]
  (retry-pending-loop)
  (process-stdin #(-> %
                      parse-json
                      process-request
                      generate-json
                      printout)))


(-main)
