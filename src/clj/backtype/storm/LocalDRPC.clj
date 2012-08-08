(ns backtype.storm.LocalDRPC
  (:require [backtype.storm.daemon [drpc :as drpc]])
  (:use [backtype.storm util])
  (:import [backtype.storm.utils  ServiceRegistry])
  (:gen-class
   :init init
   :implements [backtype.storm.ILocalDRPC]
   :constructors {[] []}
   :state state))

(defn get-handler [this]
  (:handler (.state this)))

(defn get-id [this]
  (:service-id (.state this)))

(defn -init []
  (let [handler (drpc/service-handler)
        id (ServiceRegistry/registerService handler)]
    [[] {:service-id id :handler handler}]))

(defn -execute [this func funcArgs]
  (-> (get-handler this)
      (.execute func funcArgs)))

(defn -executeBinary [this func funcArgs]
  (-> (get-handler this)
      (.executeBinary func funcArgs)))

(defn -result [this id result]
  (-> (get-handler this)
      (.result id result)))

(defn -fetchRequest [this func]
  (-> (get-handler this)
      (.fetchRequest func)))

(defn -failRequest [this id]
  (-> (get-handler this)
      (.failRequest id)))

(defn -getServiceId [this]
  (get-id this))

(defn -shutdown [this]
  (ServiceRegistry/unregisterService (get-id this))
  (.shutdown (get-handler this)))
