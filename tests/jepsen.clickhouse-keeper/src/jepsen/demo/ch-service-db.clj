(ns jepsen.service-keeper.db
  (:require [clojure.tools.logging :refer :all]
            [jepsen
             [control :as c]
             [db :as db]
             [util :as util :refer [meh]]]
;            [jepsen.clickhouse-keeper.constants :refer :all]
;            [jepsen.clickhouse-keeper.utils :refer :all]
            [clojure.java.io :as io]
            [jepsen.control.util :as cu]
            [jepsen.os.ubuntu :as ubuntu]))

(defn install-clickhouse
  [path]
  (info "Binary path chmod" path)
  (c/exec :chmod :+x path))

(defn prepare-dirs
  []
  (do
    (c/exec :mkdir :-p common-prefix)
    (c/exec :mkdir :-p coordination-data-dir)
    (c/exec :mkdir :-p coordination-logs-dir)
    (c/exec :mkdir :-p coordination-snapshots-dir)
    (c/exec :mkdir :-p bin-dir)
    (c/exec :mkdir :-p logs-dir)
    (c/exec :mkdir :-p configs-dir)
    (c/exec :touch stderr-file)
    (c/exec :chown :-R :root common-prefix)))

(defn cluster-config
  [test node config-template]
  (let [nodes (:nodes test)
        replacement-map {#"\{srv1\}" (get nodes 0)
                         #"\{srv2\}" (get nodes 1)
                         #"\{srv3\}" (get nodes 2)
                         #"\{srv4\}" (get nodes 3)
                         #"\{srv5\}" (get nodes 4)
                         #"\{id\}" (str (inc (.indexOf nodes node)))
                         #"\{host\}" (str node)}]
    (reduce #(clojure.string/replace %1 (get %2 0) (get %2 1)) config-template replacement-map)))

(defn install-configs
  [test node]
  (c/exec :echo (cluster-config test node (slurp (io/resource "config.xml"))) :> (str configs-dir "/config.xml")))

(defn collect-traces
  [test node]
  (let [pid (c/exec :pidof "clickhouse")]
    (c/exec :timeout :-s "KILL" "60" :gdb :-ex "set pagination off" :-ex (str "set logging file " logs-dir "/gdb.log") :-ex
            "set logging on" :-ex "backtrace" :-ex "thread apply all backtrace"
            :-ex "backtrace" :-ex "detach" :-ex "quit" :--pid pid :|| :true)))

(defn db
  [version reuse-binary]
  (reify db/DB
    (setup! [_ test node]
      (c/su
       (do
         (info "Preparing directories")
         (prepare-dirs)
         (if (or (not (cu/exists? binary-path)) (not reuse-binary))
           (do (info "Downloading clickhouse")
;               (install-downloaded-clickhouse (download-clickhouse version)))
               (install-clickhouse binary-path))
           (info "Binary already exsist on path" binary-path "skipping download"))
         (info "Installing configs")
         (install-configs test node)
         (info "Starting server")
         (start-clickhouse! node test)
         (info "ClickHouse started"))))

    (teardown! [_ test node]
      (info node "Tearing down clickhouse")
      (c/su
       (kill-clickhouse! node test)
       (if (not reuse-binary)
         (c/exec :rm :-rf binary-path))
       (c/exec :rm :-rf pid-file-path)
       (c/exec :rm :-rf coordination-data-dir)
       (c/exec :rm :-rf logs-dir)
       (c/exec :rm :-rf configs-dir)))

    db/LogFiles
    (log-files [_ test node]
      (c/su
       (if (cu/exists? pid-file-path)
         (do
           (info node "Collecting traces")
           (collect-traces test node))
         (info node "Pid files doesn't exists"))
       (kill-clickhouse! node test)
       (if (cu/exists? coordination-data-dir)
         (do
           (info node "data files exists, going to compress")
           (c/cd coordination-data-dir
                 (c/exec :tar :czf "data.tar.gz" "data")))))
      (let [common-logs [stderr-file (str logs-dir "/clickhouse-server.log") (str common-prefix "/data.tar.gz")]
            gdb-log (str logs-dir "/gdb.log")]
        (if (cu/exists? (str logs-dir "/gdb.log"))
          (conj common-logs gdb-log)
          common-logs)))))
