(ns serialdemo.core
  (:require [taoensso.nippy :as nippy]
            [asami.core :as d]
            [clojure.java [io :as io]]))
;;utilities
;;=========

;;serialize the database?
(defn ^bytes slurp-bytes [path]
  (with-open [in (io/input-stream path)]
    (let [l (.length (io/file path))
          buf (byte-array l)]
      (.read in buf 0 l)
      buf)))

(defn spit-bytes [^bytes ba target]
  (with-open [out (io/output-stream target)]
    (.write out ba)))

(nippy/extend-freeze asami.memory.MemoryConnection :asami/connection
   [x data-output]
   (nippy/freeze-to-out!  data-output
      {:name  (get x :name)
       :state (deref (get x :state))}))

(nippy/extend-thaw :asami/connection ; Same type id
   [data-input]
   (let [{:keys [name state]} (nippy/thaw-from-in!  data-input)]
     (asami.memory/->MemoryConnection name (atom state))))


(defn load-db! [tgt]
 (nippy/thaw (slurp-bytes tgt)))

(defn save-db! [db tgt]
  (spit-bytes (nippy/freeze db) tgt))

(defn simple-uri [nm]
  (str  "asami:mem://" nm))

(defn load-and-connect! [tgt]
  (let [conn (load-db! tgt)
        id   (get conn :name)
        uri  (simple-uri id)]
    (println [:loading tgt])
    (println [:reconnecting id])
    (swap! d/connections assoc uri conn)
    conn))

(defn save-and-close! [db & {:keys [target]}]
  (let [tgt (or target (str (get db :name) ".bin"))]
    (println [:saving (:name db) :connection :to tgt])
    (save-db! db tgt)
    (println [:closing (simple-uri (get db :name)) :connection])
    (d/delete-database (simple-uri (get db :name)))))


;;demo
;;====
(def db-uri "asami:mem://dbname")
;;this will create a memory database and register it in
;;the connection map in asami under dbname...
(d/create-database db-uri)

;;this returns a MemoryConnection, just a
;;record of {:name :state} where state is an
;;atom of the actual in-memory database...
(def conn (d/connect db-uri))

(def first-movies [{:movie/title "Explorers"
                    :movie/genre "adventure/comedy/family"
                    :movie/release-year 1985}
                   {:movie/title "Demolition Man"
                    :movie/genre "action/sci-fi/thriller"
                    :movie/release-year 1993}
                   {:movie/title "Johnny Mnemonic"
                    :movie/genre "cyber-punk/action"
                    :movie/release-year 1995}
                   {:movie/title "Toy Story"
                    :movie/genre "animation/adventure"
                    :movie/release-year 1995}])

;;returns a future with the result of the txn I think.
(d/transact conn {:tx-data first-movies})

;;gets the underlying db.
(def db (d/db conn))

;;query...
(d/q '[:find ?movie-title
       :where [?m :movie/title ?movie-title]] db)

;;(["Explorers"] ["Demolition Man"] ["Johnny Mnemonic"] ["Toy Story"])

;;Now let's depart from the original asami demo.
;;We have a database in memory.  We can serialize the connection
;;using our helper functions...

;;Let's save it and close the connection.
(save-and-close! conn :target "saved.bin")
;;[:saving dbname :connection :to saved.bin]
;;[:closing asami:mem://dbname :connection]

;;Let's reload it and connect to it...
(def new-conn (load-and-connect!  "saved.bin"))
(def new-db (d/db new-con))

(= new-db db)
;;true

(d/q '[:find ?movie-title
       :where [?m :movie/title ?movie-title]] new-db)
;;(["Explorers"] ["Demolition Man"] ["Johnny Mnemonic"] ["Toy Story"])
