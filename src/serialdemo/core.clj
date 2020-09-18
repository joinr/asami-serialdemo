(ns serialdemo.core
  (:require [taoensso.nippy :as nippy]
            [asami.core :as d]
            [clojure.java [io :as io]]))
;;utilities
;;=========

;;serialize the database?
(defn ^bytes slurp-bytes 
  "Used in load-db! function."
  [path]
  (with-open [in (io/input-stream path)]
    (let [l (.length (io/file path))
          buf (byte-array l)]
      (.read in buf 0 l)
      buf)))

(defn spit-bytes 
  "Used in save-db! function."
  [^bytes ba target]
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
  "Used in load-and-connect! function."
 (nippy/thaw (slurp-bytes tgt)))

(defn save-db! 
  "Used in save-and-connect! function."
  [db tgt]
  (spit-bytes (nippy/freeze db) tgt))

(defn simple-uri 
  "Used to generate a asami link from a db name."
  [nm]
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

(defn add 
  "Giving a `c`onnection and `d`ata, 
   returns a future with the result of the txn" 
  [d c] 
  (d/transact c {:tx-data d}))

(defn query 
  "Giving a `db`, and a `q`uery, 
   return query result." 
  [q db] (d/q q db))


(comment 
;; Demo
; =======

;; Create a memory database and register it in the connection map in asami under "dbname"
(d/create-database "asami:mem://movies")


;; Returns a MemoryConnection, a record of {:name :state} where state is an atom of the actual in-memory database...
(def conn (d/connect (simple-uri "movies")))


;; Test data
(def first-movies 
  [{:movie/title "Explorers"
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

;; Add test data to connection.
(add first-movies conn)


;; Gets the underlying db.
(def db (d/db conn))


;; Find all movie titles
(query 
  '[:find ?movie-title
    :where [?m :movie/title ?movie-title]] 
  db) ;;=> All titles



;; Find movie relased in the 80s
(query 
  '[:find ?title
    :where 
    [?m :movie/title ?title]
    [?m :movie/release-year ?y]
    [(<= ?y 1989)]] 
  db) ;;=> (["Explorers"] 


;; Save data and close the connection.  Serialize the connection and save to a file
(save-and-close! conn :target "resources/movies.bin")


;; Reload saved data and connect to it...
(def new-conn (load-and-connect! "resources/movies.bin"))


;; Get the database snapshot from connection.
(def new-db (d/db new-conn))


;; Does the loaded database equals a fresh asami memory database (empty?)
(= new-db db)


;; Add new data to the database in memory
(add 
  '[{:movie/title "The Accidental Tourist"
   :movie/release-year 1988
   :movie/genre "drama"}] 
  new-conn)

;; reload and create a new snapshot
(def new-db+ (d/db new-conn))


(query 
  '[:find ?title
    :where [?m :movie/title ?title]] 
  new-db+)

;; is the the new change reflected in the connection?
(= new-db+ new-db)
;; Find movie relased in the 80s
(query 
  '[:find ?title
    :where 
    [?m :movie/title ?title]
    [?m :movie/release-year ?y]
    [(<= ?y 1989)]] 
  new-db+) ;;=> (["Explorers"] ["The Accidental Tourist"])

)
