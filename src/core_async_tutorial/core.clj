(ns core-async-tutorial.core
  (:require [clojure.core.async :refer [chan <! >! go put! take! close! >!! <!! go-loop] :as async]
            [org.httpkit.client :as http]
            [clj-http.client :as client]
            [cheshire.core :as json :refer [parse-string]]
            [clojure.java.browse :as browse]
            [oauth.client :as oauth])
  (:import (java.io InputStreamReader BufferedReader)))

(browse/browse-url "http://www.webcomrades.com")
(browse/browse-url "http://www.panenka76.com")

;; basics

(def bytes (chan 1))

(put! bytes 1)

(take! bytes (partial println "Got:"))

(>!! bytes 1)

(<!! bytes)

(go
  (println "Got:" (<! bytes)))

;; Requirement: Clojure 1.7
;; Transducers wooo

(def inc-chan (chan 1 (comp (map inc)
                            (filter odd?))))

(put! inc-chan 2)

(<!! inc-chan)





;; typical examples of async programming
;; - interaction with remote systems (http API, sockets, ...)
;; - push architecture

;; http-kit client

(http/get "https://twitter.com")

@(http/get "https://twitter.com")

(http/get "https://twitter.com" println)

(let [c (chan 1)]
  (http/get "https://twitter.com" #(go (>! c %)
                                       (close! c)))
  (println (<!! c)))



(defn http-get
  [req c]
  (http/request req #(go (>! c %)
                         (close! c))))

;; HTTP API: The Echo Nest (http://the.echonest.com/)

(browse/browse-url "http://the.echonest.com/")

(def api-key "CHANGE_THIS")

(defn build-http-request
  [request]
  (-> request
      (update :query-params assoc
              "api_key" api-key
              "format" "json")
      (assoc-in [:query-params "api_key"] api-key)
      (update :url (partial str "http://developer.echonest.com/api/v4"))))

(defn search-artist
  [name]
  {:url          "/artist/search"
   :query-params {:name    name
                  :results 1}})

(let [response-chan (chan 1)]
  (http-get (build-http-request (search-artist "metallica")) response-chan)
  (<!! response-chan))

(let [response-chan (chan 1)]
  (http-get (build-http-request (search-artist "metallica")) response-chan)
  (-> response-chan
      <!!
      :body
      (parse-string true)
      :response
      :artists
      first))

;; ok, now find similar artists

(defn similar-artists
  [artist]
  {:url          "/artist/similar"
   :query-params (select-keys artist [:id])})

(def get-data (comp
                :response
                #(parse-string % true)
                :body
                (fn [{:keys [status error] :as response}] (if (or error (not= status 200))
                                                            (throw (ex-info "Request unsuccesful" response))
                                                            response))
                ))

(let [search-c (chan 1)]
  (http-get (build-http-request (search-artist "metallica")) search-c)
  (let [artist (-> search-c
                   <!!
                   get-data
                   :artists
                   first)
        similar-c (chan 1)]
    (http-get (build-http-request (similar-artists artist)) similar-c)
    (-> similar-c
        <!!
        get-data
        :artists)))

;; ok, now find the latest news article and the most popular song for each of these artists.

{:id          "ARLO7621187FB387FB",
 :name        "Slayer",
 :hit         {:id    "SOAZBON1392646FA8F"
               :title "Raining Blood"}
 :latest-news {:url        "http://www.revolvermag.com/news/slayer-finish-recording-new-album-announce-small-tour.html",
               :id         "741e3e6914f0715ccc23b95d352be0b8",
               :date_found "2015-04-21T00:00:00",
               :name       "Slayer Finish Recording New Album, Announce Small Tour"}}

;; bleeeh.

;; pipelines!

(defn most-popular-song
  [artist]
  {:url          "/song/search"
   :query-params {:artist_id (:id artist)
                  :sort      "song_hotttnesss-desc"
                  :results   1}})

(defn latest-news
  [artist]
  {:url          "/artist/news"
   :query-params {:id      (:id artist)
                  :results 1}})

(let [c (chan 1)]
  (http-get (build-http-request (most-popular-song {:id   "ARLO7621187FB387FB",
                                                    :name "Slayer"}))
            c)
  (-> c
      <!!
      get-data
      :songs
      first
      (select-keys [:id :title])))

(defn add-news-and-popular
  [request-fn]
  (fn [artist c]
    (let [news (chan 1 (map (comp #(select-keys % [:url :id :date_found :name]) first :news get-data)))
          song (chan 1 (map (comp #(select-keys % [:id :title]) first :songs get-data)))]
      (request-fn (build-http-request (latest-news artist)) news)
      (request-fn (build-http-request (most-popular-song artist)) song)
      (go (>! c (assoc artist
                  :hit (<! song)
                  :news (<! news)))
          (close! c)))))

(defn get-similar
  [request-fn n]
  (fn [artist c]
    (let [similar (chan 1024 (mapcat (comp :artists get-data)))
          similar-with-news-and-popular (chan 1024)]
      (request-fn (build-http-request (similar-artists artist)) similar)
      (async/pipeline-async n similar-with-news-and-popular (add-news-and-popular request-fn) similar)
      (go (>! c (assoc artist :similar (<! (async/into [] similar-with-news-and-popular))))
          (close! c)))))

(defn news-and-popular-song-for-similar-artists-pipeline
  "in: contains Strings that represent artist names
  out: will find similar artists and their most popular song and latest news article"
  [request-fn n out in]
  (let [search-reqs (chan 1024)
        artists (chan 1024 (map (comp first :artists get-data)))]
    (async/pipeline n search-reqs (comp (map (partial search-artist))
                                        (map build-http-request))
                    in)
    (async/pipeline-async n artists request-fn search-reqs)
    (async/pipeline-async n out (get-similar request-fn n) artists)))

(let [in (chan 10)
      out (chan 10)]
  (news-and-popular-song-for-similar-artists-pipeline http-get 1 out in)
  (async/onto-chan in ["metallica"])
  (<!! (async/into [] out)))



(defn throttle
  "limit: number of messages per period of time
  period: amount of time in ms"
  [limit period]
  (let [c (chan (async/dropping-buffer limit))]
    (go-loop []
      (<! (async/onto-chan c (repeat limit :token) false))
      (<! (async/timeout period))
      (recur))
    c))

(let [t (throttle 5 1000)]
  (<!! (async/into [] (async/take 20 t))))

(defn throttled-get
  [throttle]
  (fn [req c]
    (go (<! throttle)
        (http-get req c))))

(let [in (chan 10)
      out (chan 10)
      throttle (throttle 20 (* 60 1000))]
  (news-and-popular-song-for-similar-artists-pipeline (throttled-get throttle) 1 out in)
  (async/onto-chan in ["queen" "metallica"])
  (<!! (async/into [] out)))



(def consumer-key "CHANGE_THIS")
(def consumer-secret "CHANGE_THIS")

(def consumer (oauth/make-consumer consumer-key
                                   consumer-secret
                                   "https://api.twitter.com/oauth/request_token"
                                   "https://api.twitter.com/oauth/access_token"
                                   "https://api.twitter.com/oauth/authorize"
                                   :hmac-sha1))

(def request-token (oauth/request-token consumer))

(browse/browse-url (oauth/user-approval-uri consumer
                                            (:oauth_token request-token)))

(def access-token-response (oauth/access-token consumer
                                               request-token
                                               "2119728"))

(defn twitter-request
  [access-token-response request]
  (let [credentials (oauth/credentials consumer
                                       (:oauth_token access-token-response)
                                       (:oauth_token_secret access-token-response)
                                       (:method request)
                                       (:url request)
                                       (:query-params request))]
    (update request :query-params merge credentials)))

@(http/request
   (twitter-request access-token-response
                    {:method       :get
                     :url          "https://api.twitter.com/1.1/statuses/user_timeline.json"
                     :query-params {:count       100
                                    :screen_name "twitterapi"}})
   identity)

(defn get-stream
  [request out]
  (async/thread
    (let [{:keys [body]} (client/request request)
          chars (char-array 1000)
          reader (InputStreamReader. body "UTF-8")]
      (loop [read (.read reader chars 0 1000)]
        (if (= -1 read)
          (async/close! out)
          (when (>!! out (vec (aclone chars)))
            (recur (.read reader chars 0 1000))))))))

(defn partition-all-xf
  [n step]
  (fn [rf]
    (let [a (java.util.ArrayList. n)]
      (fn
        ([] (rf))
        ([result]
         (let [result (if (.isEmpty a)
                        result
                        (let [v (vec (.toArray a))]
                          ;;clear first!
                          (.clear a)
                          (unreduced (rf result v))))]
           (rf result)))
        ([result input]
         (.add a input)
         (if (= n (.size a))
           (let [v (vec (.toArray a))]
             (dotimes [_ step]
               (.remove a (int 0)))
             (rf result v))
           result))))))

(def convert
  (comp
    cat
    (partition-all-xf 2 1)
    (partition-by (partial = [\return \newline]))
    (filter (partial not= [[\return \newline]]))
    (map (partial map first))
    (map (partial apply str))
    (map #(json/parse-string % true))
    ))



(def messages (chan 1
                    convert
                    println
                    ))

(get-stream (twitter-request access-token-response
                             {:method :get
                              :url    "https://stream.twitter.com/1.1/statuses/sample.json"
                              ;:query-params {"delimited" "length"}
                              :as     :stream})
            messages)


(<!! (async/into [] (async/take 5 messages)))

(def hashtags (comp (partial map :text) :hashtags :entities))

(defn duplicate-by-hashtag
  [tweet]
  (for [hashtag (hashtags tweet)]
    (assoc tweet :pub-topic hashtag)))

(defn publish-by-hashtag
  [msgs]
  (let [by-hashtag (chan 1)]
    (async/pipeline 10 by-hashtag (mapcat duplicate-by-hashtag) msgs)
    (async/pub by-hashtag :pub-topic)))

(def pub (publish-by-hashtag messages))

(def askseanChan (chan 1))

(async/sub pub "asksean" askseanChan)

(<!! (async/into [] (async/take 1 askseanChan)))

(async/close! messages)


(comment
  (async/close! messages)

  (<!! (async/into [] messages))

  )
