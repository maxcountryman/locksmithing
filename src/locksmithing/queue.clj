(ns locksmithing.queue
  "Lock-free, non-blocking linked concurrent queue implementation.
  
  This is a Clojure implementation of the Michael-Scott lock-free queue. It is
  meant for demonstration purposes only and should not be used for anything
  but experimenetation. In particular, it is part of an effort to demonstrate
  validating linearizability of concurrent data structures using Knossos.
  
  This implementation is necessarily mutable!
  
  How does it work?
  
  This implementation follows closely the algorithm described in the paper
  'Simple, Fast, and Practical Non-Blocking and Blocking Concurrent Queue
  Algorithms'. It is a singly-linked list, with a 'dummy' head node, to ensure
  empty queues behave properly. Compare-And-Set (CAS) is used to ensure
  correct concurrent semantics.
  
  Initially both the head and the tail point to the same node. Nodes are then
  appended to the tail's next pointer as they are pushed onto the queue. The
  tail is continually swapped for the incoming node, such that a valid push is
  conditional upon the tail's next pointing to nil; when a non-nil value is
  found, an attempt is made to move the tail to its next pointer.
  
  Values are removed from the queue by looking at the head's next node
  (bearing in mind that the first node is a dummy node). When a non-nil value
  is found in this node it is removed (replaced with nil) and then the value
  returned.
  
  The push algorithm
  
  First a new node is constructed with contains the value we are to add to the
  queue. After this we enter loop wherein we bind the current tail and its
  next pointer to local vars. We then immediately make a consistency check,
  asking if the value we believe is tail is still the tail in the queue. If
  not, we recur.
  
  We then ask if the tail's next pointer we bound is nil. If it is, this means
  we are at the end of the tail; exactly where we want to be for new
  insertions. However if not, we will try to replace the tail in the queue
  with the bound next pointer we have, in effect setting us up to try again.
  
  Returning to the truthy case of the above, we try to set the tail pointer to
  our new node. If we succeed, this is the break point of the loop. The falsey
  case will trigger a recur.
  
  The pop algorithm
  
  Again we bind some locals: the queue's head and tail, as well as the first
  node that can contain a value, i.e. the next of head. Much like before, we
  check immediately for consistency between our bound head and the head in the
  queue, recuring when false.
  
  Passing the above case positively, we move on and ask if the bound head and
  tail are the same thing. If they are, the queue might be empty and we will
  ask if the first pointer is nil in which case the queue is empty and we
  return nil. Otherwise the tail has fallen behind, and we make an attempt to
  advance it and then recur.
  
  Now if the bound head and the tail are not the same thing, then we may have
  a value we can return. First we must try to read this value and bind it
  locally to ensure that it is not overwritten by a concurrent pop. If this
  value is not nil then we have a value to return and will do so, setting the
  node containing it to a nil value and exiting the loop. Otherwise it is a
  deleted element and we ignore it by recuring."
  (:require [clojure.walk :as walk])
  (:refer-clojure :exclude [pop!]))

;; Utils
(def recur->)

(defmacro loop-with-sleep
  "Similar in principle to loop, but looks for a special recur-> symbol. Where
  found, this is replaced with a let over the bindings, a sleep over a small
  random amount of time and a recur.
  
  The let over the bindings ensures that the original bindings are evaluated
  in sequential, rather than parallel ordering, before being passed to recur.
  
  Finally it is important to note that recur is always seeded with the same
  symbols that were passed to loop. The implication here is that the bindings
  will in some way mutate!"
  [bindings & body]
  {:pre [(and (vector? bindings)
              (even? (count bindings)))]}
  `(loop ~bindings
     ~@(walk/postwalk
         (fn find-recur-> [x#]
           (if (= x# 'recur->)
             `(let ~bindings
                (Thread/sleep (rand))
                (recur ~@(map first (partition 2 bindings))))
             x#))
         body)))

;; Queue
(definterface INode
  (getValue [])
  (setValue [v])
  (getNext []))

(deftype Node
  [^:volatile-mutable value
   ^:volatile-mutable ^clojure.lang.Atom next]
  INode
  (getValue [_] value)
  (setValue [this v] (set! value v) this)
  (getNext [_] next))

(defn- node
  "Given a value v and a successor s, constructs a new Node instance.
  
  Note that the successor is wrapped in an atom; this allows the successor to
  be manipulated atomically.
  
  Returns a Node instance.
  "
  [v s]
  (Node. v (atom s)))

(defn- get-next
  "Given an INode n, calls .getNext on it, returning the result."
  [^INode n]
  {:pre  [(instance? Node n)]
   :post [(instance? clojure.lang.Atom %)]}
  (.getNext n))

(defn- get-value
  "Given an INode n, calls .getValue on it, returning the result."
  [^INode n]
  {:pre  [(instance? Node n)]}
  (.getValue n))

(defn- set-value*
  "Given an INode n and a value to set x, calls .setValue with n and x as
  arguments, returning the result, a Node instance."
  [^INode n x]
  {:pre  [(instance? Node n)]
   :post [(instance? Node %)]}
  (.setValue n x))

(defn- set-value
  "Given an atom a containing a Node instance and a value v, swaps v into a
  using set-value*, returns the result of calling swap!."
  [^clojure.lang.Atom a v]
  {:pre  [(instance? clojure.lang.Atom a)
          (instance? Node @a)]}
  (swap! a set-value* v))

(definterface IQueue
  (getHead [])
  (getTail []))

(deftype Queue
  [^clojure.lang.Atom head
   ^clojure.lang.Atom tail]
  IQueue
  (getHead [_] head)
  (getTail [_] tail))

(defn queue
  "Constructs a new queue with the same empty node instance, wrapped in
  separate atoms, bound to head and tail.
  
  Note that the head and tail 'point' to the same Node instance, but because
  the node is wrapped in two different atoms, the fields can be manipulated
  indepdently of one another.
  
  Returns a Queue instance."
  []
  (let [n (node nil nil)]
    (Queue. (atom n) (atom n))))

(defn- get-head
  "Given an IQueue q, calls .getHead on q. Returns the result, an atom."
  [^IQueue q]
  {:pre  [(instance? Queue q)]
   :post [(instance? clojure.lang.Atom %)]}
  (.getHead q))

(defn- get-tail
  "Given an IQueue q, calls .getTail on q. Returns the result, an atom."
  [^IQueue q]
  {:pre  [(instance? Queue q)]
   :post [(instance? clojure.lang.Atom %)]}
  (.getTail q))

(defn- cas-next
  "Given an atom a, and INode cmp, and an INode v, calls compare-and-set! over
  the value of calling get-next on deref'd a, cmp, and v. Returns true or
  false."
  [^clojure.lang.Atom a ^INode cmp ^INode v]
  {:pre [(instance? clojure.lang.Atom a)
         (or (instance? Node cmp) (nil? cmp))
         (instance? Node v)]}
  (compare-and-set! (get-next @a) cmp v))

(defn- cas-head
  "Given an IQueue q, an INode cmp, and an INode v, calls compare-and-set! over
  the value of calling get-head on q, cmp, and v. Returns true or false."
  [^IQueue q ^INode cmp ^INode v]
  {:pre [(instance? Queue q)
         (instance? Node cmp)
         (instance? Node v)]}
  (compare-and-set! (get-head q) cmp v))

(defn- cas-tail
  "Given an IQueue q, an INode cmp, and an INode v, calls compare-and-set! over
  the value of calling get-tail on q, cmp, and v. Returns true or false."
  [^IQueue q ^INode cmp ^INode v]
  {:pre [(instance? Queue q)
         (instance? Node cmp)
         (instance? Node v)]}
  (compare-and-set! (get-tail q) cmp v))

(defn push!
  "Given an IQueue q and a value v, pushes the value onto q. Mutates q!"
  [^IQueue q v]
  ;; NOTE: Atoms are deref'd in the binding phase, to eliminate races.
  (let [n (node v nil)]
    (loop-with-sleep [t  (get-tail q)
                      t' @t
                      s  @(get-next @t)]

      (if (identical? t' @(get-tail q))
        (if (nil? s)
          (if (cas-next t s n)
            (do (cas-tail q t' n) true)
            recur->)
          (do (cas-tail q t' s) recur->))
        recur->))))

(defn pop!
  "Given an IQueue q, pops a value off of q. Mutates q!"
  [^IQueue q]
  ;; NOTE: Atoms are deref'd in the binding phase, to eliminate races.
  (loop-with-sleep [h  (get-head q)
                    h' @h
                    t  @(get-tail q)
                    f  (get-next @h)
                    f' @f]

    (if (identical? h' @(get-head q))
      (if (identical? h' t)
        (if (nil? f')
          nil
          (do (cas-tail q t f') recur->))
        (if (cas-head q h' f')
          (let [v (get-value f')]
            (if-not (nil? v)
              (do (set-value f nil) v)
              recur->))))
      recur->)))
