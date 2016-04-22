package thymeflow.utilities.pqueue

/*
 * The contents of this file are subject to the terms of the Common Development
 * and Distribution License (the License). You may not use this file except in
 * compliance with the License.
 *
 * You can obtain a copy of the License at http://www.netbeans.org/cddl.html
 * or http://www.netbeans.org/cddl.txt.
 *
 * When distributing Covered Code, include this CDDL Header Notice in each file
 * and include the License file at http://www.netbeans.org/cddl.txt.
 * If applicable, add the following below the CDDL Header, with the fields
 * enclosed by brackets [] replaced by your own identifying information:
 * "Portions Copyrighted [year] [name of copyright owner]"
 *
 * The Original Software is GraphMaker. The Initial Developer of the Original
 * Software is Nathan L. Fiedler. Portions created by Nathan L. Fiedler
 * are Copyright (C) 1999-2008. All Rights Reserved.
 *
 * Contributor(s): Nathan L. Fiedler.
 *
 * $Id$
 */

/**
  * This class implements a Fibonacci heap data structure. Much of the
  * code in this class is based on the algorithms in Chapter 21 of the
  * "Introduction to Algorithms" by Cormen, Leiserson, Rivest, and Stein.
  * The amortized running time of most of these methods is O(1), making
  * it a very fast data structure. Several have an actual running time
  * of O(1). removeMin() and delete() have O(log n) amortized running
  * times because they do the heap consolidation.
  *
  * <p><strong>Note that this implementation is not synchronized.</strong>
  * If multiple threads access a set concurrently, and at least one of the
  * threads modifies the set, it <em>must</em> be synchronized externally.
  * This is typically accomplished by synchronizing on some object that
  * naturally encapsulates the set.</p>
  *
  * @author Nathan Fiedler
  */
object FiedlerFibonacciHeap {
  /**
    * Joins two Fibonacci heaps into a new one. No heap consolidation is
    * performed at this time. The two root lists are simply joined together.
    *
    * <p><em>Running time: O(1)</em></p>
    *
    * @param  H1 first heap
    * @param  H2 second heap
    * @return new heap containing H1 and H2
    */
  def union[E, W](H1: FiedlerFibonacciHeap[E, W], H2: FiedlerFibonacciHeap[E, W]): FiedlerFibonacciHeap[E, W] = {
    implicit val ordering = H1.ordering
    val H = new FiedlerFibonacciHeap[E, W]
    if (H1 != null && H2 != null) {
      H.min = H1.min
      if (H.min != null) {
        if (H2.min != null) {
          H.min.right.left = H2.min.left
          H2.min.left.right = H.min.right
          H.min.right = H2.min
          H2.min.left = H.min
          if (H2.ordering.compare(H2.min.key, H1.min.key) < 0) {
            H.min = H2.min
          }
        }
      }
      else {
        H.min = H2.min
      }
      H.n = H1.n + H2.n
    }
    H
  }

  /**
    * Implements a node of the Fibonacci heap. It holds the information
    * necessary for maintaining the structure of the heap. It acts as
    * an opaque handle for the data element, and serves as the key to
    * retrieving the data from the heap.
    *
    * @author Nathan Fiedler
    *
    *         Two-arg constructor which sets the data and key fields to the
    *         passed arguments. It also initializes the right and left pointers,
    *         making this a circular doubly-linked list.
    * @param  data data object to associate with this node
    * @param  key  key value for this data object
    */
  class Node[E, @specialized(Double, Int) W](var data: E, var key: W) {
    /** Parent node. */
    private[pqueue] var parent: FiedlerFibonacciHeap.Node[E, W] = null
    /** First child node. */
    private[pqueue] var child: FiedlerFibonacciHeap.Node[E, W] = null
    /** Right sibling node. */
    private[pqueue] var right: FiedlerFibonacciHeap.Node[E, W] = this
    /** Left sibling node. */
    private[pqueue] var left: FiedlerFibonacciHeap.Node[E, W] = this
    /** Number of children of this node. */
    private[pqueue] var degree: Int = 0
    /** True if this node has had a child removed since this node was
      * added to its parent. */
    private[pqueue] var mark: Boolean = false

    /**
      * Performs a cascading cut operation. Cuts this from its parent
      * and then does the same for its parent, and so on up the tree.
      *
      * <p><em>Running time: O(log n)</em></p>
      *
      * @param  min the minimum heap node, to which nodes will be added.
      */
    def cascadingCut(min: FiedlerFibonacciHeap.Node[E, W]) {
      val z: FiedlerFibonacciHeap.Node[E, W] = parent
      if (z != null) {
        if (mark) {
          z.cut(this, min)
          z.cascadingCut(min)
        }
        else {
          mark = true
        }
      }
    }

    /**
      * The reverse of the link operation: removes x from the child
      * list of this node.
      *
      * <p><em>Running time: O(1)</em></p>
      *
      * @param  x   child to be removed from this node's child list
      * @param  min the minimum heap node, to which x is added.
      */
    def cut(x: FiedlerFibonacciHeap.Node[E, W], min: FiedlerFibonacciHeap.Node[E, W]) {
      x.left.right = x.right
      x.right.left = x.left
      degree -= 1
      if (degree == 0) {
        child = null
      }
      else if (child eq x) {
        child = x.right
      }
      x.right = min
      x.left = min.left
      min.left = x
      x.left.right = x
      x.parent = null
      x.mark = false
    }

    /**
      * Make this node a child of the given parent node. All linkages
      * are updated, the degree of the parent is incremented, and
      * mark is set to false.
      *
      * @param  parent the new parent node.
      */
    def link(parent: FiedlerFibonacciHeap.Node[E, W]) {
      left.right = right
      right.left = left
      this.parent = parent
      if (parent.child == null) {
        parent.child = this
        right = this
        left = this
      }
      else {
        left = parent.child
        right = parent.child.right
        parent.child.right = this
        right.left = this
      }
      parent.degree += 1
      mark = false
    }
  }

}

class FiedlerFibonacciHeap[E, @specialized(Double, Int) W](implicit val ordering: Ordering[W]) {

  /** Points to the minimum node in the heap. */
  private var min: FiedlerFibonacciHeap.Node[E, W] = null

  /** Number of nodes in the heap. If the type is ever widened,
    * (e.g. changed to long) then recalcuate the maximum degree
    * value used in the consolidate() method. */
  private var n: Int = 0

  /**
    * Removes all elements from this heap.
    *
    * <p><em>Running time: O(1)</em></p>
    */
  def clear() {
    min = null
    n = 0
  }

  /**
    * Decreases the key value for a heap node, given the new value
    * to take on. The structure of the heap may be changed, but will
    * not be consolidated.
    *
    * <p><em>Running time: O(1) amortized</em></p>
    *
    * @param  x node to decrease the key of
    * @param  k new key value for node x
    * @throws  IllegalArgumentException
    * if k is larger than x.key value.
    */
  def decreaseKey(x: FiedlerFibonacciHeap.Node[E, W], k: W) {
    if (ordering.compare(k, x.key) > 0) {
      throw new IllegalArgumentException("cannot increase key value")
    }
    x.key = k
    val y: FiedlerFibonacciHeap.Node[E, W] = x.parent
    if (y != null && (ordering.compare(k, y.key) < 0)) {
      y.cut(x, min)
      y.cascadingCut(min)
    }
    if (ordering.compare(k, min.key) < 0) {
      min = x
    }
  }

  /**
    * Deletes a node from the heap given the reference to the node.
    * The trees in the heap will be consolidated, if necessary.
    *
    * <p><em>Running time: O(log n) amortized</em></p>
    *
    * @param  x node to remove from heap.
    */
  def delete(x: FiedlerFibonacciHeap.Node[E, W]) {
    val y: FiedlerFibonacciHeap.Node[E, W] = x.parent
    if (y != null) {
      y.cut(x, min)
      y.cascadingCut(min)
    }
    min = x
    removeMin()
  }

  /**
    * Removes the smallest element from the heap. This will cause
    * the trees in the heap to be consolidated, if necessary.
    *
    * <p><em>Running time: O(log n) amortized</em></p>
    *
    * @return data object with the smallest key.
    */
  def removeMin(): Option[FiedlerFibonacciHeap.Node[E, W]] = {
    val z: FiedlerFibonacciHeap.Node[E, W] = min
    if (z == null) {
      return None
    }
    if (z.child != null) {
      z.child.parent = null

      var x: FiedlerFibonacciHeap.Node[E, W] = z.child.right
      while (x ne z.child) {
        {
          x.parent = null
        }
        x = x.right
      }

      val minleft: FiedlerFibonacciHeap.Node[E, W] = min.left
      val zchildleft: FiedlerFibonacciHeap.Node[E, W] = z.child.left
      min.left = zchildleft
      zchildleft.right = min
      z.child.left = minleft
      minleft.right = z.child
    }
    z.left.right = z.right
    z.right.left = z.left
    if (z eq z.right) {
      min = null
    }
    else {
      min = z.right
      consolidate()
    }
    n -= 1
    Some(z)
  }

  /**
    * Consolidates the trees in the heap by joining trees of equal
    * degree until there are no more trees of equal degree in the
    * root list.
    *
    * <p><em>Running time: O(log n) amortized</em></p>
    */
  private def consolidate() {
    val A = scala.Array.ofDim[FiedlerFibonacciHeap.Node[E, W]](45)
    var start: FiedlerFibonacciHeap.Node[E, W] = min
    var w: FiedlerFibonacciHeap.Node[E, W] = min
    do {
      var x: FiedlerFibonacciHeap.Node[E, W] = w
      var nextW: FiedlerFibonacciHeap.Node[E, W] = w.right
      var d: Int = x.degree
      while (A(d) != null) {
        var y: FiedlerFibonacciHeap.Node[E, W] = A(d)
        if (ordering.compare(x.key, y.key) > 0) {
          val temp: FiedlerFibonacciHeap.Node[E, W] = y
          y = x
          x = temp
        }
        if (y eq start) {
          start = start.right
        }
        if (y eq nextW) {
          nextW = nextW.right
        }
        y.link(x)
        A(d) = null
        d += 1
      }
      A(d) = x
      w = nextW
    } while (w ne start)
    min = start
    for (a <- A) {
      if (a != null && ordering.compare(a.key, min.key) < 0) {
        min = a
      }
    }
  }

  def nonEmpty: Boolean = {
    !isEmpty
  }

  /**
    * Tests if the Fibonacci heap is empty or not. Returns true if
    * the heap is empty, false otherwise.
    *
    * <p><em>Running time: O(1)</em></p>
    *
    * @return true if the heap is empty, false otherwise.
    */
  def isEmpty: Boolean = {
    min == null
  }

  /**
    * Inserts a new data element into the heap. No heap consolidation
    * is performed at this time, the new node is simply inserted into
    * the root list of this heap.
    *
    * <p><em>Running time: O(1)</em></p>
    *
    * @param  x   data object to insert into heap.
    * @param  key key value associated with data object.
    * @return newly created heap node.
    */
  def insert(x: E, key: W): FiedlerFibonacciHeap.Node[E, W] = {
    val node: FiedlerFibonacciHeap.Node[E, W] = new FiedlerFibonacciHeap.Node[E, W](x, key)
    if (min != null) {
      node.right = min
      node.left = min.left
      min.left = node
      node.left.right = node
      if (ordering.compare(key, min.key) < 0) {
        min = node
      }
    }
    else {
      min = node
    }
    n += 1
    node
  }

  /**
    * Returns the smallest element in the heap. This smallest element
    * is the one with the minimum key value.
    *
    * <p><em>Running time: O(1)</em></p>
    *
    * @return heap node with the smallest key, or null if empty.
    */
  def getMin: FiedlerFibonacciHeap.Node[E, W] = {
    min
  }

  /**
    * Returns the size of the heap which is measured in the
    * number of elements contained in the heap.
    *
    * <p><em>Running time: O(1)</em></p>
    *
    * @return number of elements in the heap.
    */
  def size: Int = {
    n
  }
}