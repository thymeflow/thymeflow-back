package thymeflow.utilities.pqueue

/*
 * JGraphT : a free Java graph-theory library
 *
 *
 * Project Info:  http://jgrapht.sourceforge.net/
 * Project Creator:  Barak Naveh (barak_naveh@users.sourceforge.net)
 *
 * (C) Copyright 2003-2007, by Barak Naveh and Contributors.
 *
 * This library is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation; either version 2.1 of the License, or
 * (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 * or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public
 * License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this library; if not, write to the Free Software Foundation,
 * Inc.,
 * 59 Temple Place, Suite 330, Boston, MA 02111-1307, USA.
 */

/* --------------------------
 * FibonnaciHeap.scala
 * --------------------------
 * (C) Copyright 1999-2003, by Nathan Fiedler and Contributors.
 *
 * Original Author:  Nathan Fiedler
 * Contributor(s):   John V. Sichi
*/

/**
  * This class implements a Fibonacci heap data structure. Much of the code in
  * this class is based on the algorithms in the "Introduction to Algorithms"by
  * Cormen, Leiserson, and Rivest in Chapter 21. The amortized running time of
  * most of these methods is O(1), making it a very fast data structure. Several
  * have an actual running time of O(1). removeMin() and delete() have O(log n)
  * amortized running times because they do the heap consolidation.
  *
  * <p><b>Note that this implementation is not synchronized.</b> If multiple
  * threads access a set concurrently, and at least one of the threads modifies
  * the set, it <i>must</i> be synchronized externally. This is typically
  * accomplished by synchronizing on some object that naturally encapsulates the
  * set.</p>
  *
  * @author Nathan Fiedler, John V. Sichi, David Montoya
  */

object FibonacciHeap {
  /**
    * Joins two Fibonacci heaps into a new one. No heap consolidation is
    * performed at this time. The two root lists are simply joined together.
    *
    * <p><em>Running time: O(1)</em></p>
    *
    * @param  heap1 first heap
    * @param  heap2 second heap
    * @return new heap containing H1 and H2
    */
  def union[E, W](heap1: FibonacciHeap[E, W], heap2: FibonacciHeap[E, W]): FibonacciHeap[E, W] = {
    implicit val ordering = heap1.ordering
    val newHeap = new FibonacciHeap[E, W]
    if (heap1 != null && heap2 != null) {
      newHeap.min = heap1.min
      if (newHeap.min != null) {
        if (heap2.min != null) {
          newHeap.min.right.left = heap2.min.left
          heap2.min.left.right = newHeap.min.right
          newHeap.min.right = heap2.min
          heap2.min.left = newHeap.min
          if (heap2.ordering.compare(heap2.min.key, heap1.min.key) < 0) {
            newHeap.min = heap2.min
          }
        }
      }
      else {
        newHeap.min = heap2.min
      }
      newHeap.n = heap1.n + heap2.n
    }
    newHeap
  }

  /**
    * Implements a node of the Fibonacci heap. It holds the information necessary
    * for maintaining the structure of the heap. It also holds the reference to the
    * key value (which is used to determine the heap structure).
    *
    * @author Nathan Fiedler
    *
    *         Initializes the right and left pointers, making this
    *         a circular doubly-linked list.
    * @param data data for this node
    * @param key  initial key for node
    */
  class Node[E, @specialized(Double, Int) W](var data: E, var key: W) {
    /** Parent node. */
    private[pqueue] var parent: FibonacciHeap.Node[E, W] = null
    /** First child node. */
    private[pqueue] var child: FibonacciHeap.Node[E, W] = null
    /** Right sibling node. */
    private[pqueue] var right: FibonacciHeap.Node[E, W] = this
    /** Left sibling node. */
    private[pqueue] var left: FibonacciHeap.Node[E, W] = this
    /** Number of children of this node. */
    private[pqueue] var degree: Int = 0
    /** True if this node has had a child removed since this node was
      * added to its parent. */
    private[pqueue] var mark: Boolean = false

    /**
      * Performs a cascading cut operation. Cuts this from its parent
      * and then does the same for its parent, and so on up the tree.
      *
      * <p><em>Running time: O(log n); O(1) excluding the recursion</em></p>
      *
      * @param min the minimum heap node
      */
    def cascadingCut(min: FibonacciHeap.Node[E, W]) {
      val z: FibonacciHeap.Node[E, W] = parent
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
    def cut(x: FibonacciHeap.Node[E, W], min: FibonacciHeap.Node[E, W]) {
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
    def link(parent: FibonacciHeap.Node[E, W]) {
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

class FibonacciHeap[E, @specialized(Double, Int) W](implicit val ordering: Ordering[W]) {

  /** Points to the minimum node in the heap. */
  private var min: FibonacciHeap.Node[E, W] = null

  /** Number of nodes in the heap. */
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
    * Decreases the key value for a heap node, given the new value to take on.
    * The structure of the heap may be changed and will not be consolidated.
    *
    * <p><em>Running time: O(1) amortized</em></p>
    *
    * @param  x node to decrease the key of
    * @param  k new key value for node x
    * @throws  IllegalArgumentException
    * if k is larger than x.key value.
    */
  def decreaseKey(x: FibonacciHeap.Node[E, W], k: W) {
    if (ordering.compare(k, x.key) > 0) {
      throw new IllegalArgumentException("cannot increase key value")
    }
    x.key = k
    val y: FibonacciHeap.Node[E, W] = x.parent
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
  def delete(x: FibonacciHeap.Node[E, W]) {
    val y: FibonacciHeap.Node[E, W] = x.parent
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
    * @return node with the smallest key.
    */
  def removeMin(): Option[FibonacciHeap.Node[E, W]] = {
    val z: FibonacciHeap.Node[E, W] = min
    if (z == null) {
      return None
    }
    if (z.child != null) {
      z.child.parent = null

      var x: FibonacciHeap.Node[E, W] = z.child.right
      while (x ne z.child) {
        {
          x.parent = null
        }
        x = x.right
      }

      val minleft: FibonacciHeap.Node[E, W] = min.left
      val zchildleft: FibonacciHeap.Node[E, W] = z.child.left
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
    val A = scala.Array.ofDim[FibonacciHeap.Node[E, W]](45)
    var start: FibonacciHeap.Node[E, W] = min
    var w: FibonacciHeap.Node[E, W] = min
    do {
      var x: FibonacciHeap.Node[E, W] = w
      var nextW: FibonacciHeap.Node[E, W] = w.right
      var d: Int = x.degree
      while (A(d) != null) {
        var y: FibonacciHeap.Node[E, W] = A(d)
        if (ordering.compare(x.key, y.key) > 0) {
          val temp: FibonacciHeap.Node[E, W] = y
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
  def insert(x: E, key: W): FibonacciHeap.Node[E, W] = {
    val node: FibonacciHeap.Node[E, W] = new FibonacciHeap.Node[E, W](x, key)
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
  def getMin: FibonacciHeap.Node[E, W] = {
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