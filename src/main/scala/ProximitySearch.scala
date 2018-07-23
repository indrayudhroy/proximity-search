/**
  * Created by Indra on 1/27/2017.
  */

import scala.collection.mutable
import scala.io.Source
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.ArrayBuffer
import scala.io.StdIn.readLine
import scala.math.abs
import scala.util.control.Breaks._

object ProximitySearch {

  def main(args: Array[String]): Unit = {

    //Read in from file
    val fileLines = Source.fromFile(args(0)).getLines.toArray
    var tokenList = new ArrayBuffer[String]()

    case class docOffsetTuple(docID: String, position: Int) //Each tuple holds an occurrence of a term -- (docID, offset in doc)
    case class termDocTuple(term: String, doc: docOffsetTuple) //Tuple that holds a term, along with its occurrence
    var termList = new ListBuffer[termDocTuple]()

    //Populate the term list (list of termDocTuples)
    for (line <- fileLines) {
      tokenList ++= line.split("\\s+")
      val currentDocID = tokenList(0)
      tokenList.remove(0)
      var positionCounter = 0
      for (term <- tokenList) {
        val currentDocPositionTuple = new docOffsetTuple(currentDocID, positionCounter)
        termList += new termDocTuple(term, currentDocPositionTuple)
        positionCounter += 1
      }
      tokenList.clear()
    }

    //Sort
    termList = termList.sortBy(t => (t.term, t.doc.docID, t.doc.position))


    //hashMapOfTerms: maps terms to their postings lists

    val hashMapOfTerms = termList.groupBy(_.term).map{case (k, v) => (k, v.map(_.doc))}
    // Creating a map with term as key. Terms are mapped to aggregated list of occurence tuples

    var mapTermToFrequency = mutable.Map[String, Int]()
    hashMapOfTerms.keys.foreach { i =>
      mapTermToFrequency += (i -> hashMapOfTerms(i).length)
    }
    
    // The map stores terms mapped to the number of occurence tuples

    val inputQuery = readLine("Enter a query > ")

    val directionalToggle = readLine("Is this query directional? (y/n) > ")
    var queryTokens = new ArrayBuffer[String](0)
    queryTokens ++= inputQuery.split(" ")

    var directionalFlag = 0 //Is the query directional? 1: if true, 0: if false. Bi-directional by default

    if (directionalToggle.toLowerCase() == "y") {
      directionalFlag = 1
    }

    //Parse the binary query: term1: first word, term2: second word
    //k: proximity as defined by the /k operator
    val term1 = queryTokens(0)
    val term2 = queryTokens(2)
    val k = queryTokens(1)(1).toInt - '0'

    val postingsList1 = hashMapOfTerms(term1)
    val postingsList2 = hashMapOfTerms(term2)

    //For the given words, for each document they appear in,
    //map the docID to a list of offsets (positions in the document)
    val offsetMap1 = postingsList1.groupBy(_.docID).mapValues(_.map(_.position))
    val offsetMap2 = postingsList2.groupBy(_.docID).mapValues(_.map(_.position))

    //Points to postings list entries in the positional-intersect algorithm
    var ptr1 = 0
    var ptr2 = 0

    //Each solution that is part of the answer set is a Tuple
    //offset1 and offset2 refer to offsets for the two terms
    case class offsetTuple(docID: String, offset1: Int, offset2: Int)

    //answer: list of Tuples -- this is the final solution
    var answer = new ListBuffer[offsetTuple]()

    //positional-intersect algorithm:
    while (ptr1 < postingsList1.length && ptr2 < postingsList2.length) {
      if (postingsList1(ptr1).docID == postingsList2(ptr2).docID) {

        var L = new ListBuffer[Int]()

        val matchingDocID = postingsList1(ptr1).docID

        var offsetPtr1 = 0
        var offsetPtr2 = 0

        val positions1 = offsetMap1(matchingDocID)
        val positions2 = offsetMap2(matchingDocID)

        var offset1 = positions1(offsetPtr1)
        var offset2 = positions2(offsetPtr2)

        while (offsetPtr1 < positions1.length) {

          breakable {
            while (offsetPtr2 < positions2.length) {

              offset1 = positions1(offsetPtr1)
              offset2 = positions2(offsetPtr2)

              if (directionalFlag == 0) {
                if (abs(offset1 - offset2) <= k) {
                  L += offset2
                }
                else if (offset2 > offset1) {
                  break
                }
              }
              else {
                if (offset2 > offset1 && offset2 - offset1 <= k) {
                  L += offset2
                }
                else if (offset2 > offset1) {
                  break
                }
              }

              offsetPtr2 += 1

            } //end of inner while
          }

          while (L.length > 0 && abs(L(0) - offset1) > k) {
            L.remove(0)
          }
          for (ps <- L) {
            val solutionTuple = new offsetTuple(matchingDocID, offset1, ps)
            answer += solutionTuple
          }

          offsetPtr1 += 1

        } //end of outer while

        while (ptr1 < postingsList1.length - 1 && postingsList1(ptr1 + 1).docID == matchingDocID) {
          ptr1 += 1
        }
        ptr1 += 1

        while (ptr2 < postingsList2.length - 1 && postingsList2(ptr2 + 1).docID == matchingDocID) {
          ptr2 += 1
        }
        ptr2 += 1

      } //end of "if docIDs are same"

      else {
        if (postingsList1(ptr1).docID.compareTo(postingsList2(ptr2).docID) < 0) {
          while (ptr1 < postingsList1.length - 1 && postingsList1(ptr1 + 1).docID == postingsList1(ptr1).docID) {
            ptr1 += 1
          }
          ptr1 += 1
        }
        else {
          while (ptr2 < postingsList2.length - 1 && postingsList2(ptr2 + 1).docID == postingsList2(ptr2).docID) {
            ptr2 += 1
          }
          ptr2 += 1
        }
      }
    }

    if (answer.length == 0) {
      println("No matches.")
    }
    else {
      println()
      println("Answer: (0-indexed, e.g. position 1 = 2nd word) ")
      println()
      for (i <- answer) {
        println("<" + i.docID + ", " + i.offset1 + ", " + i.offset2 + ">")
        println("docID: " + i.docID)
        println("position of " + term1 + ": " + i.offset1 + ", position of " + term2 + ": " + i.offset2)
        println()
      }
    }
  }
}