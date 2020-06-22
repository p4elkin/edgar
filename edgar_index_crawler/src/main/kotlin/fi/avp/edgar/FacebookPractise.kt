package fi.avp.edgar

import java.lang.Math.max
import java.util.ArrayDeque
import java.util.*
import kotlin.collections.ArrayList
import kotlin.math.min

// start moving from both ends until they meet (e >= s)
// maintain indices separately, skip whitespace and punctuation
// compare the lower-case chars
class IsArbitraryStringPalindrome {

    fun isPalindrome(s: String): Boolean {
        var start = 0
        var end: Int = s.length - 1

        while (start < end) {
            var nextNonAlphabeticChar = start
            while (nextNonAlphabeticChar < end && !isAlphbetic(s[nextNonAlphabeticChar])) {
                nextNonAlphabeticChar++;
            }

            var previousAlphabeticChar = end;
            while (previousAlphabeticChar > nextNonAlphabeticChar && !isAlphbetic(s[previousAlphabeticChar])) {
                previousAlphabeticChar--;
            }

            val leftChar = s[nextNonAlphabeticChar]
            val rightChar = s[previousAlphabeticChar]

            if (nextNonAlphabeticChar <= previousAlphabeticChar && rightChar.toLowerCase() != leftChar.toLowerCase()) {
                return false;
            }

            start = nextNonAlphabeticChar + 1;
            end = previousAlphabeticChar -1;
        }

        return true
    }

    private fun isAlphbetic(char: Char): Boolean {
        return char.isLetter()
    }
}

data class Event(val id: String, val timestamp: Int)

class CallStackExecutionTimeCalculator {

    fun calculateExecutionTime(events: MutableList<String>): List<Int> {
        val results: MutableList<Int> = ArrayList()
        val stack = ArrayDeque<Int>()
        events.forEach {
            val eventTokens  = it.split(":")
            val functionId = eventTokens[0].toInt()
            val isStart = eventTokens[1] == "start"
            val timestamp = eventTokens[2].toInt()

            if (isStart) {
                stack.peek()?.let {
                    results[stack.peek()] = timestamp - results[stack.peek()]
                }
                stack.push(functionId)
                results.add(timestamp)
            } else {
                val currentFunctionId = stack.pop()
                results[currentFunctionId] = timestamp - results[currentFunctionId] + 1
                stack.peek()?.let {
                    results[stack.peek()] -= results[currentFunctionId]
                }
            }
        }
        return results
    }
}

class FindAllIncreasingSubsequences {

    // 5 7 4 2 3 9
    fun allIncreasingSubsequences(nums: List<Int>, distinct: Set<Int>, index: Int): Set<List<Int>> {
        return if (index > 0) {
            val allIncreasingSubsequences =
                allIncreasingSubsequences(
                    nums,
                    distinct.plus(nums[index]),
                    index - 1)

            val elements = allIncreasingSubsequences.filter {
                it.last() <= nums[index]
            }.map {
                it.plusElement(nums[index])
            }

            if (distinct.contains(nums[index])) {
                allIncreasingSubsequences.plus(elements)
            } else {
                allIncreasingSubsequences.plusElement(listOf(nums[index])).plus(elements)
            }
        } else {
            setOf(listOf(nums[0]))
        }
    }
}

class LongestIncreasingSubsequence {

    // find longest increasing sub-sequence
    fun lis(nums: Array<Int>): List<Int> {
        val lengths = Array(nums.size) {1}
        val previousLargestElementPositions = Array(nums.size) {-1}
        for (i in nums.indices) {
            for (j in 0..i) {
                if (nums[j] < nums[i] && lengths[i] < lengths[j] + 1) {
                    lengths[i] = lengths[j] + 1
                    previousLargestElementPositions[i] = j
                }
            }
        }

        var lisEndPosition = lengths
            .mapIndexed { index, length ->  index to length}
            .maxBy { it.second }!!.first

        val lis = ArrayList<Int>()
        while (lisEndPosition >= 0) {
            lis.add(nums[lisEndPosition])
            lisEndPosition = previousLargestElementPositions[lisEndPosition]
        }

        return lis.reversed()
    }

}
// returns last digit (obtained with modulo 10)
fun lastDigit(number: Int): Int {
    return number % 10
}

fun sumDigits(number: Int): Int {
    val lastDigit = lastDigit(number)
    if (number == lastDigit) {
        return number;
    }
    return lastDigit + sumDigits(number / 10)
}

fun multiplyDigits(number: Int): Int {
    val lastDigit = lastDigit(number)
    if (number == lastDigit) {
        return number;
    }
    return lastDigit * multiplyDigits(number / 10)
}

fun subtractProductAndSum(n: Int): Int {
    return multiplyDigits(n) - sumDigits(n)
}

class StoneGame {

    val cache = HashMap<IntRange, Pair<Int, Int>>()

    fun makeMove(piles: IntArray, range: IntRange): Pair<Int, Int> {
        val last = range.last
        val first = range.first
        if (last - first  == 1) {
            return kotlin.math.max(piles[first], piles[last]) to min(piles[first], piles[last])
        }

        val updateScore: (Pair<Int, Int>, Int) -> Pair<Int, Int> =  { pair, value -> pair.second + value to pair.first }

        val withFirst = updateScore(cache.computeIfAbsent(first + 1 until last + 1) {
            makeMove(piles, it)
        }, piles[first])

        val withLast = updateScore(cache.computeIfAbsent(first + 1 until last) {
            makeMove(piles, it)
        }, piles[last])

        return if (withFirst.first > withLast.first) {
            withFirst
        } else {
            withLast
        }
    }

    fun stoneGame(piles: IntArray): Boolean {
        val scores = makeMove(piles, piles.indices)
        return scores.first > scores.second
    }

}

class AllPossiblePalindromes {
    val cache = HashMap<String, Boolean>()

    fun isPalindrome(str: String): Boolean {
        for (i in 0..str.length / 2) {
            if (str[i] != str[str.length - i - 1]) {
                return false
            }
        }
        return true
    }

    fun countSubstrings(s: String): Int {
        val list: MutableList<String> = ArrayList()

        for (i in 0..s.length) {
            for (j in i + 1..s.length) {

                val str = s.substring(i, j)

                val isPalindrome = str.length == 1 || cache.computeIfAbsent(str) {
                    isPalindrome(it)
                }

                if (isPalindrome) {
                    list.add(str)
                }
            }
        }
        return list.size
    }
}


class Bfs {
    // Complete the bfs function below.
    fun bfs(nodeCount: Int, edges: Array<Array<Int>>, startIndex: Int): List<Int> {
        val distances: MutableList<Int> = ArrayList()
        loop@ for (i in 0..nodeCount) {
            if (i == startIndex) {
                break@loop
            }

            val toVisit:Deque<Int> = ArrayDeque()
            toVisit.add(startIndex)
            var distance = 0;

            var found = false
            val visited = ArrayList<Int>()

            while (!found && toVisit.isNotEmpty()) {
                val currentNode = toVisit.pop()
                if (currentNode == i) {
                    distances.add(distance)
                    found = true
                } else {
                    distance++
                    toVisit.addAll(getAdjacentNodes(currentNode, edges))
                }
            }
        }
        return distances
    }

    private fun getAdjacentNodes(startIndex: Int, edges: Array<Array<Int>>): List<Int> {
        return edges.filter { it[0] == startIndex }.map { it[1] }
    }
}

// To execute Kotlin code, please define a top level function named main
fun main(args: Array<String>) {
//    print(calculateExecutionTime(mutableListOf("0:start:0","1:start:2","1:end:5","0:end:6")))
//    print(lis(arrayOf(0, 1 , 3, 6)))
//    val list = arrayOf(4, 6, 7, 7)
//    print(allIncreasingSubsequences(list.toList(), emptySet(), list.size - 1).filter { it.size > 1 })

//    HashMap<Int, Int>()
//    args.first()
//    print(subtractProductAndSum(1234))


//    print(stoneGame(intArrayOf(9,9,10,1,7,3)))
//    print(cache)
    print(AllPossiblePalindromes().countSubstrings("aaaaaaaaaaaaaaaaaaaaa"))




}
