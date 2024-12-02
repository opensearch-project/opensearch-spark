/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

import java.io.BufferedReader
import java.io.FileReader
import java.io.PrintStream
import java.math.BigInteger
import scala.collection.mutable.ListBuffer

var ipv4NodeCount = 0
var ipv6NodeCount = 0
var ipv4NodeOutputCount = 0
var ipv6NodeOutputCount = 0

/* Create a binary tree based on the bits of the start IP address of the subnets. Only use the
   first bits needed for the netmask. For example with a subnet of "192.168.2.0/24", only use the
   first 24 bits.

   If a node for a subnet has children, then there is an overlap that must be corrected. To correct
   an overlap, make sure that both children of the node exist and remove the subnet for the current
   node. Finally check the child nodes for overlapping subnets and continue.
 */
class TreeNode(var ipAddressBytes: Array[Byte], var netmask: Int, var isIPv4: Boolean, var lineRemainder: String) {
    var falseChild: TreeNode = null
    var trueChild: TreeNode = null

    def maxNetmask: Integer = if (isIPv4) 32 else 128

    // Add a new node to the tree in the correct position
    def addNode(nodeToAdd: TreeNode): Unit = {
        if (netmask >= nodeToAdd.netmask || netmask == maxNetmask) {
            return
        }
        
        var byteIndex = netmask / 8
        var bitValue = (nodeToAdd.ipAddressBytes(byteIndex) & (1 << (7 - (netmask % 8)))) > 0

        if (netmask + 1 == nodeToAdd.netmask) {
            if (bitValue) {
                trueChild = nodeToAdd
            } else {
                falseChild = nodeToAdd
            }
        } else {
            var nextChild: TreeNode = null
            if (bitValue) {
                nextChild = trueChild
                if (trueChild == null) {
                    nextChild = new TreeNode(null, netmask + 1, isIPv4, null)
                    trueChild = nextChild
                }
            } else {
                nextChild = falseChild
                if (falseChild == null) {
                    nextChild = new TreeNode(null, netmask + 1, isIPv4, null)
                    falseChild = nextChild
                }
            }
            
            nextChild.addNode(nodeToAdd)
        }
        
        return
    }
    
    def haveOverlap(): Boolean = falseChild != null || trueChild != null

    // Convert the IP address to a string. For IPv6, this is more complicated, since it may
    // need to be reduced.
    def ipAddressString(): String = {
        if (isIPv4) {
            return ipAddressBytes.map(v => 255 & v).mkString(".")
        } else {
            var allZeroes = true
            for (b <- ipAddressBytes) {
                if (b != 0) {
                    allZeroes = false
                }
            }
            
            if (allZeroes) {
                return "::"
            }
            
            var zeroes: ListBuffer[(Int, Int)] = ListBuffer()
            var zeroesStart = -1
            var zeroesStartIndex = -1
            for (i <- 0 to 7) {
                if (ipAddressBytes(i * 2) == 0 && ipAddressBytes(i * 2 + 1) == 0) {
                    if (zeroesStart == -1) {
                        zeroesStart = i
                        zeroesStartIndex = zeroes.length
                        zeroes = zeroes :+ (i, 1)
                    } else {
                        var existingTuple = zeroes(zeroesStartIndex)
                        zeroes.update(zeroesStartIndex, (existingTuple._1, 1 + existingTuple._2))
                    }
                } else {
                    zeroesStart = -1
                    zeroesStartIndex = -1
                }
            }
            
            var longestZeroesIndex = -1
            var longestZeroesLength = 0
            for (v <- zeroes) {
                if (v._2 >= longestZeroesLength) {
                    longestZeroesLength = v._2
                    longestZeroesIndex = v._1
                }
            }
            
            var fullIpAddress: Array[String] = Array.fill(8){null}
            for (i <- 0 to 7) {
                var strValue = (((255 & ipAddressBytes(i * 2)) << 8) + (255 & ipAddressBytes(i * 2 + 1))).toHexString
                fullIpAddress(i) = strValue
            }
            
            if (longestZeroesIndex == -1) {
                return fullIpAddress.mkString(":")
            } else {
                var ipPartsStart = fullIpAddress.slice(0, longestZeroesIndex)
                var ipPartsEnd = fullIpAddress.slice(longestZeroesIndex + longestZeroesLength, 8)
                return ipPartsStart.mkString(":") + "::" + ipPartsEnd.mkString(":")
            }
        }
    }

    def getStart(): BigInteger = new BigInteger(ipAddressBytes)

    def getEnd(): BigInteger = {
        var valueToAdd = new BigInteger(Array.fill(maxNetmask / 8){0.toByte})
        if (netmask < maxNetmask) {
            valueToAdd = valueToAdd.flipBit(maxNetmask - netmask)
            valueToAdd = valueToAdd.subtract(new BigInteger("1"))
        }
        return getStart().add(valueToAdd)
    }
    
    def valueToByteArray(value: BigInteger): Array[Byte] = {
        var fullArray = Array.fill(maxNetmask / 8){0.toByte}
        var valueArray = value.toByteArray()
        valueArray.copyToArray(fullArray, (maxNetmask / 8) - valueArray.length, valueArray.length)
        return fullArray
    }

    def incrementNodeCount(): Unit = {
        if (isIPv4) {
            ipv4NodeCount += ipv4NodeCount
        } else {
            ipv6NodeCount += ipv6NodeCount
        }
    }

    // Split a node. Make sure that both children exist and remove the subnet for the current node.
    def split(): Unit = {
        if (ipAddressBytes == null) {
            return
        }
        
        var ipAddressStr = ipAddressString()
        println(s">>> Splitting IP: $ipAddressStr")
        
        if (falseChild == null) {
            falseChild = new TreeNode(ipAddressBytes, netmask + 1, isIPv4, lineRemainder)
        } else if (falseChild.ipAddressBytes == null) {
            falseChild.ipAddressBytes = ipAddressBytes
            falseChild.lineRemainder = lineRemainder
        }
        
        if (trueChild == null) {
            var valueStart = falseChild.getEnd().add(new BigInteger("1"))
            var startArray = valueToByteArray(valueStart)
            trueChild = new TreeNode(startArray, netmask + 1, isIPv4, lineRemainder)
        } else if (trueChild.ipAddressBytes == null) {
            var valueStart = falseChild.getEnd().add(new BigInteger("1"))
            var startArray = valueToByteArray(valueStart)
            trueChild.ipAddressBytes = startArray
            trueChild.lineRemainder = lineRemainder
        }
        
        ipAddressBytes = null
        lineRemainder = null

        return
    }

    def fixTree(): Unit = {
        if (haveOverlap()) {
            split()
        }
        
        if (falseChild != null) {
            falseChild.fixTree()
        }
        
        if (trueChild != null) {
            trueChild.fixTree()
        }
    }
    
    def printTree(outStream: PrintStream, tenPercentCount: Int): Unit = {
        if (ipAddressBytes != null) {
            outStream.print(ipAddressString())
            outStream.print("/")
            outStream.print(netmask.toString)
            outStream.print(",")
            outStream.print(lineRemainder)
            outStream.print(",")
            outStream.print(getStart().toString())
            outStream.print(",")
            outStream.print(getEnd().toString())
            outStream.print(",")
            outStream.println(isIPv4.toString)

            var currentNodeCount = if (isIPv4) ipv4NodeOutputCount else ipv6NodeOutputCount
            if (currentNodeCount % tenPercentCount == 0) {
                print((currentNodeCount * 10 / tenPercentCount).toString + "%..")
            }

            if (isIPv4) {
                ipv4NodeOutputCount += 1
            } else {
                ipv6NodeOutputCount += 1
            }
        }
        
        if (falseChild != null) {
            falseChild.printTree(outStream, tenPercentCount)
        }
        if (trueChild != null) {
            trueChild.printTree(outStream, tenPercentCount)
        }
    }
}

// Create a node for an IPv4 entry
def createIPv4TreeNode(fullLine: String): TreeNode = {
    var charIndex = fullLine.indexOf(",")
    var subnet = fullLine.substring(0, charIndex)
    var lineRemainder = fullLine.substring(charIndex + 1)

    charIndex = subnet.indexOf("/")
    var ipAddressStr = subnet.substring(0, charIndex)
    var netmask = subnet.substring(charIndex + 1).toInt

    var addrParts = ipAddressStr.split("\\.")
    var bytes = Array[Byte](
        addrParts(0).toInt.toByte,
        addrParts(1).toInt.toByte,
        addrParts(2).toInt.toByte,
        addrParts(3).toInt.toByte
    )

    return new TreeNode(bytes, netmask, true, lineRemainder)
}

// Create a node for an IPv6 entry
def createIPv6TreeNode(fullLine: String): TreeNode = {
    var charIndex = fullLine.indexOf(",")
    var subnet = fullLine.substring(0, charIndex)
    var lineRemainder = fullLine.substring(charIndex + 1)

    charIndex = subnet.indexOf("/")
    var ipAddressStr = subnet.substring(0, charIndex)
    var netmask = subnet.substring(charIndex + 1).toInt

    var bytes: Array[Byte] = null
    charIndex = ipAddressStr.indexOf("::")

    if (charIndex == -1) {
        var values = ipAddressStr.split(":").map(x => Integer.parseInt(x, 16))
        bytes = Array.fill(16){0.toByte}
        for (i <- 0 to 7) {
            bytes(i * 2) = (values(i) >> 8).toByte
            bytes(i * 2 + 1) = (values(i) & 255).toByte
        }
    } else if ("::" == ipAddressStr) {
        bytes = Array.fill(16){0.toByte}
    } else {
        if (charIndex == 0) {
            var values = ipAddressStr.substring(2).split(":").map(x => Integer.parseInt(x, 16))
            bytes = Array.fill(16){0.toByte}
            for (i <- 8 - values.length to 7) {
                var valuesIndex = i - 8 + values.length
                bytes(i * 2) = (values(valuesIndex) >> 8).toByte
                bytes(i * 2 + 1) = (values(valuesIndex) & 255).toByte
            }
        } else if (charIndex == ipAddressStr.length - 2) {
            var values = ipAddressStr.substring(0, ipAddressStr.length - 2).split(":").map(x => Integer.parseInt(x, 16))
            bytes = Array.fill(16){0.toByte}
            for (i <- 0 to values.length - 1) {
                bytes(i * 2) = (values(i) >> 8).toByte
                bytes(i * 2 + 1) = (values(i) & 255).toByte
            }
        } else {
            var startValues = ipAddressStr.substring(0, charIndex).split(":").map(x => Integer.parseInt(x, 16))
            var endValues = ipAddressStr.substring(charIndex + 2).split(":").map(x => Integer.parseInt(x, 16))
            bytes = Array.fill(16){0.toByte}
            for (i <- 0 to startValues.length - 1) {
                bytes(i * 2) = (startValues(i) >> 8).toByte
                bytes(i * 2 + 1) = (startValues(i) & 255).toByte
            }
            for (i <- 8 - endValues.length to 7) {
                var valuesIndex = i - 8 + endValues.length
                bytes(i * 2) = (endValues(valuesIndex) >> 8).toByte
                bytes(i * 2 + 1) = (endValues(valuesIndex) & 255).toByte
            }
        }
    }
        
    return new TreeNode(bytes, netmask, false, lineRemainder)
}

def createTreeNode(fullLine: String): TreeNode = {
    var charIndex = fullLine.indexOf(",")
    var subnet = fullLine.substring(0, charIndex)
    if (subnet.indexOf(':') > -1) {
        return createIPv6TreeNode(fullLine)
    } else {
        return createIPv4TreeNode(fullLine)
    }
}

var header: String = null
def readSubnets(fileName: String, ipv4Root: TreeNode, ipv6Root: TreeNode): Unit = {
    var reader = new BufferedReader(new FileReader(fileName))
    header = reader.readLine()

    var line = reader.readLine()
    while (line != null) {
        var newNode = createTreeNode(line)
        if (newNode.isIPv4) {
            ipv4Root.addNode(newNode)
            ipv4NodeCount += 1
        } else {
            ipv6Root.addNode(newNode)
            ipv6NodeCount += 1
        }
        
        line = reader.readLine()
    }
    
    reader.close()
}

def writeSubnets(fileName: String, ipv4Root: TreeNode, ipv6Root: TreeNode): Unit = {
    var outStream = new PrintStream(fileName)
    outStream.print(header)
    outStream.print(",ip_range_start,ip_range_end,ipv4")
    outStream.print("\r\n")

    println("Writing IPv4 data")
    ipv4NodeOutputCount = 0
    ipv4Root.printTree(outStream, (ipv4NodeCount / 10).floor.toInt)
    println()

    println("Writing IPv6 data")
    ipv6NodeOutputCount = 0
    ipv6Root.printTree(outStream, (ipv6NodeCount / 10).floor.toInt)
    println()

    outStream.close()
}

// Create the table in Spark
def createTable(fileName: String, tableName: String): Unit = {
    try {
        var sparkSessionClass = Class.forName("org.apache.spark.sql.SparkSession")
        var activeSessionMethod = sparkSessionClass.getMethod("active")
        var sparkSession = activeSessionMethod.invoke(sparkSessionClass)

        var readMethod = sparkSessionClass.getMethod("read")
        var dataFrameReader = readMethod.invoke(sparkSession)

        var dataFrameReaderClass = Class.forName("org.apache.spark.sql.DataFrameReader")
        var formatMethod = dataFrameReaderClass.getMethod("format", classOf[java.lang.String])
        dataFrameReader = formatMethod.invoke(dataFrameReader, "csv")

        var optionMethod = dataFrameReaderClass.getMethod("option", classOf[java.lang.String], classOf[java.lang.String])
        dataFrameReader = optionMethod.invoke(dataFrameReader, "inferSchema", "true")
        dataFrameReader = optionMethod.invoke(dataFrameReader, "header", "true")

        var loadMethod = dataFrameReaderClass.getMethod("load", classOf[java.lang.String])
        var dataset = loadMethod.invoke(dataFrameReader, fileName)

        var datasetClass = Class.forName("org.apache.spark.sql.Dataset")
        var writeMethod = datasetClass.getMethod("write")
        var dataFrameWriter = writeMethod.invoke(dataset)

        var dataFrameWriterClass = Class.forName("org.apache.spark.sql.DataFrameWriter")
        var saveAsTableMethod = dataFrameWriterClass.getMethod("saveAsTable", classOf[java.lang.String])
        saveAsTableMethod.invoke(dataFrameWriter, tableName)
    } catch {
        case e: Exception => {
            println("Unable to load data into table")
            e.printStackTrace()
        }
    }
}

// Sanitize the data and import it into a Spark table
def cleanAndImport(inputFile: String, outputFile: String, tableName: String): Unit = {
    if (tableName != null) {
        try {
            Class.forName("org.apache.spark.sql.SparkSession")
        } catch {
            case e: ClassNotFoundException => {
                println("Must run in Spark CLI to create the Spark table")
                return
            }
        }
    }
    
    println("Loading data")
    var ipv4Root = new TreeNode(null, 0, true, null)
    var ipv6Root = new TreeNode(null, 0, false, null)
    readSubnets(inputFile, ipv4Root, ipv6Root)

    println("Fixing overlapping subnets")
    ipv4Root.fixTree()
    ipv6Root.fixTree()

    println("Writing data to file")
    writeSubnets(outputFile, ipv4Root, ipv6Root)

    if (tableName != null) {
        println("Creating and populating Spark table")
        createTable(outputFile, tableName)
    }
    
    println("Done")
}

var FILE_PATH_TO_INPUT_CSV: String = "/replace/this/value"
var FILE_PATH_TO_OUTPUT_CSV: String = "/replace/this/value"
var TABLE_NAME: String = null
var result = cleanAndImport(FILE_PATH_TO_INPUT_CSV, FILE_PATH_TO_OUTPUT_CSV, TABLE_NAME)
