// Databricks notebook source
// MAGIC %python
// MAGIC 
// MAGIC import pyspark
// MAGIC from typing import Callable, Any, Iterable, List, Set, Tuple
// MAGIC 
// MAGIC #############################################
// MAGIC # Deprecated test functions
// MAGIC #############################################
// MAGIC 
// MAGIC def dbTest(id, expected, result):
// MAGIC   import uuid
// MAGIC   
// MAGIC   if id: eventId = "Test-"+id 
// MAGIC   else: eventId = "Test-"+str(uuid.uuid1())
// MAGIC 
// MAGIC   evaluation = str(expected) == str(result)
// MAGIC   status = "passed" if evaluation else "failed"
// MAGIC   daLogger.logEvent(id, f"{eventId}\nDBTest Assertion\n{status}\n1")
// MAGIC 
// MAGIC   assert evaluation, f"{result} does not equal expected {expected}"
// MAGIC   
// MAGIC #############################################
// MAGIC # Test Suite classes
// MAGIC #############################################
// MAGIC 
// MAGIC # Test case
// MAGIC class TestCase(object):
// MAGIC   __slots__=('description', 'testFunction', 'id', 'dependsOn', 'escapeHTML', 'points')
// MAGIC   def __init__(self,
// MAGIC                description:str,
// MAGIC                testFunction:Callable[[], Any],
// MAGIC                id:str=None,
// MAGIC                dependsOn:Iterable[str]=[],
// MAGIC                escapeHTML:bool=False,
// MAGIC                points:int=1):
// MAGIC     
// MAGIC     self.description=description
// MAGIC     self.testFunction=testFunction
// MAGIC     self.id=id
// MAGIC     self.dependsOn=dependsOn
// MAGIC     self.escapeHTML=escapeHTML
// MAGIC     self.points=points
// MAGIC 
// MAGIC # Test result
// MAGIC class TestResult(object):
// MAGIC   __slots__ = ('test', 'skipped', 'debug', 'passed', 'status', 'points', 'exception', 'message')
// MAGIC   def __init__(self, test, skipped = False, debug = False):
// MAGIC     try:
// MAGIC       self.test = test
// MAGIC       self.skipped = skipped
// MAGIC       self.debug = debug
// MAGIC       if skipped:
// MAGIC         self.status = 'skipped'
// MAGIC         self.passed = False
// MAGIC         self.points = 0
// MAGIC       else:
// MAGIC         assert test.testFunction() != False, "Test returned false"
// MAGIC         self.status = "passed"
// MAGIC         self.passed = True
// MAGIC         self.points = self.test.points
// MAGIC       self.exception = None
// MAGIC       self.message = ""
// MAGIC     except Exception as e:
// MAGIC       self.status = "failed"
// MAGIC       self.passed = False
// MAGIC       self.points = 0
// MAGIC       self.exception = e
// MAGIC       self.message = repr(self.exception)
// MAGIC       if (debug and not isinstance(e, AssertionError)):
// MAGIC         raise e
// MAGIC 
// MAGIC # Decorator to lazy evaluate - used by TestSuite
// MAGIC def lazy_property(fn):
// MAGIC     '''Decorator that makes a property lazy-evaluated.
// MAGIC     '''
// MAGIC     attr_name = '_lazy_' + fn.__name__
// MAGIC 
// MAGIC     @property
// MAGIC     def _lazy_property(self):
// MAGIC         if not hasattr(self, attr_name):
// MAGIC             setattr(self, attr_name, fn(self))
// MAGIC         return getattr(self, attr_name)
// MAGIC     return _lazy_property
// MAGIC 
// MAGIC   
// MAGIC testResultsStyle = """
// MAGIC <style>
// MAGIC   table { text-align: left; border-collapse: collapse; margin: 1em; caption-side: bottom; font-family: Sans-Serif; font-size: 16px}
// MAGIC   caption { text-align: left; padding: 5px }
// MAGIC   th, td { border: 1px solid #ddd; padding: 5px }
// MAGIC   th { background-color: #ddd }
// MAGIC   .passed { background-color: #97d897 }
// MAGIC   .failed { background-color: #e2716c }
// MAGIC   .skipped { background-color: #f9d275 }
// MAGIC   .results .points { display: none }
// MAGIC   .results .message { display: none }
// MAGIC   .results .passed::before  { content: "Passed" }
// MAGIC   .results .failed::before  { content: "Failed" }
// MAGIC   .results .skipped::before { content: "Skipped" }
// MAGIC   .grade .passed  .message:empty::before { content:"Passed" }
// MAGIC   .grade .failed  .message:empty::before { content:"Failed" }
// MAGIC   .grade .skipped .message:empty::before { content:"Skipped" }
// MAGIC </style>
// MAGIC     """.strip()
// MAGIC 
// MAGIC 
// MAGIC class __TestResultsAggregator(object):
// MAGIC   testResults = dict()
// MAGIC   
// MAGIC   def update(self, result:TestResult):
// MAGIC     self.testResults[result.test.id] = result
// MAGIC     return result
// MAGIC   
// MAGIC   @lazy_property
// MAGIC   def score(self) -> int:
// MAGIC     return __builtins__.sum(map(lambda result: result.points, self.testResults.values()))
// MAGIC   
// MAGIC   @lazy_property
// MAGIC   def maxScore(self) -> int:
// MAGIC     return __builtins__.sum(map(lambda result: result.test.points, self.testResults.values()))
// MAGIC 
// MAGIC   @lazy_property
// MAGIC   def percentage(self) -> int:
// MAGIC     return 0 if self.maxScore == 0 else int(100.0 * self.score / self.maxScore)
// MAGIC 
// MAGIC   def displayResults(self):
// MAGIC     displayHTML(testResultsStyle + f"""
// MAGIC     <table class='results'>
// MAGIC       <tr><th colspan="2">Test Summary</th></tr>
// MAGIC       <tr><td>Number of Passing Tests</td><td style="text-align:right">{self.score}</td></tr>
// MAGIC       <tr><td>Number of Failing Tests</td><td style="text-align:right">{self.maxScore-self.score}</td></tr>
// MAGIC       <tr><td>Percentage Passed</td><td style="text-align:right">{self.percentage}%</td></tr>
// MAGIC     </table>
// MAGIC     """)
// MAGIC # Lazy-man's singleton
// MAGIC TestResultsAggregator = __TestResultsAggregator()
// MAGIC 
// MAGIC 
// MAGIC # Test suite class
// MAGIC class TestSuite(object):
// MAGIC   def __init__(self, initialTestCases: Iterable[TestCase] = None) -> None:
// MAGIC     self.ids = set()
// MAGIC     self.testCases = list()
// MAGIC     if initialTestCases:
// MAGIC       for tC in initialTestCases:
// MAGIC         self.addTest(tC)
// MAGIC         
// MAGIC   @lazy_property
// MAGIC   def testResults(self) -> List[TestResult]:
// MAGIC     return self.runTests()
// MAGIC   
// MAGIC   def runTests(self, debug=False) -> List[TestResult]:
// MAGIC     import re
// MAGIC     import uuid
// MAGIC     failedTests = set()
// MAGIC     testResults = list()
// MAGIC 
// MAGIC     for test in self.testCases:
// MAGIC       skip = any(testId in failedTests for testId in test.dependsOn)
// MAGIC       result = TestResult(test, skip, debug)
// MAGIC 
// MAGIC       if (not result.passed and test.id != None):
// MAGIC         failedTests.add(test.id)
// MAGIC 
// MAGIC       if result.test.id: eventId = "Test-"+result.test.id 
// MAGIC       elif result.test.description: eventId = "Test-"+re.sub("[^a-zA-Z0-9_]", "", result.test.description).upper()
// MAGIC       else: eventId = "Test-"+str(uuid.uuid1())
// MAGIC       message = f"{eventId}\n{result.test.description}\n{result.status}\n{result.points}"
// MAGIC       daLogger.logEvent(eventId, message)
// MAGIC 
// MAGIC       testResults.append(result)
// MAGIC       TestResultsAggregator.update(result)
// MAGIC     
// MAGIC     return testResults
// MAGIC 
// MAGIC   def _display(self, cssClass:str="results", debug=False) -> None:
// MAGIC     from html import escape
// MAGIC     testResults = self.testResults if not debug else self.runTests(debug=True)
// MAGIC     lines = []
// MAGIC     lines.append(testResultsStyle)
// MAGIC     lines.append("<table class='"+cssClass+"'>")
// MAGIC     lines.append("  <tr><th class='points'>Points</th><th class='test'>Test</th><th class='result'>Result</th></tr>")
// MAGIC     for result in testResults:
// MAGIC       resultHTML = "<td class='result "+result.status+"'><span class='message'>"+result.message+"</span></td>"
// MAGIC       descriptionHTML = escape(str(result.test.description)) if (result.test.escapeHTML) else str(result.test.description)
// MAGIC       lines.append("  <tr><td class='points'>"+str(result.points)+"</td><td class='test'>"+descriptionHTML+"</td>"+resultHTML+"</tr>")
// MAGIC     lines.append("  <caption class='points'>Score: "+str(self.score)+"</caption>")
// MAGIC     lines.append("</table>")
// MAGIC     html = "\n".join(lines)
// MAGIC     displayHTML(html)
// MAGIC   
// MAGIC   def displayResults(self) -> None:
// MAGIC     self._display("results")
// MAGIC   
// MAGIC   def grade(self) -> int:
// MAGIC     self._display("grade")
// MAGIC     return self.score
// MAGIC   
// MAGIC   def debug(self) -> None:
// MAGIC     self._display("grade", debug=True)
// MAGIC   
// MAGIC   @lazy_property
// MAGIC   def score(self) -> int:
// MAGIC     return __builtins__.sum(map(lambda result: result.points, self.testResults))
// MAGIC   
// MAGIC   @lazy_property
// MAGIC   def maxScore(self) -> int:
// MAGIC     return __builtins__.sum(map(lambda result: result.test.points, self.testResults))
// MAGIC 
// MAGIC   @lazy_property
// MAGIC   def percentage(self) -> int:
// MAGIC     return 0 if self.maxScore == 0 else int(100.0 * self.score / self.maxScore)
// MAGIC 
// MAGIC   def addTest(self, testCase: TestCase):
// MAGIC     if not testCase.id: raise ValueError("The test cases' id must be specified")
// MAGIC     if testCase.id in self.ids: raise ValueError(f"Duplicate test case id: {testCase.id}")
// MAGIC     self.testCases.append(testCase)
// MAGIC     self.ids.add(testCase.id)
// MAGIC     return self
// MAGIC   
// MAGIC   def test(self, id:str, description:str, testFunction:Callable[[], Any], points:int=1, dependsOn:Iterable[str]=[], escapeHTML:bool=False):
// MAGIC     testCase = TestCase(id=id, description=description, testFunction=testFunction, dependsOn=dependsOn, escapeHTML=escapeHTML, points=points)
// MAGIC     return self.addTest(testCase)
// MAGIC   
// MAGIC   def testEquals(self, id:str, description:str, valueA, valueB, points:int=1, dependsOn:Iterable[str]=[], escapeHTML:bool=False):
// MAGIC     testFunction = lambda: valueA == valueB
// MAGIC     testCase = TestCase(id=id, description=description, testFunction=testFunction, dependsOn=dependsOn, escapeHTML=escapeHTML, points=points)
// MAGIC     return self.addTest(testCase)
// MAGIC     
// MAGIC   def testFloats(self, id:str, description:str, valueA, valueB, tolerance=0.01, points:int=1, dependsOn:Iterable[str]=[], escapeHTML:bool=False):
// MAGIC     testFunction = lambda: compareFloats(valueA, valueB, tolerance)
// MAGIC     testCase = TestCase(id=id, description=description, testFunction=testFunction, dependsOn=dependsOn, escapeHTML=escapeHTML, points=points)
// MAGIC     return self.addTest(testCase)
// MAGIC 
// MAGIC   def testRows(self, id:str, description:str, rowA: pyspark.sql.Row, rowB: pyspark.sql.Row, points:int=1, dependsOn:Iterable[str]=[], escapeHTML:bool=False):
// MAGIC     testFunction = lambda: compareRows(rowA, rowB)
// MAGIC     testCase = TestCase(id=id, description=description, testFunction=testFunction, dependsOn=dependsOn, escapeHTML=escapeHTML, points=points)
// MAGIC     return self.addTest(testCase)
// MAGIC   
// MAGIC   def testDataFrames(self, id:str, description:str, dfA: pyspark.sql.DataFrame, dfB: pyspark.sql.DataFrame, testColumnOrder: bool, testNullable: bool, points:int=1, dependsOn:Iterable[str]=[], escapeHTML:bool=False):
// MAGIC     testFunction = lambda: compareDataFrames(dfA, dfB, testColumnOrder, testNullable)
// MAGIC     testCase = TestCase(id=id, description=description, testFunction=testFunction, dependsOn=dependsOn, escapeHTML=escapeHTML, points=points)
// MAGIC     return self.addTest(testCase)
// MAGIC   
// MAGIC   def testSchemas(self, id:str, description:str, schemaA: pyspark.sql.types.StructType, schemaB: pyspark.sql.types.StructType, testColumnOrder: bool, testNullable: bool, points:int=1, dependsOn:Iterable[str]=[], escapeHTML:bool=False):
// MAGIC     testFunction = lambda: compareSchemas(schemaA, schemaB, testColumnOrder, testNullable)
// MAGIC     testCase = TestCase(id=id, description=description, testFunction=testFunction, dependsOn=dependsOn, escapeHTML=escapeHTML, points=points)
// MAGIC     return self.addTest(testCase)
// MAGIC   
// MAGIC   def testContains(self, id:str, description:str, listOfValues, value, points:int=1, dependsOn:Iterable[str]=[], escapeHTML:bool=False):
// MAGIC     testFunction = lambda: value in listOfValues
// MAGIC     testCase = TestCase(id=id, description=description, testFunction=testFunction, dependsOn=dependsOn, escapeHTML=escapeHTML, points=points)
// MAGIC     return self.addTest(testCase)
// MAGIC 
// MAGIC #############################################
// MAGIC # Test Suite utilities
// MAGIC #############################################
// MAGIC 
// MAGIC 
// MAGIC def getQueryString(df: pyspark.sql.DataFrame) -> str:
// MAGIC   # redirect sys.stdout to a buffer
// MAGIC   import sys, io
// MAGIC   stdout = sys.stdout
// MAGIC   sys.stdout = io.StringIO()
// MAGIC 
// MAGIC   # call module
// MAGIC   df.explain(extended=True)
// MAGIC 
// MAGIC   # get output and restore sys.stdout
// MAGIC   output = sys.stdout.getvalue()
// MAGIC   sys.stdout = stdout
// MAGIC 
// MAGIC   return output
// MAGIC 
// MAGIC 
// MAGIC #############################################
// MAGIC # Test Suite comparison functions
// MAGIC #############################################
// MAGIC 
// MAGIC def compareFloats(valueA, valueB, tolerance=0.01):
// MAGIC   # Usage: compareFloats(valueA, valueB) (uses default tolerance of 0.01)
// MAGIC   #        compareFloats(valueA, valueB, tolerance=0.001)
// MAGIC   from builtins import abs 
// MAGIC   
// MAGIC   try:
// MAGIC     if (valueA == None and valueB == None):
// MAGIC          return True
// MAGIC       
// MAGIC     else:
// MAGIC          return abs(float(valueA) - float(valueB)) <= tolerance 
// MAGIC       
// MAGIC   except:
// MAGIC     return False
// MAGIC   
// MAGIC   
// MAGIC def compareRows(rowA: pyspark.sql.Row, rowB: pyspark.sql.Row):
// MAGIC   # Usage: compareRows(rowA, rowB)
// MAGIC   # compares two Dictionaries
// MAGIC   
// MAGIC   if (rowA == None and rowB == None):
// MAGIC     return True
// MAGIC   
// MAGIC   elif (rowA == None or rowB == None):
// MAGIC     return False
// MAGIC   
// MAGIC   else: 
// MAGIC     return rowA.asDict() == rowB.asDict()
// MAGIC 
// MAGIC 
// MAGIC def compareDataFrames(dfA: pyspark.sql.DataFrame, dfB: pyspark.sql.DataFrame, testColumnOrder: bool, testNullable: bool):
// MAGIC   if dfA == None and dfB == None: return True
// MAGIC   if dfA == None or dfB == None: return False
// MAGIC   if compareSchemas(dfA.schema, dfB.schema, testColumnOrder, testNullable) == False: return False
// MAGIC   if dfA.count() != dfB.count(): return False
// MAGIC 
// MAGIC   rowsA = dfA.collect()
// MAGIC   rowsB = dfB.collect()
// MAGIC 
// MAGIC   for i in range(0, len(rowsA)):
// MAGIC     rowA = rowsA[i]
// MAGIC     rowB = rowsB[i]
// MAGIC     for column in dfA.columns:
// MAGIC       valueA = rowA[column]
// MAGIC       valueB = rowB[column]
// MAGIC       if (valueA != valueB): return False
// MAGIC 
// MAGIC   return True
// MAGIC 
// MAGIC 
// MAGIC def compareSchemas(schemaA: pyspark.sql.types.StructType, schemaB: pyspark.sql.types.StructType, testColumnOrder: bool, testNullable: bool): 
// MAGIC   
// MAGIC   from pyspark.sql.types import StructField
// MAGIC   
// MAGIC   if (schemaA == None and schemaB == None): return True
// MAGIC   if (schemaA == None or schemaB == None): return False
// MAGIC   
// MAGIC   schA = schemaA
// MAGIC   schB = schemaB
// MAGIC 
// MAGIC   if (testNullable == False):  
// MAGIC       schA = [StructField(s.name, s.dataType, True) for s in schemaA]
// MAGIC       schB = [StructField(s.name, s.dataType, True) for s in schemaB]
// MAGIC 
// MAGIC   if (testColumnOrder == True):
// MAGIC     return [schA] == [schB]
// MAGIC   else:
// MAGIC     return set(schA) == set(schB)
// MAGIC   
// MAGIC displayHTML("Initializing Databricks Academy's testing framework...")

// COMMAND ----------

// MAGIC %scala
// MAGIC 
// MAGIC //*******************************************
// MAGIC // Deprecated test functions
// MAGIC //*******************************************
// MAGIC 
// MAGIC def dbTest[T](id: String, expected: T, result: => T): Unit = {
// MAGIC   val eventId = "Test-" + (if (id != null) id else java.util.UUID.randomUUID)
// MAGIC   val evaluation = (result == expected)
// MAGIC   val status = if (evaluation) "passed" else "failed"
// MAGIC   daLogger.logEvent(id, s"$eventId\nDBTest Assertion\n$status\n1")
// MAGIC 
// MAGIC   assert(evaluation, s"$result does not equal $expected expected")
// MAGIC }
// MAGIC 
// MAGIC //*******************************************
// MAGIC // TEST SUITE CLASSES
// MAGIC //*******************************************
// MAGIC 
// MAGIC // Test case
// MAGIC case class TestCase(description:String,
// MAGIC                     testFunction:()=>Any,
// MAGIC                     id:String=null,
// MAGIC                     dependsOn:Seq[String]=Nil,
// MAGIC                     escapeHTML:Boolean=false,
// MAGIC                     points:Int=1)
// MAGIC 
// MAGIC // Test result
// MAGIC case class TestResult(test: TestCase, skipped:Boolean = false, debug:Boolean = false) {
// MAGIC   val exception: Option[Throwable] = {
// MAGIC     if (skipped)
// MAGIC       None
// MAGIC     else if (debug) {
// MAGIC       if (test.testFunction() != false)
// MAGIC         None
// MAGIC       else 
// MAGIC         Some(new AssertionError("Test returned false"))
// MAGIC     } else {
// MAGIC       try {
// MAGIC         assert(test.testFunction() != false, "Test returned false")
// MAGIC         None
// MAGIC       } catch {
// MAGIC         case e: Exception => Some(e)
// MAGIC         case e: AssertionError => Some(e)
// MAGIC       }
// MAGIC     }
// MAGIC   }
// MAGIC 
// MAGIC   val passed: Boolean = !skipped && exception.isEmpty
// MAGIC 
// MAGIC   val message: String = {
// MAGIC     exception.map(ex => {
// MAGIC       val msg = ex.getMessage()
// MAGIC       if (msg == null || msg.isEmpty()) ex.toString() else msg
// MAGIC     }).getOrElse("")
// MAGIC   }
// MAGIC   
// MAGIC   val status: String = if (skipped) "skipped" else if (passed) "passed" else "failed"
// MAGIC   
// MAGIC   val points: Int = if (passed) test.points else 0
// MAGIC }
// MAGIC 
// MAGIC val testResultsStyle = """
// MAGIC <style>
// MAGIC   table { text-align: left; border-collapse: collapse; margin: 1em; caption-side: bottom; font-family: Sans-Serif; font-size: 16px}
// MAGIC   caption { text-align: left; padding: 5px }
// MAGIC   th, td { border: 1px solid #ddd; padding: 5px }
// MAGIC   th { background-color: #ddd }
// MAGIC   .passed { background-color: #97d897 }
// MAGIC   .failed { background-color: #e2716c }
// MAGIC   .skipped { background-color: #f9d275 }
// MAGIC   .results .points { display: none }
// MAGIC   .results .message { display: none }
// MAGIC   .results .passed::before  { content: "Passed" }
// MAGIC   .results .failed::before  { content: "Failed" }
// MAGIC   .results .skipped::before { content: "Skipped" }
// MAGIC   .grade .passed  .message:empty::before { content:"Passed" }
// MAGIC   .grade .failed  .message:empty::before { content:"Failed" }
// MAGIC   .grade .skipped .message:empty::before { content:"Skipped" }
// MAGIC </style>
// MAGIC     """.trim
// MAGIC 
// MAGIC object TestResultsAggregator {
// MAGIC   val testResults = scala.collection.mutable.Map[String,TestResult]()
// MAGIC   def update(result:TestResult):TestResult = {
// MAGIC     testResults.put(result.test.id, result)
// MAGIC     return result
// MAGIC   }
// MAGIC   def score = testResults.values.map(_.points).sum
// MAGIC   def maxScore = testResults.values.map(_.test.points).sum
// MAGIC   def percentage = (if (maxScore == 0) 0 else 100.0 * score / maxScore).toInt
// MAGIC   def displayResults():Unit = {
// MAGIC     displayHTML(testResultsStyle + s"""
// MAGIC     <table class='results'>
// MAGIC       <tr><th colspan="2">Test Summary</th></tr>
// MAGIC       <tr><td>Number of Passing Tests</td><td style="text-align:right">${score}</td></tr>
// MAGIC       <tr><td>Number of Failing Tests</td><td style="text-align:right">${maxScore-score}</td></tr>
// MAGIC       <tr><td>Percentage Passed</td><td style="text-align:right">${percentage}%</td></tr>
// MAGIC     </table>
// MAGIC     """)
// MAGIC   }
// MAGIC }
// MAGIC 
// MAGIC // Test Suite
// MAGIC case class TestSuite(initialTestCases : TestCase*) {
// MAGIC   
// MAGIC   val testCases : scala.collection.mutable.ArrayBuffer[TestCase] = scala.collection.mutable.ArrayBuffer[TestCase]()
// MAGIC   val ids : scala.collection.mutable.ArrayBuffer[String] = scala.collection.mutable.ArrayBuffer[String]()
// MAGIC   
// MAGIC   initialTestCases.foreach(addTest)
// MAGIC   
// MAGIC   import scala.collection.mutable.ListBuffer
// MAGIC   import scala.xml.Utility.escape
// MAGIC   
// MAGIC   def runTests(debug:Boolean = false):scala.collection.mutable.ArrayBuffer[TestResult] = {
// MAGIC     val failedTests = scala.collection.mutable.Set[String]()
// MAGIC     val results = scala.collection.mutable.ArrayBuffer[TestResult]()
// MAGIC     for (testCase <- testCases) {
// MAGIC       val skip = testCase.dependsOn.exists(failedTests contains _)
// MAGIC       val result = TestResult(testCase, skip, debug)
// MAGIC 
// MAGIC       if (result.passed == false && testCase.id != null) {
// MAGIC         failedTests += testCase.id
// MAGIC       }
// MAGIC       val eventId = "Test-" + (if (result.test.id != null) result.test.id 
// MAGIC                           else if (result.test.description != null) result.test.description.replaceAll("[^a-zA-Z0-9_]", "").toUpperCase() 
// MAGIC                           else java.util.UUID.randomUUID)
// MAGIC       val message = s"${eventId}\n${result.test.description}\n${result.status}\n${result.points}"
// MAGIC       daLogger.logEvent(eventId, message)
// MAGIC       
// MAGIC       results += result
// MAGIC       TestResultsAggregator.update(result)
// MAGIC     }
// MAGIC     return results
// MAGIC   }
// MAGIC 
// MAGIC   lazy val testResults = runTests()
// MAGIC 
// MAGIC   private def display(cssClass:String="results", debug:Boolean=false) : Unit = {
// MAGIC     val testResults = if (!debug) this.testResults else runTests(debug=true)
// MAGIC     val lines = ListBuffer[String]()
// MAGIC     lines += testResultsStyle
// MAGIC     lines += s"<table class='$cssClass'>"
// MAGIC     lines += "  <tr><th class='points'>Points</th><th class='test'>Test</th><th class='result'>Result</th></tr>"
// MAGIC     for (result <- testResults) {
// MAGIC       val resultHTML = s"<td class='result ${result.status}'><span class='message'>${result.message}</span></td>"
// MAGIC       val descriptionHTML = if (result.test.escapeHTML) escape(result.test.description) else result.test.description
// MAGIC       lines += s"  <tr><td class='points'>${result.points}</td><td class='test'>$descriptionHTML</td>$resultHTML</tr>"
// MAGIC     }
// MAGIC     lines += s"  <caption class='points'>Score: $score</caption>"
// MAGIC     lines += "</table>"
// MAGIC     val html = lines.mkString("\n")
// MAGIC     displayHTML(html)
// MAGIC   }
// MAGIC   
// MAGIC   def displayResults() : Unit = {
// MAGIC     display("results")
// MAGIC   }
// MAGIC   
// MAGIC   def grade() : Int = {
// MAGIC     display("grade")
// MAGIC     score
// MAGIC   }
// MAGIC   
// MAGIC   def debug() : Unit = {
// MAGIC     display("grade", debug=true)
// MAGIC   }
// MAGIC   
// MAGIC   def score = testResults.map(_.points).sum
// MAGIC   
// MAGIC   def maxScore : Integer = testCases.map(_.points).sum
// MAGIC 
// MAGIC   def percentage = (if (maxScore == 0) 0 else 100.0 * score / maxScore).toInt
// MAGIC   
// MAGIC   def addTest(testCase: TestCase):TestSuite = {
// MAGIC     if (testCase.id == null) throw new IllegalArgumentException("The test cases' id must be specified")
// MAGIC     if (ids.contains(testCase.id)) throw new IllegalArgumentException(f"Duplicate test case id: {testCase.id}")
// MAGIC     testCases += testCase
// MAGIC     ids += testCase.id
// MAGIC     return this
// MAGIC   }
// MAGIC   
// MAGIC   def test(id:String, description:String, testFunction:()=>Any, points:Int=1, dependsOn:Seq[String]=Nil, escapeHTML:Boolean=false): TestSuite = {
// MAGIC     val testCase = TestCase(id=id, description=description, testFunction=testFunction, dependsOn=dependsOn, escapeHTML=escapeHTML, points=points)
// MAGIC     return addTest(testCase)
// MAGIC   }
// MAGIC   
// MAGIC   def testEquals(id:String, description:String, valueA:Any, valueB:Any, points:Int=1, dependsOn:Seq[String]=Nil, escapeHTML:Boolean=false): TestSuite = {
// MAGIC     val testFunction = ()=> (valueA == valueB)
// MAGIC     val testCase = TestCase(id=id, description=description, testFunction=testFunction, dependsOn=dependsOn, escapeHTML=escapeHTML, points=points)
// MAGIC     return addTest(testCase)
// MAGIC   }
// MAGIC   
// MAGIC   def testFloats(id:String, description:String, valueA: Any, valueB: Any, tolerance: Double=0.01, points:Int=1, dependsOn:Seq[String]=Nil, escapeHTML:Boolean=false): TestSuite = {
// MAGIC     val testFunction = ()=> compareFloats(valueA, valueB, tolerance)
// MAGIC     val testCase = TestCase(id=id, description=description, testFunction=testFunction, dependsOn=dependsOn, escapeHTML=escapeHTML, points=points)
// MAGIC     return addTest(testCase)
// MAGIC   }
// MAGIC   
// MAGIC   def testRows(id:String, description:String, rowA: org.apache.spark.sql.Row, rowB: org.apache.spark.sql.Row, points:Int=1, dependsOn:Seq[String]=Nil, escapeHTML:Boolean=false): TestSuite = {
// MAGIC     val testFunction = ()=> compareRows(rowA, rowB)
// MAGIC     val testCase = TestCase(id=id, description=description, testFunction=testFunction, dependsOn=dependsOn, escapeHTML=escapeHTML, points=points)
// MAGIC     return addTest(testCase)
// MAGIC   }
// MAGIC   
// MAGIC   def testDataFrames(id:String, description:String, dfA:org.apache.spark.sql.DataFrame, dfB:org.apache.spark.sql.DataFrame, testColumnOrder:Boolean, testNullable:Boolean, points:Int=1, dependsOn:Seq[String]=Nil, escapeHTML:Boolean=false): TestSuite = {
// MAGIC     val testFunction = ()=> compareDataFrames(dfA, dfB, testColumnOrder, testNullable)
// MAGIC     val testCase = TestCase(id=id, description=description, testFunction=testFunction, dependsOn=dependsOn, escapeHTML=escapeHTML, points=points)
// MAGIC     return addTest(testCase)
// MAGIC   }
// MAGIC   
// MAGIC   def testSchemas(id:String, description:String, schemaA: org.apache.spark.sql.types.StructType, schemaB: org.apache.spark.sql.types.StructType, testColumnOrder: Boolean, testNullable: Boolean, points:Int=1, dependsOn:Seq[String]=Nil, escapeHTML:Boolean=false): TestSuite = {
// MAGIC     val testFunction = ()=> compareSchemas(schemaA, schemaB, testColumnOrder, testNullable)
// MAGIC     val testCase = TestCase(id=id, description=description, testFunction=testFunction, dependsOn=dependsOn, escapeHTML=escapeHTML, points=points)
// MAGIC     return addTest(testCase)
// MAGIC   }
// MAGIC 
// MAGIC   def testContains(id:String, description:String, list:Seq[Any], value:Any, points:Int=1, dependsOn:Seq[String]=Nil, escapeHTML:Boolean=false): TestSuite = {
// MAGIC     val testFunction = ()=> list.contains(value)
// MAGIC     val testCase = TestCase(id=id, description=description, testFunction=testFunction, dependsOn=dependsOn, escapeHTML=escapeHTML, points=points)
// MAGIC     return addTest(testCase)
// MAGIC   }
// MAGIC }
// MAGIC 
// MAGIC //*******************************************
// MAGIC // SCORE FUNCTIONS AND PLACEHOLDER GENERATION
// MAGIC //*******************************************
// MAGIC 
// MAGIC def getQueryString(df: org.apache.spark.sql.Dataset[Row]): String = {
// MAGIC   df.explain.toString
// MAGIC }
// MAGIC 
// MAGIC //*******************************************
// MAGIC // Test Suite comparison functions
// MAGIC //*******************************************
// MAGIC 
// MAGIC def compareFloats(valueA: Any, valueB: Any, tolerance: Double=0.01): Boolean = {
// MAGIC   // Usage: compareFloats(valueA, valueB) (uses default tolerance of 0.01)
// MAGIC   //        compareFloats(valueA, valueB, tolerance=0.001)
// MAGIC   
// MAGIC   import scala.math
// MAGIC   try{
// MAGIC        if (valueA == null && valueB == null) {
// MAGIC          true
// MAGIC        } else if (valueA == null || valueB == null) {
// MAGIC          false
// MAGIC        } else {
// MAGIC          math.abs(valueA.asInstanceOf[Number].doubleValue - valueB.asInstanceOf[Number].doubleValue) <= tolerance 
// MAGIC        }
// MAGIC   } catch {
// MAGIC     case e: ClassCastException => {
// MAGIC       false
// MAGIC    }   
// MAGIC   }
// MAGIC }
// MAGIC 
// MAGIC 
// MAGIC def compareRows(rowA: org.apache.spark.sql.Row, rowB: org.apache.spark.sql.Row): Boolean = {
// MAGIC   // Usage: compareRows(rowA, rowB)
// MAGIC   // compares two rows as unordered Maps
// MAGIC 
// MAGIC   if (rowA == null && rowB == null) {
// MAGIC     true
// MAGIC     
// MAGIC   } else if (rowA == null || rowB == null) {
// MAGIC     false
// MAGIC     
// MAGIC   // for some reason, the schema didn't make it
// MAGIC   } else if (rowA.schema == null || rowB.schema == null) {
// MAGIC     rowA.toSeq.toSet == rowB.toSeq.toSet
// MAGIC     
// MAGIC   } else {
// MAGIC     rowA.getValuesMap[String](rowA.schema.fieldNames.toSeq) == rowB.getValuesMap[String](rowB.schema.fieldNames.toSeq)
// MAGIC   }
// MAGIC }
// MAGIC 
// MAGIC 
// MAGIC def compareDataFrames(dfA: org.apache.spark.sql.DataFrame, dfB: org.apache.spark.sql.DataFrame, testColumnOrder: Boolean, testNullable: Boolean): Boolean = {
// MAGIC   if (dfA == null && dfB == null) return true
// MAGIC   if (dfA == null || dfB == null) return false
// MAGIC   if (compareSchemas(dfA.schema, dfB.schema, testColumnOrder, testNullable) == false) return false
// MAGIC   if (dfA.count != dfB.count) return false
// MAGIC 
// MAGIC   val rowsA = dfA.collect()
// MAGIC   val rowsB = dfB.collect()
// MAGIC   
// MAGIC   for (i <- 0 until rowsA.length) {
// MAGIC     val rowA = rowsA(i)
// MAGIC     val rowB = rowsB(i)
// MAGIC     for (column <- dfA.columns) {
// MAGIC       val valueA = rowA.getAs[Any](column)
// MAGIC       val valueB = rowB.getAs[Any](column)
// MAGIC       if (valueA != valueB) return false
// MAGIC     }
// MAGIC   }
// MAGIC   return true
// MAGIC }
// MAGIC 
// MAGIC 
// MAGIC def compareSchemas(schemaA: org.apache.spark.sql.types.StructType, schemaB: org.apache.spark.sql.types.StructType, testColumnOrder: Boolean, testNullable: Boolean): Boolean = {
// MAGIC   
// MAGIC   if (schemaA == null && schemaB == null) return true
// MAGIC   if (schemaA == null || schemaB == null) return false
// MAGIC   
// MAGIC   var schA = schemaA.toSeq
// MAGIC   var schB = schemaB.toSeq
// MAGIC 
// MAGIC   if (testNullable == false) {   
// MAGIC     schA = schemaA.map(_.copy(nullable=true)) 
// MAGIC     schB = schemaB.map(_.copy(nullable=true)) 
// MAGIC   }
// MAGIC 
// MAGIC   if (testColumnOrder == true) {
// MAGIC     schA == schB
// MAGIC   } else {
// MAGIC     schA.toSet == schB.toSet
// MAGIC   }
// MAGIC }
// MAGIC 
// MAGIC displayHTML("Initializing Databricks Academy's testing framework...")
