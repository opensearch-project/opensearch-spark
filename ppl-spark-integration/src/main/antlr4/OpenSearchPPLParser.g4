/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

parser grammar OpenSearchPPLParser;


options { tokenVocab = OpenSearchPPLLexer; }
root
   : pplStatement? EOF
   ;

// statement
pplStatement
   : dmlStatement
   ;

dmlStatement
   : (explainCommand PIPE)? queryStatement
   ;

queryStatement
   : pplCommands (PIPE commands)*
   ;

subSearch
   : searchCommand (PIPE commands)*
   ;

// commands
pplCommands
   : searchCommand
   | describeCommand
   ;

commands
   : whereCommand
   | correlateCommand
   | joinCommand
   | fieldsCommand
   | statsCommand
   | dedupCommand
   | sortCommand
   | headCommand
   | topCommand
   | rareCommand
   | evalCommand
   | grokCommand
   | parseCommand
   | patternsCommand
   | lookupCommand
   | renameCommand
   | fillnullCommand
   ;

searchCommand
   : (SEARCH)? fromClause                       # searchFrom
   | (SEARCH)? fromClause logicalExpression     # searchFromFilter
   | (SEARCH)? logicalExpression fromClause     # searchFilterFrom
   ;

describeCommand
   : DESCRIBE tableSourceClause
   ;

explainCommand
    : EXPLAIN explainMode
    ;

explainMode
    : FORMATTED
    | COST
    | CODEGEN
    | EXTENDED
    | SIMPLE
    ;

showDataSourcesCommand
    : SHOW DATASOURCES
    ;

whereCommand
    : WHERE logicalExpression
    ;

correlateCommand
    : CORRELATE correlationType FIELDS LT_PRTHS fieldList RT_PRTHS (scopeClause)? mappingList
    ;

correlationType
    : SELF
    | EXACT
    | APPROXIMATE
    ;

scopeClause
    : SCOPE LT_PRTHS fieldExpression COMMA value = literalValue (unit = timespanUnit)? RT_PRTHS
    ;

mappingList
    : MAPPING LT_PRTHS ( mappingClause (COMMA mappingClause)* ) RT_PRTHS
    ;

mappingClause
    : left = qualifiedName comparisonOperator right = qualifiedName # mappingCompareExpr
    ;

fieldsCommand
   : FIELDS (PLUS | MINUS)? fieldList
   ;

renameCommand
   : RENAME renameClasue (COMMA renameClasue)*
   ;

statsCommand
   : STATS (PARTITIONS EQUAL partitions = integerLiteral)? (ALLNUM EQUAL allnum = booleanLiteral)? (DELIM EQUAL delim = stringLiteral)? statsAggTerm (COMMA statsAggTerm)* (statsByClause)? (DEDUP_SPLITVALUES EQUAL dedupsplit = booleanLiteral)?
   ;

dedupCommand
   : DEDUP (number = integerLiteral)? fieldList (KEEPEMPTY EQUAL keepempty = booleanLiteral)? (CONSECUTIVE EQUAL consecutive = booleanLiteral)?
   ;

sortCommand
   : SORT sortbyClause
   ;

evalCommand
   : EVAL evalClause (COMMA evalClause)*
   ;

headCommand
   : HEAD (number = integerLiteral)? (FROM from = integerLiteral)?
   ;

topCommand
   : TOP (number = integerLiteral)? fieldList (byClause)?
   ;

rareCommand
   : RARE fieldList (byClause)?
   ;

grokCommand
   : GROK (source_field = expression) (pattern = stringLiteral)
   ;

parseCommand
   : PARSE (source_field = expression) (pattern = stringLiteral)
   ;

patternsCommand
   : PATTERNS (patternsParameter)* (source_field = expression)
   ;

patternsParameter
   : (NEW_FIELD EQUAL new_field = stringLiteral)
   | (PATTERN EQUAL pattern = stringLiteral)
   ;

patternsMethod
   : PUNCT
   | REGEX
   ;

// lookup
lookupCommand
   : LOOKUP tableSource lookupMappingList ((APPEND | REPLACE) outputCandidateList)?
   ;

lookupMappingList
   : lookupPair (COMMA lookupPair)*
   ;

outputCandidateList
   : lookupPair (COMMA lookupPair)*
   ;

 // The lookup pair will generate a K-V pair.
 // The format is Key -> Alias(outputFieldName, inputField), Value -> outputField. For example:
 // 1. When lookupPair is "name AS cName", the key will be Alias(cName, Field(name)), the value will be Field(cName)
 // 2. When lookupPair is "dept", the key is Alias(dept, Field(dept)), value is Field(dept)
lookupPair
   : inputField = fieldExpression (AS outputField = fieldExpression)?
   ;

fillnullCommand
   : FILLNULL (fillNullWithTheSameValue
   | fillNullWithFieldVariousValues)
   ;

 fillNullWithTheSameValue
 : WITH nullReplacement IN nullableField (COMMA nullableField)*
 ;

 fillNullWithFieldVariousValues
 : USING nullableField EQUAL nullReplacement (COMMA nullableField EQUAL nullReplacement)*
 ;


   nullableField
   : fieldExpression
   ;

   nullReplacement
   : expression
   ;


kmeansCommand
   : KMEANS (kmeansParameter)*
   ;

kmeansParameter
   : (CENTROIDS EQUAL centroids = integerLiteral)
   | (ITERATIONS EQUAL iterations = integerLiteral)
   | (DISTANCE_TYPE EQUAL distance_type = stringLiteral)
   ;

adCommand
   : AD (adParameter)*
   ;

adParameter
   : (NUMBER_OF_TREES EQUAL number_of_trees = integerLiteral)
   | (SHINGLE_SIZE EQUAL shingle_size = integerLiteral)
   | (SAMPLE_SIZE EQUAL sample_size = integerLiteral)
   | (OUTPUT_AFTER EQUAL output_after = integerLiteral)
   | (TIME_DECAY EQUAL time_decay = decimalLiteral)
   | (ANOMALY_RATE EQUAL anomaly_rate = decimalLiteral)
   | (CATEGORY_FIELD EQUAL category_field = stringLiteral)
   | (TIME_FIELD EQUAL time_field = stringLiteral)
   | (DATE_FORMAT EQUAL date_format = stringLiteral)
   | (TIME_ZONE EQUAL time_zone = stringLiteral)
   | (TRAINING_DATA_SIZE EQUAL training_data_size = integerLiteral)
   | (ANOMALY_SCORE_THRESHOLD EQUAL anomaly_score_threshold = decimalLiteral)
   ;

mlCommand
   : ML (mlArg)*
   ;

mlArg
   : (argName = ident EQUAL argValue = literalValue)
   ;

// clauses
fromClause
   : SOURCE EQUAL tableSourceClause
   | INDEX EQUAL tableSourceClause
   ;

tableSourceClause
   : tableSource (COMMA tableSource)*
   ;

// join
joinCommand
   : (joinType) JOIN sideAlias joinHintList? joinCriteria? right = tableSource
   ;

joinType
   : INNER?
   | CROSS
   | LEFT OUTER?
   | RIGHT OUTER?
   | FULL OUTER?
   | LEFT? SEMI
   | LEFT? ANTI
   ;

sideAlias
   : LEFT EQUAL leftAlias = ident COMMA? RIGHT EQUAL rightAlias = ident
   ;

joinCriteria
   : ON logicalExpression
   ;

joinHintList
    : hintPair (COMMA? hintPair)*
    ;

hintPair
    : leftHintKey = LEFT_HINT DOT ID EQUAL leftHintValue = ident             #leftHint
    | rightHintKey = RIGHT_HINT DOT ID EQUAL rightHintValue = ident          #rightHint
    ;

renameClasue
   : orignalField = wcFieldExpression AS renamedField = wcFieldExpression
   ;

byClause
   : BY fieldList
   ;

statsByClause
   : BY fieldList
   | BY bySpanClause
   | BY bySpanClause COMMA fieldList
   ;

bySpanClause
   : spanClause (AS alias = qualifiedName)?
   ;

spanClause
   : SPAN LT_PRTHS fieldExpression COMMA value = literalValue (unit = timespanUnit)? RT_PRTHS
   ;

sortbyClause
   : sortField (COMMA sortField)*
   ;

evalClause
   : fieldExpression EQUAL expression
   ;

// aggregation terms
statsAggTerm
   : statsFunction (AS alias = wcFieldExpression)?
   ;

// aggregation functions
statsFunction
   : statsFunctionName LT_PRTHS valueExpression RT_PRTHS                                                                            # statsFunctionCall
   | COUNT LT_PRTHS RT_PRTHS                                                                                                        # countAllFunctionCall
   | (DISTINCT_COUNT | DC) LT_PRTHS valueExpression RT_PRTHS                                                                        # distinctCountFunctionCall
   | percentileFunctionName = (PERCENTILE | PERCENTILE_APPROX) LT_PRTHS valueExpression COMMA percent = integerLiteral RT_PRTHS     # percentileFunctionCall
   ;

statsFunctionName
   : AVG
   | COUNT
   | SUM
   | MIN
   | MAX
   | STDDEV_SAMP
   | STDDEV_POP
   ;

takeAggFunction
   : TAKE LT_PRTHS fieldExpression (COMMA size = integerLiteral)? RT_PRTHS
   ;

percentileAggFunction
   : PERCENTILE LESS value = integerLiteral GREATER LT_PRTHS aggField = fieldExpression RT_PRTHS
   ;

// expressions
expression
   : logicalExpression
   | valueExpression
   ;

logicalExpression
   : NOT logicalExpression                                      # logicalNot
   | comparisonExpression                                       # comparsion
   | left = logicalExpression (AND)? right = logicalExpression  # logicalAnd
   | left = logicalExpression OR right = logicalExpression      # logicalOr
   | left = logicalExpression XOR right = logicalExpression     # logicalXor
   | booleanExpression                                          # booleanExpr
   ;

comparisonExpression
   : left = valueExpression comparisonOperator right = valueExpression  # compareExpr
   | valueExpression IN valueList                                       # inExpr
   ;

valueExpressionList
   : valueExpression
   | LT_PRTHS valueExpression (COMMA valueExpression)* RT_PRTHS
   ;

valueExpression
   : left = valueExpression binaryOperator = (STAR | DIVIDE | MODULE) right = valueExpression   # binaryArithmetic
   | left = valueExpression binaryOperator = (PLUS | MINUS) right = valueExpression             # binaryArithmetic
   | primaryExpression                                                                          # valueExpressionDefault
   | positionFunction                                                                           # positionFunctionCall
   | caseFunction                                                                               # caseExpr
   | LT_PRTHS valueExpression RT_PRTHS                                                          # parentheticValueExpr
   | LT_SQR_PRTHS subSearch RT_SQR_PRTHS                                                        # scalarSubqueryExpr
   ;

primaryExpression
   : evalFunctionCall
   | fieldExpression
   | literalValue
   ;

positionFunction
   : positionFunctionName LT_PRTHS functionArg IN functionArg RT_PRTHS
   ;

booleanExpression
   : booleanFunctionCall                                                # booleanFunctionCallExpr
   | isEmptyExpression                                                  # isEmptyExpr
   | valueExpressionList NOT? IN LT_SQR_PRTHS subSearch RT_SQR_PRTHS    # inSubqueryExpr
   | EXISTS LT_SQR_PRTHS subSearch RT_SQR_PRTHS                         # existsSubqueryExpr
   ;

 isEmptyExpression
   : (ISEMPTY | ISBLANK) LT_PRTHS functionArg RT_PRTHS
   ;

 caseFunction
    : CASE LT_PRTHS logicalExpression COMMA valueExpression (COMMA logicalExpression COMMA valueExpression)* (ELSE valueExpression)? RT_PRTHS
    ;

relevanceExpression
   : singleFieldRelevanceFunction
   | multiFieldRelevanceFunction
   ;

// Field is a single column
singleFieldRelevanceFunction
   : singleFieldRelevanceFunctionName LT_PRTHS field = relevanceField COMMA query = relevanceQuery (COMMA relevanceArg)* RT_PRTHS
   ;

// Field is a list of columns
multiFieldRelevanceFunction
   : multiFieldRelevanceFunctionName LT_PRTHS LT_SQR_PRTHS field = relevanceFieldAndWeight (COMMA field = relevanceFieldAndWeight)* RT_SQR_PRTHS COMMA query = relevanceQuery (COMMA relevanceArg)* RT_PRTHS
   ;

// tables
tableSource
   : tableQualifiedName
   | ID_DATE_SUFFIX
   ;

tableFunction
   : qualifiedName LT_PRTHS functionArgs RT_PRTHS
   ;

// fields
fieldList
   : fieldExpression (COMMA fieldExpression)*
   ;

wcFieldList
   : wcFieldExpression (COMMA wcFieldExpression)*
   ;

sortField
   : (PLUS | MINUS)? sortFieldExpression
   ;

sortFieldExpression
   : fieldExpression
   | AUTO LT_PRTHS fieldExpression RT_PRTHS
   | STR LT_PRTHS fieldExpression RT_PRTHS
   | IP LT_PRTHS fieldExpression RT_PRTHS
   | NUM LT_PRTHS fieldExpression RT_PRTHS
   ;

fieldExpression
   : qualifiedName
   ;

wcFieldExpression
   : wcQualifiedName
   ;

// functions
evalFunctionCall
   : evalFunctionName LT_PRTHS functionArgs RT_PRTHS
   ;

// cast function
dataTypeFunctionCall
   : CAST LT_PRTHS expression AS convertedDataType RT_PRTHS
   ;

// boolean functions
booleanFunctionCall
   : conditionFunctionBase LT_PRTHS functionArgs RT_PRTHS
   ;

convertedDataType
   : typeName = DATE
   | typeName = TIME
   | typeName = TIMESTAMP
   | typeName = INT
   | typeName = INTEGER
   | typeName = DOUBLE
   | typeName = LONG
   | typeName = FLOAT
   | typeName = STRING
   | typeName = BOOLEAN
   ;

evalFunctionName
   : mathematicalFunctionName
   | dateTimeFunctionName
   | textFunctionName
   | conditionFunctionBase
   | systemFunctionName
   | positionFunctionName
   | coalesceFunctionName
   ;

functionArgs
   : (functionArg (COMMA functionArg)*)?
   ;

functionArg
   : (ident EQUAL)? valueExpression
   ;

relevanceArg
   : relevanceArgName EQUAL relevanceArgValue
   ;

relevanceArgName
   : ALLOW_LEADING_WILDCARD
   | ANALYZER
   | ANALYZE_WILDCARD
   | AUTO_GENERATE_SYNONYMS_PHRASE_QUERY
   | BOOST
   | CUTOFF_FREQUENCY
   | DEFAULT_FIELD
   | DEFAULT_OPERATOR
   | ENABLE_POSITION_INCREMENTS
   | ESCAPE
   | FIELDS
   | FLAGS
   | FUZZINESS
   | FUZZY_MAX_EXPANSIONS
   | FUZZY_PREFIX_LENGTH
   | FUZZY_REWRITE
   | FUZZY_TRANSPOSITIONS
   | LENIENT
   | LOW_FREQ_OPERATOR
   | MAX_DETERMINIZED_STATES
   | MAX_EXPANSIONS
   | MINIMUM_SHOULD_MATCH
   | OPERATOR
   | PHRASE_SLOP
   | PREFIX_LENGTH
   | QUOTE_ANALYZER
   | QUOTE_FIELD_SUFFIX
   | REWRITE
   | SLOP
   | TIE_BREAKER
   | TIME_ZONE
   | TYPE
   | ZERO_TERMS_QUERY
   ;

relevanceFieldAndWeight
   : field = relevanceField
   | field = relevanceField weight = relevanceFieldWeight
   | field = relevanceField BIT_XOR_OP weight = relevanceFieldWeight
   ;

relevanceFieldWeight
   : integerLiteral
   | decimalLiteral
   ;

relevanceField
   : qualifiedName
   | stringLiteral
   ;

relevanceQuery
   : relevanceArgValue
   ;

relevanceArgValue
   : qualifiedName
   | literalValue
   ;

mathematicalFunctionName
   : ABS
   | CBRT
   | CEIL
   | CEILING
   | CONV
   | CRC32
   | E
   | EXP
   | FLOOR
   | LN
   | LOG
   | LOG10
   | LOG2
   | MOD
   | PI
   | POW
   | POWER
   | RAND
   | ROUND
   | SIGN
   | SIGNUM
   | SQRT
   | TRUNCATE
   | trigonometricFunctionName
   ;

trigonometricFunctionName
   : ACOS
   | ASIN
   | ATAN
   | ATAN2
   | COS
   | COT
   | DEGREES
   | RADIANS
   | SIN
   | TAN
   ;

dateTimeFunctionName
   : ADDDATE
   | ADDTIME
   | CONVERT_TZ
   | CURDATE
   | CURRENT_DATE
   | CURRENT_TIME
   | CURRENT_TIMESTAMP
   | CURTIME
   | DATE
   | DATEDIFF
   | DATETIME
   | DATE_ADD
   | DATE_FORMAT
   | DATE_SUB
   | DAY
   | DAYNAME
   | DAYOFMONTH
   | DAYOFWEEK
   | DAYOFYEAR
   | DAY_OF_MONTH
   | DAY_OF_WEEK
   | DAY_OF_YEAR
   | FROM_DAYS
   | FROM_UNIXTIME
   | HOUR
   | HOUR_OF_DAY
   | LAST_DAY
   | LOCALTIME
   | LOCALTIMESTAMP
   | MAKEDATE
   | MAKE_DATE
   | MAKETIME
   | MICROSECOND
   | MINUTE
   | MINUTE_OF_DAY
   | MINUTE_OF_HOUR
   | MONTH
   | MONTHNAME
   | MONTH_OF_YEAR
   | NOW
   | PERIOD_ADD
   | PERIOD_DIFF
   | QUARTER
   | SECOND
   | SECOND_OF_MINUTE
   | SEC_TO_TIME
   | STR_TO_DATE
   | SUBDATE
   | SUBTIME
   | SYSDATE
   | TIME
   | TIMEDIFF
   | TIMESTAMP
   | TIME_FORMAT
   | TIME_TO_SEC
   | TO_DAYS
   | TO_SECONDS
   | UNIX_TIMESTAMP
   | UTC_DATE
   | UTC_TIME
   | UTC_TIMESTAMP
   | WEEK
   | WEEKDAY
   | WEEK_OF_YEAR
   | YEAR
   | YEARWEEK
   ;

getFormatFunction
   : GET_FORMAT LT_PRTHS getFormatType COMMA functionArg RT_PRTHS
   ;

getFormatType
   : DATE
   | DATETIME
   | TIME
   | TIMESTAMP
   ;

extractFunction
   : EXTRACT LT_PRTHS datetimePart FROM functionArg RT_PRTHS
   ;

simpleDateTimePart
   : MICROSECOND
   | SECOND
   | MINUTE
   | HOUR
   | DAY
   | WEEK
   | MONTH
   | QUARTER
   | YEAR
   ;

complexDateTimePart
   : SECOND_MICROSECOND
   | MINUTE_MICROSECOND
   | MINUTE_SECOND
   | HOUR_MICROSECOND
   | HOUR_SECOND
   | HOUR_MINUTE
   | DAY_MICROSECOND
   | DAY_SECOND
   | DAY_MINUTE
   | DAY_HOUR
   | YEAR_MONTH
   ;

datetimePart
   : simpleDateTimePart
   | complexDateTimePart
   ;

timestampFunction
   : timestampFunctionName LT_PRTHS simpleDateTimePart COMMA firstArg = functionArg COMMA secondArg = functionArg RT_PRTHS
   ;

timestampFunctionName
   : TIMESTAMPADD
   | TIMESTAMPDIFF
   ;

// condition function return boolean value
conditionFunctionBase
   : LIKE
   | IF
   | ISNULL
   | ISNOTNULL
   | IFNULL
   | NULLIF
   | ISPRESENT
   ;

systemFunctionName
   : TYPEOF
   ;

textFunctionName
   : SUBSTR
   | SUBSTRING
   | TRIM
   | LTRIM
   | RTRIM
   | LOWER
   | UPPER
   | CONCAT
   | CONCAT_WS
   | LENGTH
   | STRCMP
   | RIGHT
   | LEFT
   | ASCII
   | LOCATE
   | REPLACE
   | REVERSE
   | ISEMPTY
   | ISBLANK
   ;

positionFunctionName
   : POSITION
   ;

coalesceFunctionName
   : COALESCE
   ;

// operators
 comparisonOperator
   : EQUAL
   | NOT_EQUAL
   | LESS
   | NOT_LESS
   | GREATER
   | NOT_GREATER
   | REGEXP
   ;

singleFieldRelevanceFunctionName
   : MATCH
   | MATCH_PHRASE
   | MATCH_BOOL_PREFIX
   | MATCH_PHRASE_PREFIX
   ;

multiFieldRelevanceFunctionName
   : SIMPLE_QUERY_STRING
   | MULTI_MATCH
   | QUERY_STRING
   ;

// literals and values
literalValue
   : stringLiteral
   | integerLiteral
   | decimalLiteral
   | booleanLiteral
   | datetimeLiteral //#datetime
   ;

intervalLiteral
   : INTERVAL valueExpression intervalUnit
   ;

stringLiteral
   : DQUOTA_STRING
   | SQUOTA_STRING
   ;

integerLiteral
   : (PLUS | MINUS)? INTEGER_LITERAL
   ;

decimalLiteral
   : (PLUS | MINUS)? DECIMAL_LITERAL
   ;

booleanLiteral
   : TRUE
   | FALSE
   ;

// Date and Time Literal, follow ANSI 92
datetimeLiteral
   : dateLiteral
   | timeLiteral
   | timestampLiteral
   ;

dateLiteral
   : DATE date = stringLiteral
   ;

timeLiteral
   : TIME time = stringLiteral
   ;

timestampLiteral
   : TIMESTAMP timestamp = stringLiteral
   ;

intervalUnit
   : MICROSECOND
   | SECOND
   | MINUTE
   | HOUR
   | DAY
   | WEEK
   | MONTH
   | QUARTER
   | YEAR
   | SECOND_MICROSECOND
   | MINUTE_MICROSECOND
   | MINUTE_SECOND
   | HOUR_MICROSECOND
   | HOUR_SECOND
   | HOUR_MINUTE
   | DAY_MICROSECOND
   | DAY_SECOND
   | DAY_MINUTE
   | DAY_HOUR
   | YEAR_MONTH
   ;

timespanUnit
   : MS
   | S
   | M
   | H
   | D
   | W
   | Q
   | Y
   | MILLISECOND
   | SECOND
   | MINUTE
   | HOUR
   | DAY
   | WEEK
   | MONTH
   | QUARTER
   | YEAR
   ;

valueList
   : LT_PRTHS literalValue (COMMA literalValue)* RT_PRTHS
   ;

qualifiedName
   : ident (DOT ident)* # identsAsQualifiedName
   ;

tableQualifiedName
   : tableIdent (DOT ident)* # identsAsTableQualifiedName
   ;

wcQualifiedName
   : wildcard (DOT wildcard)* # identsAsWildcardQualifiedName
   ;

ident
   : (DOT)? ID
   | BACKTICK ident BACKTICK
   | BQUOTA_STRING
   | keywordsCanBeId
   ;

tableIdent
   : (CLUSTER)? ident
   ;

wildcard
   : ident (MODULE ident)* (MODULE)?
   | SINGLE_QUOTE wildcard SINGLE_QUOTE
   | DOUBLE_QUOTE wildcard DOUBLE_QUOTE
   | BACKTICK wildcard BACKTICK
   ;

keywordsCanBeId
   : D // OD SQL and ODBC special
   | timespanUnit
   | SPAN
   | evalFunctionName
   | relevanceArgName
   | intervalUnit
   | dateTimeFunctionName
   | textFunctionName
   | mathematicalFunctionName
   | positionFunctionName
   // commands
   | SEARCH
   | DESCRIBE
   | SHOW
   | FROM
   | WHERE
   | CORRELATE
   | FIELDS
   | RENAME
   | STATS
   | DEDUP
   | SORT
   | EVAL
   | HEAD
   | TOP
   | RARE
   | PARSE
   | METHOD
   | REGEX
   | PUNCT
   | GROK
   | PATTERN
   | PATTERNS
   | NEW_FIELD
   | KMEANS
   | AD
   | ML
   | EXPLAIN
   // commands assist keywords
   | SOURCE
   | INDEX
   | DESC
   | DATASOURCES
   // CLAUSEKEYWORDS
   | SORTBY
   // FIELDKEYWORDSAUTO
   | STR
   | IP
   | NUM
   // ARGUMENT KEYWORDS
   | KEEPEMPTY
   | CONSECUTIVE
   | DEDUP_SPLITVALUES
   | PARTITIONS
   | ALLNUM
   | DELIM
   | CENTROIDS
   | ITERATIONS
   | DISTANCE_TYPE
   | NUMBER_OF_TREES
   | SHINGLE_SIZE
   | SAMPLE_SIZE
   | OUTPUT_AFTER
   | TIME_DECAY
   | ANOMALY_RATE
   | CATEGORY_FIELD
   | TIME_FIELD
   | TIME_ZONE
   | TRAINING_DATA_SIZE
   | ANOMALY_SCORE_THRESHOLD
   // AGGREGATIONS
   | AVG
   | COUNT
   | DISTINCT_COUNT
   | ESTDC
   | ESTDC_ERROR
   | MAX
   | MEAN
   | MEDIAN
   | MIN
   | MODE
   | RANGE
   | STDEV
   | STDEVP
   | SUM
   | SUMSQ
   | VAR_SAMP
   | VAR_POP
   | STDDEV_SAMP
   | STDDEV_POP
   | PERCENTILE
   | PERCENTILE_APPROX
   | TAKE
   | FIRST
   | LAST
   | LIST
   | VALUES
   | EARLIEST
   | EARLIEST_TIME
   | LATEST
   | LATEST_TIME
   | PER_DAY
   | PER_HOUR
   | PER_MINUTE
   | PER_SECOND
   | RATE
   | SPARKLINE
   | C
   | DC
   // JOIN TYPE
   | OUTER
   | INNER
   | CROSS
   | LEFT
   | RIGHT
   | FULL
   | SEMI
   | ANTI
   ;
