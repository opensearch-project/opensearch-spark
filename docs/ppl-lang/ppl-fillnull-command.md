## fillnull syntax proposal

1. **Proposed syntax changes with `null` replacement with the same values in various fields**
    - `... | fillnull with 0 in field1`
    - `... | fillnull with 'N/A' in field1, field2, field3`
    - `... | fillnull with 2*pi() + field1 in field2`
    - `... | fillnull with concat(field1, field2) in field3, field4`
    - `... | fillnull with 'N/A'`
        - incorrect syntax
    - `... | fillnull with 'N/A' in`
        - validation error related to missing columns

2. **Proposed syntax changes with `null` replacement with the various values in various fields**
* currently implemented, not conform to previous syntax proposal (`fields` addition)
    - `... | fillnull fields status_code=101`
    -  `... | fillnull fields request_path='/not_found', timestamp='*'`
* New syntax proposal
    - `... | fillnull using field1=101`
    - `... | fillnull using field1=concat(field2, field3), field4=2*pi()*field5`
    - `... | fillnull using field1=concat(field2, field3), field4=2*pi()*field5, field6 = 'N/A'`
    - `... | fillnull using`
        -  validation error related to missing columns

### New syntax definition in ANTLR

```ANTLR
  
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

```