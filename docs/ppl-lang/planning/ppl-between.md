## between syntax proposal

1. **Proposed syntax**
    - `... | where expr1 [NOT] BETWEEN expr2 AND expr3`
        - evaluate if expr1 is [not] in between expr2 and expr3 
    - `... | where a between 1 and 4` - Note: This returns a >= 1 and a <= 4, i.e. [1, 4]
    - `... | where b not between '2024-09-10' and '2025-09-10'` - Note: This returns b >= '2024-09-10' and b <= '2025-09-10'

### New syntax definition in ANTLR

```ANTLR
  
logicalExpression
   ...
   | expr1 = functionArg NOT? BETWEEN expr2 = functionArg AND expr3 = functionArg   # between

```