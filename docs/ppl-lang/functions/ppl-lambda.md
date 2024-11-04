## Lambda Functions

### `FORALL`

**Description**

`forall(array, lambda)` Evaluates whether a lambda predicate holds for all elements in the array.

**Argument type:** ARRAY, LAMBDA

**Return type:** BOOLEAN

Returns `TRUE` if all elements in the array satisfy the lambda predicate, otherwise `FALSE`.

Example:

    os> source=people | eval array = json_array(1, -1, 2), result = forall(array, x -> x > 0) | fields result
    fetched rows / total rows = 1/1
    +-----------+
    | result    |
    +-----------+
    | false     |
    +-----------+

    os> source=people | eval array = json_array(1, 3, 2), result = forall(array, x -> x > 0) | fields result
    fetched rows / total rows = 1/1
    +-----------+
    | result    |
    +-----------+
    | true      |
    +-----------+

 **Note:** The lambda expression can access the nested fields of the array elements. This applies to all lambda functions introduced in this document. See the examples below:

    os> source=people | eval array = json_array(json_object("a", 1, "b", 1), json_object("a" , -1, "b", 2)), result = forall(array, x -> x.a > 0) | fields result
    fetched rows / total rows = 1/1
    +-----------+
    | result    |
    +-----------+
    | false     |
    +-----------+

    os> source=people | eval array = json_array(json_object("a", 1, "b", 1), json_object("a" , -1, "b", 2)), result = forall(array, x -> x.b > 0) | fields result
    fetched rows / total rows = 1/1
    +-----------+
    | result    |
    +-----------+
    | true      |
    +-----------+

### `EXISTS`

**Description**

`exists(array, lambda)` Evaluates whether a lambda predicate holds for one or more elements in the array.

**Argument type:** ARRAY, LAMBDA

**Return type:** BOOLEAN

Returns `TRUE` if at least one element in the array satisfies the lambda predicate, otherwise `FALSE`.

Example:

    os> source=people | eval array = json_array(1, -1, 2), result = exists(array, x -> x > 0) | fields result
    fetched rows / total rows = 1/1
    +-----------+
    | result    |
    +-----------+
    | true      |
    +-----------+

    os> source=people | eval array = json_array(-1, -3, -2), result = exists(array, x -> x > 0) | fields result
    fetched rows / total rows = 1/1
    +-----------+
    | result    |
    +-----------+
    | false     |
    +-----------+


### `FILTER`

**Description**

`filter(array, lambda)`  Filters the input array using the given lambda function.

**Argument type:** ARRAY, LAMBDA

**Return type:** ARRAY

An ARRAY that contains all elements in the input array that satisfy the lambda predicate.

Example:

    os> source=people | eval array = json_array(1, -1, 2), result = filter(array, x -> x > 0) | fields result
    fetched rows / total rows = 1/1
    +-----------+
    | result    |
    +-----------+
    | [1, 2]    |
    +-----------+

    os> source=people | eval array = json_array(-1, -3, -2), result = filter(array, x -> x > 0) | fields result
    fetched rows / total rows = 1/1
    +-----------+
    | result    |
    +-----------+
    | []        |
    +-----------+

### `TRANSFORM`

**Description**

`transform(array, lambda)` Transform elements in an array using the lambda transform function. The second argument implies the index of the element if using binary lambda function. This is similar to a `map` in functional programming.

**Argument type:** ARRAY, LAMBDA

**Return type:** ARRAY

An ARRAY that contains the result of applying the lambda transform function to each element in the input array.

Example:

    os> source=people | eval array = json_array(1, 2, 3), result = transform(array, x -> x + 1) | fields result
    fetched rows / total rows = 1/1
    +--------------+
    | result       |
    +--------------+
    | [2, 3, 4]    |
    +--------------+

    os> source=people | eval array = json_array(1, 2, 3), result = transform(array, (x, i) -> x + i) | fields result
    fetched rows / total rows = 1/1
    +--------------+
    | result       |
    +--------------+
    | [1, 3, 5]    |
    +--------------+

### `REDUCE`

**Description**

`reduce(array, start, merge_lambda, finish_lambda)` Applies a binary merge lambda function to a start value and all elements in the array, and reduces this to a single state. The final state is converted into the final result by applying a finish lambda function.

**Argument type:** ARRAY, ANY, LAMBDA, LAMBDA

**Return type:** ANY

The final result of applying the lambda functions to the start value and the input array.

Example:

    os> source=people | eval array = json_array(1, 2, 3), result = reduce(array, 0, (acc, x) -> acc + x) | fields result
    fetched rows / total rows = 1/1
    +-----------+
    | result    |
    +-----------+
    | 6         |
    +-----------+

    os> source=people | eval array = json_array(1, 2, 3), result = reduce(array, 10, (acc, x) -> acc + x) | fields result
    fetched rows / total rows = 1/1
    +-----------+
    | result    |
    +-----------+
    | 16        |
    +-----------+

    os> source=people | eval array = json_array(1, 2, 3), result = reduce(array, 0, (acc, x) -> acc + x, acc -> acc * 10) | fields result
    fetched rows / total rows = 1/1
    +-----------+
    | result    |
    +-----------+
    | 60        |
    +-----------+
