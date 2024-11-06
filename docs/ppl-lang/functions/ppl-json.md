## PPL JSON Functions

### `JSON`

**Description**

`json(value)` Evaluates whether a string can be parsed as JSON format. Returns the string value if valid, null otherwise.

**Argument type:** STRING

**Return type:** STRING/NULL

A STRING expression of a valid JSON object format.

Example:

    os> source=people | eval `valid_json()` = json('[1,2,3,{"f1":1,"f2":[5,6]},4]') | fields valid_json
    fetched rows / total rows = 1/1
    +---------------------------------+
    | valid_json                      |
    +---------------------------------+
    | [1,2,3,{"f1":1,"f2":[5,6]},4]   |
    +---------------------------------+

    os> source=people | eval `invalid_json()` = json('{"invalid": "json"') | fields invalid_json
    fetched rows / total rows = 1/1
    +----------------+
    | invalid_json   |
    +----------------+
    | null           |
    +----------------+


### `JSON_OBJECT`

**Description**

`json_object(<key>, <value>[, <key>, <value>]...)` returns a JSON object from members of key-value pairs.

**Argument type:**
- A \<key\> must be STRING.
- A \<value\> can be any data types.

**Return type:** JSON_OBJECT (Spark StructType)

A StructType expression of a valid JSON object.

Example:

    os> source=people | eval result = json_object('key', 123.45) | fields result
    fetched rows / total rows = 1/1
    +------------------+
    | result           |
    +------------------+
    | {"key":123.45}   |
    +------------------+

    os> source=people | eval result = json_object('outer', json_object('inner', 123.45)) | fields result
    fetched rows / total rows = 1/1
    +------------------------------+
    | result                       |
    +------------------------------+
    | {"outer":{"inner":123.45}}   |
    +------------------------------+


### `JSON_ARRAY`

**Description**

`json_array(<value>...)` Creates a JSON ARRAY using a list of values.

**Argument type:**
- A \<value\> can be any kind of value such as string, number, or boolean.

**Return type:** ARRAY (Spark ArrayType)

An array of any supported data type for a valid JSON array.

Example:

    os> source=people | eval `json_array` = json_array(1, 2, 0, -1, 1.1, -0.11)
    fetched rows / total rows = 1/1
    +------------------------------+
    | json_array                   |
    +------------------------------+
    | [1.0,2.0,0.0,-1.0,1.1,-0.11] |
    +------------------------------+

    os> source=people | eval `json_array_object` = json_object("array", json_array(1, 2, 0, -1, 1.1, -0.11))
    fetched rows / total rows = 1/1
    +----------------------------------------+
    | json_array_object                      |
    +----------------------------------------+
    | {"array":[1.0,2.0,0.0,-1.0,1.1,-0.11]} |
    +----------------------------------------+

### `TO_JSON_STRING`

**Description**

`to_json_string(jsonObject)` Returns a JSON string with a given json object value.

**Argument type:** JSON_OBJECT (Spark StructType/ArrayType)

**Return type:** STRING

Example:

    os> source=people | eval `json_string` = to_json_string(json_array(1, 2, 0, -1, 1.1, -0.11)) | fields json_string
    fetched rows / total rows = 1/1
    +--------------------------------+
    | json_string                    |
    +--------------------------------+
    | [1.0,2.0,0.0,-1.0,1.1,-0.11]   |
    +--------------------------------+

    os> source=people | eval `json_string` = to_json_string(json_object('key', 123.45)) | fields json_string
    fetched rows / total rows = 1/1
    +-----------------+
    | json_string     |
    +-----------------+
    | {'key', 123.45} |
    +-----------------+


### `JSON_ARRAY_LENGTH`

**Description**

`json_array_length(jsonArrayString)` Returns the number of elements in the outermost JSON array string.

**Argument type:** STRING

A STRING expression of a valid JSON array format.

**Return type:** INTEGER

`NULL` is returned in case of any other valid JSON string, `NULL` or an invalid JSON.

Example:

    os> source=people | eval `lenght1` = json_array_length('[1,2,3,4]'), `lenght2` = json_array_length('[1,2,3,{"f1":1,"f2":[5,6]},4]'), `not_array` = json_array_length('{"key": 1}')
    fetched rows / total rows = 1/1
    +-----------+-----------+-------------+
    | lenght1   | lenght2   | not_array   |
    +-----------+-----------+-------------+
    | 4         | 5         | null        |
    +-----------+-----------+-------------+


### `ARRAY_LENGTH`

**Description**

`array_length(jsonArray)` Returns the number of elements in the outermost array.

**Argument type:** ARRAY

ARRAY or JSON_ARRAY object.

**Return type:** INTEGER

Example:

    os> source=people | eval `json_array` = json_array_length(json_array(1,2,3,4)), `empty_array` = json_array_length(json_array())
    fetched rows / total rows = 1/1
    +--------------+---------------+
    | json_array   | empty_array   |
    +--------------+---------------+
    | 4            | 0             |
    +--------------+---------------+


### `JSON_EXTRACT`

**Description**

`json_extract(jsonStr, path)` Extracts json object from a json string based on json path specified. Return null if the input json string is invalid.

**Argument type:** STRING, STRING

**Return type:** STRING

A STRING expression of a valid JSON object format.

`NULL` is returned in case of an invalid JSON.

Example:

    os> source=people | eval `json_extract('{"a":"b"}', '$.a')` = json_extract('{"a":"b"}', '$a')
    fetched rows / total rows = 1/1
    +----------------------------------+
    | json_extract('{"a":"b"}', 'a')   |
    +----------------------------------+
    | b                                |
    +----------------------------------+

    os> source=people | eval `json_extract('{"a":[{"b":1},{"b":2}]}', '$.a[1].b')` = json_extract('{"a":[{"b":1},{"b":2}]}', '$.a[1].b')
    fetched rows / total rows = 1/1
    +-----------------------------------------------------------+
    | json_extract('{"a":[{"b":1.0},{"b":2.0}]}', '$.a[1].b')   |
    +-----------------------------------------------------------+
    | 2.0                                                       |
    +-----------------------------------------------------------+

    os> source=people | eval `json_extract('{"a":[{"b":1},{"b":2}]}', '$.a[*].b')` = json_extract('{"a":[{"b":1},{"b":2}]}', '$.a[*].b')
    fetched rows / total rows = 1/1
    +-----------------------------------------------------------+
    | json_extract('{"a":[{"b":1.0},{"b":2.0}]}', '$.a[*].b')   |
    +-----------------------------------------------------------+
    | [1.0,2.0]                                                 |
    +-----------------------------------------------------------+

    os> source=people | eval `invalid_json` = json_extract('{"invalid": "json"')
    fetched rows / total rows = 1/1
    +----------------+
    | invalid_json   |
    +----------------+
    | null           |
    +----------------+


### `JSON_KEYS`

**Description**

`json_keys(jsonStr)` Returns all the keys of the outermost JSON object as an array.

**Argument type:** STRING

A STRING expression of a valid JSON object format.

**Return type:** ARRAY[STRING]

`NULL` is returned in case of any other valid JSON string, or an empty string, or an invalid JSON.

Example:

    os> source=people | eval `keys` = json_keys('{"f1":"abc","f2":{"f3":"a","f4":"b"}}')
    fetched rows / total rows = 1/1
    +------------+
    | keus       |
    +------------+
    | [f1, f2]   |
    +------------+

    os> source=people | eval `keys` = json_keys('[1,2,3,{"f1":1,"f2":[5,6]},4]')
    fetched rows / total rows = 1/1
    +--------+
    | keys   |
    +--------+
    | null   |
    +--------+

### `JSON_VALID`

**Description**

`json_valid(jsonStr)` Evaluates whether a JSON string uses valid JSON syntax and returns TRUE or FALSE.

**Argument type:** STRING

**Return type:** BOOLEAN

Example:

    os> source=people | eval `valid_json` = json_valid('[1,2,3,4]'), `invalid_json` = json_valid('{"invalid": "json"') | feilds `valid_json`, `invalid_json`
    fetched rows / total rows = 1/1
    +--------------+----------------+
    | valid_json   | invalid_json   |
    +--------------+----------------+
    | True         | False          |
    +--------------+----------------+

    os> source=accounts | where json_valid('[1,2,3,4]') and isnull(email) | fields account_number, email
    fetched rows / total rows = 1/1
    +------------------+---------+
    | account_number   | email   |
    |------------------+---------|
    | 13               | null    |
    +------------------+---------+
