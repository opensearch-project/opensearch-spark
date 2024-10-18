## PPL JSON Functions

### `JSON_OBJECT`

**Description**

`json_object(<key>, <value>[, <key>, <value>]...)` returns a JSON object string from members of key-value pairs.

**Argument type:**
- A \<key\> must be STRING.
- A \<value\> can be any data types.

**Return type:** STRING

A STRING expression of a valid JSON object format.

Example:

    os> source=people | eval `json_object('key', 123.45)` = json_object('key', 123.45) | fields `json_object('key', 123.45)`
    fetched rows / total rows = 1/1
    +------------------------------+
    | json_object('key', 123.45)   |
    +------------------------------+
    | {"key":123.45}               |
    +------------------------------+


### `JSON`

**Description**

`json(value)` Evaluates whether a string can be parsed as JSON. Returns the json string if valid, null otherwise.


**Argument type:** STRING

**Return type:** STRING

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


### `JSON_ARRAY`

**Description**

`json_array(<value>...)` Creates a JSON ARRAY string using a list of values.

**Argument type:**
- A \<value\> can be any kind of value such as string, number, or boolean.

**Return type:** STRING

A STRING expression of a valid JSON array format.

Example:

    os> source=people | eval `json_array` = json_array(1, 2, 0, -1, 1.1, -0.11)
    fetched rows / total rows = 1/1
    +------------------------------+
    | json_array                   |
    +------------------------------+
    | [1.0,2.0,0.0,-1.0,1.1,-0.11] |
    +------------------------------+


### `JSON_ARRAY_LENGTH`

**Description**

`json_array_length(jsonArray)` Returns the number of elements in the outermost JSON array.

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


### `JSON_EXTRACT`

**Description**

`json_extract(jsonStr, path)` Extracts json object from a json string based on json path specified. Return null if the input json string is invalid.

**Argument type:** STRING, STRING

**Return type:** STRING

A STRING expression of a valid JSON object format.

`NULL` is returned in case of an invalid JSON.

Example:

    os> source=people | eval `json_extract('{"a":"b"}', 'a')` = json_extract('{"a":"b"}', 'a')
    fetched rows / total rows = 1/1
    +----------------------------------+
    | json_extract('{"a":"b"}', 'a')   |
    +----------------------------------+
    | b                                |
    +----------------------------------+

    os> source=people | eval `invalid_json` = json_extract('{"invalid": "json"')
    fetched rows / total rows = 1/1
    +----------------+
    | invalid_json   |
    +----------------+
    | null           |
    +----------------+


### `JSON_KEYS`

**Description**

`json_keys()(jsonStr)` Returns all the keys of the outermost JSON object as an array.

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
