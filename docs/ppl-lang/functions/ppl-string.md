## PPL String Functions

### `CONCAT`

**Description**

`CONCAT(str1, str2, ...., str_9)` adds up to 9 strings together.

**Argument type:**
 - STRING, STRING, ...., STRING
 - Return type: **STRING**

Example:

    os> source=people | eval `CONCAT('hello', 'world')` = CONCAT('hello', 'world'), `CONCAT('hello ', 'whole ', 'world', '!')` = CONCAT('hello ', 'whole ', 'world', '!') | fields `CONCAT('hello', 'world')`, `CONCAT('hello ', 'whole ', 'world', '!')`
    fetched rows / total rows = 1/1
    +----------------------------+--------------------------------------------+
    | CONCAT('hello', 'world')   | CONCAT('hello ', 'whole ', 'world', '!')   |
    |----------------------------+--------------------------------------------|
    | helloworld                 | hello whole world!                         |
    +----------------------------+--------------------------------------------+


### `CONCAT_WS`

**Description**

`CONCAT_WS(sep, str1, str2)` returns str1 concatenated with str2 using sep as a separator between them.

**Argument type:**
- STRING, STRING, ...., STRING
- Return type: **STRING**

Example:

    os> source=people | eval `CONCAT_WS(',', 'hello', 'world')` = CONCAT_WS(',', 'hello', 'world') | fields `CONCAT_WS(',', 'hello', 'world')`
    fetched rows / total rows = 1/1
    +------------------------------------+
    | CONCAT_WS(',', 'hello', 'world')   |
    |------------------------------------|
    | hello,world                        |
    +------------------------------------+


### `LENGTH`
------

**Description**

Specifications:

`length(str)` returns length of string measured in bytes.

**Argument type:**
 - STRING
 - Return type: **INTEGER**

Example:

    os> source=people | eval `LENGTH('helloworld')` = LENGTH('helloworld') | fields `LENGTH('helloworld')`
    fetched rows / total rows = 1/1
    +------------------------+
    | LENGTH('helloworld')   |
    |------------------------|
    | 10                     |
    +------------------------+

### `LOWER`

**Description**

`lower(string)` converts the string to lowercase.

**Argument type:**
 - STRING
 - Return type: **STRING**

Example:

    os> source=people | eval `LOWER('helloworld')` = LOWER('helloworld'), `LOWER('HELLOWORLD')` = LOWER('HELLOWORLD') | fields `LOWER('helloworld')`, `LOWER('HELLOWORLD')`
    fetched rows / total rows = 1/1
    +-----------------------+-----------------------+
    | LOWER('helloworld')   | LOWER('HELLOWORLD')   |
    |-----------------------+-----------------------|
    | helloworld            | helloworld            |
    +-----------------------+-----------------------+


### `LTRIM`

**Description**

`ltrim(str)` trims leading space characters from the string.

**Argument type:**
 - STRING
 - Return type: **STRING**

Example:

    os> source=people | eval `LTRIM('   hello')` = LTRIM('   hello'), `LTRIM('hello   ')` = LTRIM('hello   ') | fields `LTRIM('   hello')`, `LTRIM('hello   ')`
    fetched rows / total rows = 1/1
    +---------------------+---------------------+
    | LTRIM('   hello')   | LTRIM('hello   ')   |
    |---------------------+---------------------|
    | hello               | hello               |
    +---------------------+---------------------+


### `POSITION`

**Description**

The syntax `POSITION(substr IN str)` returns the position of the first occurrence of substring substr in string str. Returns 0 if substr is not in str. Returns NULL if any argument is NULL.

**Argument type:**
 -  STRING, STRING
 -  Return type **INTEGER**


Example:

    os> source=people | eval `POSITION('world' IN 'helloworld')` = POSITION('world' IN 'helloworld'), `POSITION('invalid' IN 'helloworld')`= POSITION('invalid' IN 'helloworld')  | fields `POSITION('world' IN 'helloworld')`, `POSITION('invalid' IN 'helloworld')`
    fetched rows / total rows = 1/1
    +-------------------------------------+---------------------------------------+
    | POSITION('world' IN 'helloworld')   | POSITION('invalid' IN 'helloworld')   |
    |-------------------------------------+---------------------------------------|
    | 6                                   | 0                                     |
    +-------------------------------------+---------------------------------------+


### `REVERSE`

**Description**

`REVERSE(str)` returns reversed string of the string supplied as an argument.

**Argument type:**
  - STRING
  - Return type: **STRING**

Example:

    os> source=people | eval `REVERSE('abcde')` = REVERSE('abcde') | fields `REVERSE('abcde')`
    fetched rows / total rows = 1/1
    +--------------------+
    | REVERSE('abcde')   |
    |--------------------|
    | edcba              |
    +--------------------+


### `RIGHT`

**Description**

`right(str, len)` returns the rightmost len characters from the string str, or NULL if any argument is NULL.

**Argument type:**
  - STRING, INTEGER
  - Return type: **STRING**

Example:

    os> source=people | eval `RIGHT('helloworld', 5)` = RIGHT('helloworld', 5), `RIGHT('HELLOWORLD', 0)` = RIGHT('HELLOWORLD', 0) | fields `RIGHT('helloworld', 5)`, `RIGHT('HELLOWORLD', 0)`
    fetched rows / total rows = 1/1
    +--------------------------+--------------------------+
    | RIGHT('helloworld', 5)   | RIGHT('HELLOWORLD', 0)   |
    |--------------------------+--------------------------|
    | world                    |                          |
    +--------------------------+--------------------------+


### `RTRIM`

**Description**

`rtrim(str)` trims trailing space characters from the string.

**Argument type:**
 - STRING
 - Return type: **STRING**

Example:

    os> source=people | eval `RTRIM('   hello')` = RTRIM('   hello'), `RTRIM('hello   ')` = RTRIM('hello   ') | fields `RTRIM('   hello')`, `RTRIM('hello   ')`
    fetched rows / total rows = 1/1
    +---------------------+---------------------+
    | RTRIM('   hello')   | RTRIM('hello   ')   |
    |---------------------+---------------------|
    |    hello            | hello               |
    +---------------------+---------------------+


### `SUBSTRING`

**Description**

`substring(str, start)` or `substring(str, start, length)` returns substring using start and length. With no length, entire string from start is returned.

**Argument type:**
 - STRING, INTEGER, INTEGER
 - Return type: **STRING**

Example:

    os> source=people | eval `SUBSTRING('helloworld', 5)` = SUBSTRING('helloworld', 5), `SUBSTRING('helloworld', 5, 3)` = SUBSTRING('helloworld', 5, 3) | fields `SUBSTRING('helloworld', 5)`, `SUBSTRING('helloworld', 5, 3)`
    fetched rows / total rows = 1/1
    +------------------------------+---------------------------------+
    | SUBSTRING('helloworld', 5)   | SUBSTRING('helloworld', 5, 3)   |
    |------------------------------+---------------------------------|
    | oworld                       | owo                             |
    +------------------------------+---------------------------------+


### `TRIM`

**Description**

**Argument type:**
  -  STRING
  - Return type: **STRING**

Example:

    os> source=people | eval `TRIM('   hello')` = TRIM('   hello'), `TRIM('hello   ')` = TRIM('hello   ') | fields `TRIM('   hello')`, `TRIM('hello   ')`
    fetched rows / total rows = 1/1
    +--------------------+--------------------+
    | TRIM('   hello')   | TRIM('hello   ')   |
    |--------------------+--------------------|
    | hello              | hello              |
    +--------------------+--------------------+


### `UPPER`

**Description**

`upper(string)` converts the string to uppercase.

**Argument type:**
 - STRING
 - Return type: **STRING**

Example:

    os> source=people | eval `UPPER('helloworld')` = UPPER('helloworld'), `UPPER('HELLOWORLD')` = UPPER('HELLOWORLD') | fields `UPPER('helloworld')`, `UPPER('HELLOWORLD')`
    fetched rows / total rows = 1/1
    +-----------------------+-----------------------+
    | UPPER('helloworld')   | UPPER('HELLOWORLD')   |
    |-----------------------+-----------------------|
    | HELLOWORLD            | HELLOWORLD            |
    +-----------------------+-----------------------+
