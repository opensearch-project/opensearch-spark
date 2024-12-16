## PPL `appendcol` command

### Description
Using `appendcol` command append the result of a sub-search and attach it alongside with the input search results (The main search).

### Syntax - APPENDCOL
`APPENDCOL <override=?> [sub-search]...`

* <override=?>: optional boolean field to specify should result from main-result be overwritten in the case of column name conflict.
* sub-search: The PPL commands to execute for the secondary search.  


#### Example 1: To append the result of `stats ave(age) as AVG_AGE` into existing search result   

The example append the result of sub-search `stats ave(age) as AVG_AGE` alongside with the main-search.

PPL query:

    os> source=employees | FIELDS name, dept, age | APPENDCOL [ stats avg(age) as AVG_AGE ];
    fetched rows / total rows = 9/9
    +------+-------------+-----+------------------+  
    | name | dept        | age | AVG_AGE          |  
    +------+-------------+-----+------------------+  
    | Lisa | Sales------ |  35 | 31.2222222222222|  
    | Fred | Engineering |  28 | NULL            |  
    | Paul | Engineering |  23 | NULL            |  
    | Evan | Sales------ |  38 | NULL            |   
    | Chloe| Engineering |  25 | NULL            |   
    | Tom  | Engineering |  33 | NULL            |   
    | Alex | Sales       |  33 | NULL            |  
    | Jane | Marketing   |  28 | NULL            |  
    | Jeff | Marketing   |  38 | NULL            |  
    +------+-------------+-----+------------------+  
    

#### Example 2: Append multiple sub-search result

The example demonstrate multiple APPENCOL commands can be chained to provide one comprehensive view for user. 

PPL query:

    os> source=employees | FIELDS name, dept, age | APPENDCOL [ stats avg(age) as AVG_AGE ] | APPENDCOL [ stats max(age) as MAX_AGE ];
    fetched rows / total rows = 9/9
    +------+-------------+-----+------------------+---------+  
    | name | dept        | age | AVG_AGE          | MAX_AGE |  
    +------+-------------+-----+------------------+---------+  
    | Lisa | Sales------ |  35 | 31.22222222222222|      38 |  
    | Fred | Engineering |  28 | NULL             |    NULL |  
    | Paul | Engineering |  23 | NULL             |    NULL |  
    | Evan | Sales------ |  38 | NULL             |    NULL |  
    | Chloe| Engineering |  25 | NULL             |    NULL |  
    | Tom  | Engineering |  33 | NULL             |    NULL |  
    | Alex | Sales       |  33 | NULL             |    NULL |  
    | Jane | Marketing   |  28 | NULL             |    NULL |  
    | Jeff | Marketing   |  38 | NULL             |    NULL |  
    +------+-------------+-----+------------------+---------+  

#### Example 3: Over main-search in the case of column name conflict

The example demonstrate the usage of `OVERRIDE` option to overwrite the `age` column from the main-search, 
when the option is set to true and column with same name `age` present on sub-search.

PPL query:

    os> source=employees | FIELDS name, dept, age | APPENDCOL OVERRIDE=true [ stats avg(age) as age ];
    fetched rows / total rows = 9/9
    +------+-------------+------------------+  
    | name | dept        | age              |  
    +------+-------------+------------------+  
    | Lisa | Sales------ | 31.22222222222222|  
    | Fred | Engineering | NULL             |  
    | Paul | Engineering | NULL             |  
    | Evan | Sales------ | NULL             |  
    | Chloe| Engineering | NULL             |  
    | Tom  | Engineering | NULL             |  
    | Alex | Sales       | NULL             |  
    | Jane | Marketing   | NULL             |  
    | Jeff | Marketing   | NULL             |
    +------+-------------+------------------+
