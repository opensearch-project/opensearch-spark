## PPL `appendcol` command

### Description
Using `appendcol` command to append the result of a sub-search and attach it alongside with the input search results (The main search).

### Syntax - APPENDCOL
`APPENDCOL <override=?> [sub-search]...`

* <override=?>: optional boolean field to specify should result from main-result be overwritten in the case of column name conflict.
* sub-search: Executes PPL commands as a secondary search. The sub-search uses the same data specified in the source clause of the main search results as its input.


#### Example 1: To append the result of `stats avg(age) as AVG_AGE` into existing search result   

The example append the result of sub-search `stats avg(age) as AVG_AGE` alongside with the main-search.

PPL query:

    os> source=employees | FIELDS name, dept, age | APPENDCOL [ stats avg(age) as AVG_AGE ];
    fetched rows / total rows = 9/9
    +------+-------------+-----+------------------+  
    | name | dept        | age | AVG_AGE          |  
    +------+-------------+-----+------------------+  
    | Lisa | Sales       |  35 | 31.2222222222222 |  
    | Fred | Engineering |  28 | NULL             |  
    | Paul | Engineering |  23 | NULL             |  
    | Evan | Sales       |  38 | NULL             |   
    | Chloe| Engineering |  25 | NULL             |   
    | Tom  | Engineering |  33 | NULL             |   
    | Alex | Sales       |  33 | NULL             |  
    | Jane | Marketing   |  28 | NULL             |  
    | Jeff | Marketing   |  38 | NULL             |  
    +------+-------------+-----+------------------+  


#### Example 2: To compare multiple stats commands with side by side with appendCol.

This example demonstrates a common use case: performing multiple statistical calculations and displaying the results side by side in a horizontal layout.

PPL query:

    os> source=employees | stats avg(age) as avg_age1 by dept | fields dept, avg_age1 | APPENDCOL  [ stats avg(age) as avg_age2 by dept | fields avg_age2 ];
    fetched rows / total rows = 3/3
    +-------------+-----------+----------+
    | dept        | avg_age1  | avg_age2 |
    +-------------+-----------+----------+
    | Engineering | 27.25     |  27.25   |
    | Sales       | 35.33     |  35.33   |
    | Marketing   | 33.00     |  33.00   |
    +-------------+-----------+----------+


#### Example 3: Append multiple sub-search result

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

#### Example 4: Over main-search in the case of column name conflict

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

#### Example 5: AppendCol command with duplicated columns

The example demonstrate what could happen when conflicted columns exist, with `override` set to false or absent.
In this particular case, average aggregation is being performed over column `age` with group-by `dept`, on main and sub query respectively.
As the result, `dept` and `avg_age1` will be returned by the main query, with `avg_age2` and `dept` for the sub-query,
and take into consideration `override` is absent, duplicated columns won't be dropped, hence all four columns will be displayed as the final result.

PPL query:

    os> source=employees | stats avg(age) as avg_age1 by dept | APPENDCOL  [ stats avg(age) as avg_age2 by dept ];
    fetched rows / total rows = 3/3
    +------------+--------------+------------+--------------+
    | Avg Age 1  | Dept         | Avg Age 2  | Dept         |
    +------------+--------------+------------+--------------+
    |   35.33    | Sales        |   35.33    | Sales        |
    |   27.25    | Engineering  |   27.25    | Engineering  |
    |   33.00    | Marketing    |   33.00    | Marketing    |
    +------------+--------------+------------+--------------+


### Limitation: 
When override is set to true, only `FIELDS` and `STATS` commands are allowed as the final clause in a sub-search. 
Otherwise, an IllegalStateException with the message `Not Supported operation: APPENDCOL should specify the output fields` will be thrown.
