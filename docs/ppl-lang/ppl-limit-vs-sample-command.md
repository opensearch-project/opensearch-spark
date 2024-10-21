### `head` Vs `TABLESAMPLE`
The primary difference between `head` and `TABLESAMPLE` in PPL lies in how they operate on the data:

1. `head`:

   **Purpose**: head is used to return a specified number of rows from the result set, after all other operations (like filtering, aggregations, etc.) have been applied.

   **Behavior**: It retrieves the first N rows from the query result in a deterministic manner, based on the ordering of the data (or lack thereof). If there is no explicit `sort` clause, the result may be somewhat arbitrary but repeatable.
   
   **Execution**: head is applied after any `where`, `stats`, or `sort` clauses. It restricts the number of rows after processing the entire dataset.
   
   **Example**:
    ```sql
       source = t | head 10
    ```
   This will return exactly 10 rows from the account table.

2. `TABLESAMPLE`:

   **Purpose**: `TABLESAMPLE` is used to retrieve a random subset of rows from a table. It selects a percentage of the rows from the table in a non-deterministic, probabilistic manner.
  
   **Behavior**: The rows returned by `TABLESAMPLE` are randomly selected based on a given percentage or fraction of the table. It is not guaranteed that the same number of rows will be returned each time the query is run.
   
   **Execution**: `TABLESAMPLE` is applied directly to the underlying table before any other operations like filtering or aggregation. _**It reduces the dataset size early in the query execution._**
   
   **Example**:
    ```sql
        source = t TABLESAMPLE(50 PERCENT)
    ```
   This will randomly select approximately 50% of the rows from the account table.
   
3. Key Differences:
   Practical Implications:

| Feature                   | `head`                                                      | `TABLESAMPLE`                                      |
|---------------------------|-------------------------------------------------------------|----------------------------------------------------|
| **Behavior**              | Returns a fixed number of rows                              | Returns a random subset of rows                    |
| **Determinism**           | Deterministic (returns same rows if no ordering is changed) | Non-deterministic (random rows each time)          |
| **Scope**                 | Applied after all operations                                | Applied directly to the table                      |
| **Use case**              | Retrieve a specific number of rows                          | Work with a random sample of data                  |
| **Impact on performance** | Still processes the full dataset and then heads             | Can reduce the amount of data processed earlier    |
| **Percentage/Fraction**   | Not supported, only absolute numbers                        | Supports percentage-based sampling                 |

- Use `head` when you need an exact number of rows, especially in a final result set .
- Use `TABLESAMPLE` when you need a rough approximation of the data or want to randomly sample data without processing the entire table, which can help improve query performance when working with large datasets.





