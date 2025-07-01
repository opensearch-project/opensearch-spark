# Spark SQL Testing Server

A lightweight web server for testing Spark SQL queries and extensions.
Provides a simple REST API to load data, execute queries, and manage tables in a local Spark cluster.

## Getting Started

1. Start the server and Spark cluster:
```bash
docker-compose up
```

The server will be available at `http://localhost:5000`.

## API Endpoints

### Load Data

Load data into a Spark SQL table:

```bash
$ curl -X POST http://localhost:5000/load \
  -H "Content-Type: application/json" \
  -d '{
    "table": "employees",
    "data": [
      {"id": 1, "name": "Alice", "department": "IT", "salary": 80000},
      {"id": 2, "name": "Bob", "department": "HR", "salary": 65000},
      {"id": 3, "name": "Charlie", "department": "IT", "salary": 75000}
    ]
  }'
{
  "status": "success"
}
```

### Execute Query

Run any Spark SQL query:

```bash
$ curl -X POST http://localhost:5000/query \
  -H "Content-Type: application/json" \
  -d '{
    "query": "SELECT department, AVG(salary) as avg_salary FROM employees GROUP BY department"
  }'
{
  "result": [
    {
      "avg_salary": 77500,
      "department": "IT"
    },
    {
      "avg_salary": 65000,
      "department": "HR"
    }
  ],
  "schema": {
    "avg_salary": "double",
    "department": "string"
  },
  "status": "success"
}
```

This supports our SQL extensions, including PPL:

```bash
$ curl -X POST http://localhost:5000/query \
  -H "Content-Type: application/json" \
  -d '{
    "query": "source = employees | stats avg(salary) as avg_salary by department"
  }'
{
  "result": [
    {
      "avg_salary": 77500,
      "department": "IT"
    },
    {
      "avg_salary": 65000,
      "department": "HR"
    }
  ],
  "schema": {
    "avg_salary": "double",
    "department": "string"
  },
  "status": "success"
}
```

### Drop Table

Remove a table when done:

```bash
$ curl -X POST http://localhost:5000/drop \
  -H "Content-Type: application/json" \
  -d '{
    "table": "employees"
  }'
{
  "status": "success"
}
```
