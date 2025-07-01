#!/usr/bin/env bash

echo "Loading sample data..."
curl -X POST http://localhost:5000/load \
  -H "Content-Type: application/json" \
  -d '{
    "table": "employees",
    "data": [
      {"id": 1, "name": "Alice", "department": "IT", "salary": 80000},
      {"id": 2, "name": "Bob", "department": "HR", "salary": 65000},
      {"id": 3, "name": "Charlie", "department": "IT", "salary": 75000}
    ]
  }' \
  --silent | jq

echo -e "\nRunning SQL select query..."
curl -X POST http://localhost:5000/query \
  -H "Content-Type: application/json" \
  -d '{"query": "SELECT * FROM employees"}' \
  --silent | jq

echo -e "\nRunning SQL aggregation query..."
curl -X POST http://localhost:5000/query \
  -H "Content-Type: application/json" \
  -d '{"query": "SELECT department, AVG(salary) as avg_salary FROM employees GROUP BY department"}' \
  --silent | jq

echo -e "\nRunning PPL filter query..."
curl -X POST http://localhost:5000/query \
  -H "Content-Type: application/json" \
  -d '{"query": "source = employees | where salary > 70000"}' \
  --silent | jq

echo -e "\nDropping sample data..."
curl -X post http://localhost:5000/drop \
  -H "Content-Type: application/json" \
  -d '{"table": "employees"}' \
  --silent | jq
