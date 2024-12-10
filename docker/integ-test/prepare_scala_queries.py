#!/usr/bin/env python3

import csv

queries = None
with open('../../integ-test/script/test_cases.csv', 'r') as f:
	reader = csv.DictReader(f)
	queries = [(row['query'], i, row.get('expected_status', None)) for i, row in enumerate(reader, start=1) if row['query'].strip()]

print('try {')
for query in queries:
	query_str = query[0].replace('\n', '').replace('"', '\\"')
	if 'FAILED' == query[2]:
		print('    try {')
		print(f'        spark.sql("{query_str}")')
		print('        throw new Error')
		print('    } catch {')
		print('        case e: Exception => null')
		print('    }\n')
	else:
		print(f'    spark.sql("{query_str}")\n')
print('}')

