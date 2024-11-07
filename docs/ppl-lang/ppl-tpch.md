## TPC-H Benchmark

TPC-H is a decision support benchmark designed to evaluate the performance of database systems in handling complex business-oriented queries and concurrent data modifications. The benchmark utilizes a dataset that is broadly representative of various industries, making it widely applicable. TPC-H simulates a decision support environment where large volumes of data are analyzed, intricate queries are executed, and critical business questions are answered.

### Test PPL Queries

TPC-H 22 test query statements: [TPCH-Query-PPL](https://github.com/opensearch-project/opensearch-spark/blob/main/integ-test/src/integration/resources/tpch)

### Data Preparation

#### Option 1 - from PyPi

```
# Create the virtual environment
python3 -m venv .venv

# Activate the virtual environment
. .venv/bin/activate

pip install tpch-datagen
```

#### Option 2 - from source

```
git clone https://github.com/gizmodata/tpch-datagen

cd tpch-datagen

# Create the virtual environment
python3 -m venv .venv

# Activate the virtual environment
. .venv/bin/activate

# Upgrade pip, setuptools, and wheel
pip install --upgrade pip setuptools wheel

# Install TPC-H Datagen - in editable mode with client and dev dependencies
pip install --editable .[dev]
```

#### Usage

Here are the options for the tpch-datagen command:
```
tpch-datagen --help
Usage: tpch-datagen [OPTIONS]

Options:
  --version / --no-version        Prints the TPC-H Datagen package version and
                                  exits.  [required]
  --scale-factor INTEGER          The TPC-H Scale Factor to use for data
                                  generation.
  --data-directory TEXT           The target output data directory to put the
                                  files into  [default: data; required]
  --work-directory TEXT           The work directory to use for data
                                  generation.  [default: /tmp; required]
  --overwrite / --no-overwrite    Can we overwrite the target directory if it
                                  already exists...  [default: no-overwrite;
                                  required]
  --num-chunks INTEGER            The number of chunks that will be generated
                                  - more chunks equals smaller memory
                                  requirements, but more files generated.
                                  [default: 10; required]
  --num-processes INTEGER         The maximum number of processes for the
                                  multi-processing pool to use for data
                                  generation.  [default: 10; required]
  --duckdb-threads INTEGER        The number of DuckDB threads to use for data
                                  generation (within each job process).
                                  [default: 1; required]
  --per-thread-output / --no-per-thread-output
                                  Controls whether to write the output to a
                                  single file or multiple files (for each
                                  process).  [default: per-thread-output;
                                  required]
  --compression-method [none|snappy|gzip|zstd]
                                  The compression method to use for the
                                  parquet files generated.  [default: zstd;
                                  required]
  --file-size-bytes TEXT          The target file size for the parquet files
                                  generated.  [default: 100m; required]
  --help                          Show this message and exit.
```

### Generate 1 GB data with zstd (by default) compression

```
tpch-datagen --scale-factor 1
```

### Generate 10 GB data with snappy compression

```
tpch-datagen --scale-factor 10 --compression-method snappy
```

### Query Test

All TPC-H PPL Queries located in `integ-test/src/integration/resources/tpch` folder.

To test all queries, run `org.opensearch.flint.spark.ppl.tpch.TPCHQueryITSuite`.