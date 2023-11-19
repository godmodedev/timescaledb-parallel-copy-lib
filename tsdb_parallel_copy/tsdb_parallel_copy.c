#include <Python.h>

/* Will come from go */
void parallelCopy(char* fromFile, char* postgresConnect, char* dbName, char* schemaName, char* tableName, int truncate, char* copyOptions, char* splitCharacter, char* quoteCharacter, char* escapeCharacter, char* columns, int skipHeader, int headerLinesCnt, int workers, long int limit, int batchSize, int logBatches, int reportingPeriod, int verbose, long int rowCount);




static PyObject *method_tsdb_parallel_copy(PyObject *self, PyObject *args) {

    char *fromFile, *postgresConnect, *dbName, *schemaName, *tableName, *copyOptions, *splitCharacter, *quoteCharacter, \
        *escapeCharacter, *columns = NULL;
    int *truncate, *skipHeader, *headerLinesCnt, *workers, *batchSize, *logBatches, *reportingPeriod, *verbose = NULL;
    long *limit, *rowCount = NULL;

    if(!PyArg_ParseTuple(args, "ssssspssssspiilipipl", &fromFile, &postgresConnect, &dbName, &schemaName, &tableName, 
    &truncate, &copyOptions, &splitCharacter, &quoteCharacter, &escapeCharacter, &columns, &skipHeader, 
    &headerLinesCnt, &workers, &limit, &batchSize, &logBatches, &reportingPeriod, &verbose, &rowCount)) {
        return NULL;
    }   

    parallelCopy(fromFile, postgresConnect, dbName, schemaName, tableName, 
    *truncate, copyOptions, splitCharacter, quoteCharacter, escapeCharacter, columns, *skipHeader, 
    *headerLinesCnt, *workers, *limit, *batchSize, *logBatches, *reportingPeriod, *verbose, *rowCount);

    Py_RETURN_NONE;
}


static struct PyMethodDef methods[] = {
    {"tsdb_parallel_copy", (PyCFunction)method_tsdb_parallel_copy, METH_VARARGS, 
        "Use timescaledb parallel copy to upload a file to a database.\n"
        "\n"
        "Args:\n"
        "    file: File to read from\n"
        "    url: PostgreSQL connection URL\n"
        "    db_name: Destination database name\n"
        "    table_name: Destination table name\n"
        "    schema: Destination table schema. Defaults to \"public\".\n"
        "    truncate: Truncate the destination table before insert. Defaults to False.\n"
        "    copy_options: Additional options to pass to COPY (e.g., NULL 'NULL'). Defaults to \"CSV\".\n"
        "    split: Character to split by. Defaults to \",\".\n"
        "    quote: The QUOTE `character` to use during COPY (default '\"'). Defaults to \"\".\n"
        "    escape: The ESCAPE `character` to use during COPY (default '\"'). Defaults to \"\".\n"
        "    columns: Comma-separated columns present in CSV. Defaults to "".\n"
        "    skip_header: Skip the first line of the input. Defaults to False.\n"
        "    header_line_count: Number of header lines. Defaults to 1.\n"
        "    batch_size: Number of rows per insert. Defaults to 5000.\n"
        "    limit: Number of rows to insert overall; 0 means to insert all. Defaults to 0.\n"
        "    workers: Number of parallel requests to make. Defaults to 1.\n"
        "    log_batches: Whether to time individual batches. Defaults to False.\n"
        "    reporting_period: Period (in seconds) to report insert stats; if 0s, intermediate results will not be reporte. Defaults to 0.\n"
        "    verbose: Print more information about copying statistics. Defaults to False.\n"
    },
    {NULL, NULL}
};

static struct PyModuleDef module = {
    PyModuleDef_HEAD_INIT,
    "tsdb_parallel_copy",
    NULL,
    -1,
    methods
};

PyMODINIT_FUNC PyInit_tsdb_parallel_copy(void) {
    return PyModule_Create(&module);
}