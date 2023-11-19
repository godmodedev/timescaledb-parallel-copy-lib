#include <Python.h>
#include <string.h>

/* Will come from go */
void parallelCopy(char* fromFile, char* postgresConnect, char* dbName, char* schemaName, char* tableName, int truncate, char* copyOptions, char* splitCharacter, char* quoteCharacter, char* escapeCharacter, char* columns, int skipHeader, int headerLinesCnt, int workers, long int limit, int batchSize, int logBatches, int reportingPeriod, int verbose);

void splitPath(char **a, char **b) {
    if (*a == NULL) {
        char *lastSlash = strrchr(*b, '/');
        if (lastSlash != NULL) {
            *lastSlash = '\0';  // Null-terminate at the last slash
            *a = lastSlash + 1; // Set *a to the last part
            *b = *b;           // Set *b to the first part
        }
    }
}

static PyObject *method_tsdb_parallel_copy(PyObject *self, PyObject *args, PyObject *kwargs) {
    char *fromFile, *postgresConnect, *tableName = NULL;
    char *dbName = "";
    char *schemaName = "public";
    char *copyOptions = "CSV";
    char *splitCharacter = ",";
    char *quoteCharacter = "";
    char *escapeCharacter = "";
    char *columns = "";

    int truncate = 0;
    int skipHeader = 0;
    int headerLinesCnt = 1;
    int workers = 1; 
    int batchSize = 5000;
    int logBatches = 0;
    int reportingPeriod = 0; 
    int verbose = 0;
    long limit = 0;

    static char *kwlist[] = {
        "file", "url", "table_name", "db_name", "schema", "truncate", "copy_options", "split", "quote",
        "escape", "columns", "skip_header", "header_line_count", "workers", "limit", "batch_size", "log_batches",
        "reporting_period", "verbose", NULL
    };

    if(!PyArg_ParseTupleAndKeywords(args, kwargs, "sss|sspssssspiilipipl", kwlist, &fromFile, &postgresConnect, &tableName, &dbName, 
        &schemaName, &truncate, &copyOptions, &splitCharacter, &quoteCharacter, &escapeCharacter, &columns, &skipHeader, 
        &headerLinesCnt, &workers, &limit, &batchSize, &logBatches, &reportingPeriod, &verbose)) 
    {
        return NULL;
    }
    splitPath(&dbName, &postgresConnect);

    parallelCopy(fromFile, postgresConnect, dbName, schemaName, tableName, 
    truncate, copyOptions, splitCharacter, quoteCharacter, escapeCharacter, columns, skipHeader, 
    headerLinesCnt, workers, limit, batchSize, logBatches, reportingPeriod, verbose);

    Py_RETURN_NONE;
}


static struct PyMethodDef methods[] = {
    {"tsdb_parallel_copy", (PyCFunctionWithKeywords)method_tsdb_parallel_copy, METH_VARARGS | METH_KEYWORDS, 
        "Use timescaledb parallel copy to upload a file to a database.\n"
        "\n"
        "Args:\n"
        "    file: File to read from\n"
        "    url: PostgreSQL connection URL\n"
        "    table_name: Destination table name\n"
        "    db_name: Destination database name. If unset, inferred from the URL.\n"
        "    schema: Destination table schema. Defaults to \"public\".\n"
        "    truncate: Truncate the destination table before insert. Defaults to False.\n"
        "    copy_options: Additional options to pass to COPY (e.g., NULL 'NULL'). Defaults to \"CSV\".\n"
        "    split: Character to split by. Defaults to \",\".\n"
        "    quote: The QUOTE `character` to use during COPY (default '\"').\n"
        "    escape: The ESCAPE `character` to use during COPY (default '\"').\n"
        "    columns: Comma-separated columns present in CSV. Defaults to "".\n"
        "    skip_header: Skip the first line of the input. Defaults to False.\n"
        "    header_line_count: Number of header lines. Defaults to 1.\n"
        "    workers: Number of parallel requests to make. Defaults to 1.\n"
        "    limit: Number of rows to insert overall; 0 means to insert all. Defaults to 0.\n"
        "    batch_size: Number of rows per insert. Defaults to 5000.\n"
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