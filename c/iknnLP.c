#include "postgres.h"
#include "fmgr.h"
#include "utils/array.h"
#include "catalog/pg_type.h"
#include "executor/spi.h"
#include "utils/builtins.h"

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(iknnLP);

void string2int(char *number, int *k);
void chopQueryFieldNames(char *fieldNames, char **qFnames);
void chopQueryValues(char *queryValues, int *qValues, int nQueryFields);
void digestQuery(char *iknnQuery, char *tbl, char **qFnames, int *qValues, int *k, int nQueryFields);

void string2int(char *number, int *k) {
    int i;
    *k = 0;
    for (i = 0; i < strlen(number); i++) {
        if (number[i] >= '0' && number[i] < '9') {
            *k = *k * 10 + number[i] - '0';
        }
        else {
            // error handling
        }
    }
}

void chopQueryFieldNames(char *fieldNames, char **qFnames) {
    int i, j;

    i = 0;
    while ((fieldNames != NULL) && (strlen(fieldNames) > 0)) {
        for (j = 0; j < strlen(fieldNames); j++) {
            if (fieldNames[j] == ',')
                break;
            qFnames[i][j] = fieldNames[j];
        }
        qFnames[i++][j] = '\0';
        if (j < strlen(fieldNames) && fieldNames[j] == ',')
            strcpy(fieldNames, fieldNames + j + 1);
        else
            break;
    }
}

void chopQueryValues(char *queryValues, int *qValues, int nQueryFields) {
    int nQV = 0;    // number of query values
    int i, j;
    char *number = (char*)palloc0(sizeof(char) * 256);

    for (i = 0; i < strlen(queryValues); i++)
        if (queryValues[i] == ',')
            nQV++;
    i = 0;
    while ((queryValues != NULL) && (strlen(queryValues) > 0)) {
        memset(number, 0, 256);
        for (j = 0; j < strlen(queryValues); j++) {
            if (queryValues[j] == ',')
                break;
            number[j] = queryValues[j];
        }

        number[j] = '\0';
        string2int(number, qValues + (i++));
        if (j < strlen(queryValues) && queryValues[j] == ',')
            strcpy(queryValues, queryValues + j + 1);
        else
            break;
    }
}

void digestQuery(char *iknnQuery, char *tbl, char **qFnames, int *qValues, int *k, int nQueryFields) {
//    find 3 nearest neighbour of (a, b, c)(1, 2, 3) from hash
    char *number;
    char *tmpQuery;
    char *fieldNames;
    char *queryValues;
    tmpQuery = (char*)palloc0(strlen(iknnQuery) * sizeof(char));

    // get k
    strcpy(tmpQuery, strchr(iknnQuery, ' ') + 1);
    while (tmpQuery[0] == ' ')
        strcpy(tmpQuery, tmpQuery + 1);
    number = (char*)palloc0((strchr(tmpQuery, ' ') - tmpQuery + 1) * sizeof(char));
    strncpy(number, tmpQuery, strchr(tmpQuery, ' ') - tmpQuery);
    string2int(number, k);
    
    // get query field names
    strcpy(tmpQuery, strchr(tmpQuery, '(') + 1);
    fieldNames = (char*)palloc0((strchr(tmpQuery, ')') - tmpQuery + 1) * sizeof(char));
    strncpy(fieldNames, tmpQuery, strchr(tmpQuery, ')') - tmpQuery);
    chopQueryFieldNames(fieldNames, qFnames);

    // get query values
    strcpy(tmpQuery, strchr(tmpQuery, '(') + 1);
    queryValues = (char*)palloc0((strchr(tmpQuery, ')') - tmpQuery + 1) * sizeof(char));
    strncpy(queryValues, tmpQuery, strchr(tmpQuery, ')') - tmpQuery);
    chopQueryValues(queryValues, qValues, nQueryFields);

    // get table name
    strcpy(tmpQuery, strstr(tmpQuery, "from ") + 5);
    while (tmpQuery[0] == ' ')
        strcpy(tmpQuery, tmpQuery + 1);
    while (tmpQuery[strlen(tmpQuery) - 1] == ' ')
        strncpy(tmpQuery, tmpQuery, strlen(tmpQuery) - 1);
    tmpQuery[strlen(tmpQuery)] = '\0';
    strcpy(tbl, tmpQuery);
}

Datum
iknnLP(PG_FUNCTION_ARGS) {
    char *iknnQuery;
    char *tbl;
    char **qFnames;
    int *qValues;
    int nQueryFields = 0;
    int k, i;
    int ret;
    int proc = 0;

    /* get arguments, convert command to C string */
    iknnQuery = text_to_cstring(PG_GETARG_TEXT_P(0));
    for (i = strchr(iknnQuery, '(') - iknnQuery + 1; i < strchr(iknnQuery, ')') - iknnQuery - 1; i++)
        if (iknnQuery[i] == ',')
            nQueryFields++;
    qFnames = (char**)palloc(sizeof(char*) * (++nQueryFields));
    qValues = (int*)palloc(sizeof(int) * nQueryFields);
    for (i = 0; i < nQueryFields; i++)
        qFnames[i] = (char*)palloc(sizeof(char) * 256);
    tbl = (char*)palloc(sizeof(char) * 256);
    digestQuery(iknnQuery, tbl, qFnames, qValues, &k, nQueryFields);

    /* open internal connection */
    SPI_connect();
    /* run the SQL command */
    ret = SPI_exec("select column_name from information_schema.columns where table_name = 'hash'", 4294967296); /* save the number of rows */
    proc = SPI_processed;
    /* If some rows were fetched, print them via elog(INFO). */ 
    if (ret > 0 && SPI_tuptable != NULL) {
        TupleDesc tupdesc = SPI_tuptable->tupdesc; SPITupleTable *tuptable = SPI_tuptable; char buf[8192];
        int i, j;
        for (j = 0; j < proc; j++) {
            HeapTuple tuple = tuptable->vals[j];
            // construct a string representing the tuple
            for (i = 1, buf[0] = 0; i <= tupdesc->natts; i++)
                snprintf(buf + strlen (buf), sizeof(buf) - strlen(buf), "%s(%s::%s)%s", SPI_fname(tupdesc, i), SPI_getvalue(tuple, tupdesc, i), SPI_gettype(tupdesc, i),(i == tupdesc->natts) ? " " : " |");
            ereport(INFO, (errmsg("ROW: %s", buf)));
        }
    }
    SPI_finish(); //pfree(command);
    PG_RETURN_INT32(proc);
}
