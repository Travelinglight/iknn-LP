#include "postgres.h"
#include "fmgr.h"
#include "utils/array.h"
#include "catalog/pg_type.h"
#include "executor/spi.h"
#include "utils/builtins.h"

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(add_ab);

void string2int(char *number, int *k) {
    int i;
    *k = 0;
    for (i = 0; i < strlen(number); i++) {
        if (number[i] >= '0' && number[i] < '9') {
            k = k * 10 + number[i] - '0';
        }
        else {
            // error handling
        }
    }
}

void chopQueryFieldNames(char *fieldNames, char **qFnames) {
    int nFN = 0;    // number of field names;
    int i, j;

    for (i = 0; i < strlen(fieldNames); i++)
        if (fieldNames[i] == ',')
            nFN++;
    qFnames = (char**)palloc(sizeof(char*) * (++nFN));
    for (i = 0; i < nFN; i++)
        qFnames[i] = (char*)palloc(sizeof(char) * 256);
    i = 0;
    while ((fieldNames != NULL) && (strlen(fieldNames) > 0)) {
        for (j = 0; j < strlen(fieldNames); j++) {
            if (fieldNames[j] == ',') {
                strcpy(fieldNames, fieldNames + j + 1);
                qFnames[i][j] = '\0';
                continue;
            }
            qFnames[i][j] = fieldNames[j];
        }
        qFnames[i][j] = '\0';
        if (j >= strlen(fidleNames))
            break;
    }
}

void chopQueryValues(char *queryValues, int *qValues) {
    int nQV = 0;    // number of query values
    int i, j;
    char *number = (char*)palloc(sizeof(char) * 256);

    for (i = 0; i < strlen(queryValues; i++))
        if (queryValues[i] == ',')
            nQV++;
    qValues = (int*)palloc(sizeof(int) * (++nQV));
    i = 0;
    while ((queryValues != NULL) && (strlen(queryValues) > 0)) {
        *number = "";
        for (j = 0; j < strlen(queryValues); j++) {
            if (queryValues[j] == ',') {
                strcpy(queryValues, queryValues + j + 1);
                number[j] = '\0';
                string2int(number, qValues + i);
                continue;
            }
            number[j] = queryValues[j];
        }
        number[j] = '\0';
        string2int(number, qValues + i);
        if (j >= strlen(fidleNames))
            break;
    }
}

void digestQuery(char *iknnQuery, char *tbl, char **qFnames, int *qValues, int *k) {
//    find 3 nearest neighbour of (a, b, c)(1, 2, 3) from hash
    char *number;;
    char *tmpQuery = (char*)palloc(strlen(iknnQuery) * sizeof(char));
    char *fieldNames;
    char *queryValues;

    // get k
    strcpy(tmpQuery, strchr(iknnQuery, ' ') + 1);
    while (tmpQuery[0] == ' ')
        strcpy(tmpQuery, tmpQuery + 1);
    number = (char*)palloc((strchr(tmpQuery, ' ') - tmpQuery + 1) * sizeof(char));
    strncpy(number, tmpQuery, strchr(tmpquery, ' ') - tmpQuery);
    string2int(number, k);
    
    // get query field names
    strcpy(tmpQuery, strchr(tmpquery, '(') + 1);
    fieldNames = (char*)palloc((strchr(tmpQuery, ')') - tmpQuery + 1) * sizeof(char));
    strncpy(fieldNames, tmpQuery, strchr(tmpQuery, ')') - tmpQuery);
    chopQueryFieldNames(fieldNames, qFnames);

    // get query values
    strcpy(tmpQuery, strchr(tmpQuery, '(') + 1);
    queryValues = (char*)palloc((strchr(tmpQuery, ')') - tmpQuery + 1) * sizeof(char));
    strncpy(queryValues, tmpQuery, strchr(tmpQuery, ')') - tmpQuery);
    chopQueryValues(queryValues, qValues);

    // get table name
    strcpy(tmpQuery, strstr(tmpQuery, "from ") + 5);
    while (tmpQuery[0] == ' ')
        strcpy(tmpQuery, tmpQuery + 1);
    while (tmpQuery[strlen(tmpQuery) - 1] == ' ')
        strncpy(tmpQuery, tmpQuery, strlen(tmpQuery) - 1);
    tmpQuery[strlen(tmpQuery)] = '\0';
    tbl = (char*)palloc(sizeof(char) * strlen(tmpQuery) + 1);
    *tbl = tmpQuery;
}

Datum
iknnLP(PG_FUNCTION_ARGS) {
    char *iknnQuery;
    char *tbl;
    char **qFnames;
    int *qValues;
    int k;
    int ret;
    int proc;

    /* get arguments, convert command to C string */
    iknnQuery = text_to_cstring(PG_GETARG_TEXT_P(0));
    digestQuery(iknnQuery, tbl, qFnames, qValues, &k);

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
                snprintf(buf + strlen (buf),
                    sizeof(buf) - strlen(buf), "%s(%s::%s)%s", SPI_fname(tupdesc, i), SPI_getvalue(tuple, tupdesc, i), SPI_gettype(tupdesc, i),(i == tupdesc->natts) ? " " : " |");
                    ereport(INFO, (errmsg("ROW: %s", buf)));
        }
    }
    SPI_finish(); //pfree(command);
    PG_RETURN_INT32(proc);
}
