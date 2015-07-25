#include "postgres.h"
#include "fmgr.h"
#include "utils/array.h"
#include "catalog/pg_type.h"
#include "executor/spi.h"
#include "utils/builtins.h"

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(iknnLP);

typedef struct {
    char *fieldName;
    int value;
    char complete;
}queryObj;

typedef struct {
    HeapTuple tuple;
    double dist;
}HeapRec;

typedef struct {
    HeapRec *rec;
    long size;
    long length;
}Heap;

double fabs(double n);
double string2double(char *number);
void string2int(char *number, int *k);
void chopQueryFieldNames(char *fieldNames, char **qFnames);
void chopQueryValues(char *queryValues, int *qValues, int nQueryFields);
void digestQuery(char *iknnQuery, char *tbl, char **qFnames, int *qValues, int *k, int nQueryFields);
void heapInsert(Heap *heap, HeapRec newRec);
void heapCover(Heap *heap, long node, HeapRec newRec);
int binarySearch(TupleDesc *tupdescObj, SPITupleTable *tuptableObj, long procObj, double qAlpha, int dim);

double fabs(double n) {
    return n < 0 ? -n : n;
}

double string2double(char *number) {
    int i, flag = 1;    // flag=1 before point, flag=0 after point
    long div10 = 1, sign = 1;
    double k = 0;
    for (i = 0; i < strlen(number); i++) {
        if (i == 0 && number[i] == '-') {
            sign = -1;
        }
        else if (number[i] == '.') {
            flag = 0;
            div10 *= 10;
        }
        else if (number[i] >= '0' && number[i] <= '9') {
            k = k * (flag > 0 ? 10 : 1) + (float)(number[i] - '0') / (flag > 0 ? 1 : div10);
            if (flag == 0)
                div10 *= 10;
        }
        else {
            // error handling
        }
    }
    return k * sign;
}

void string2int(char *number, int *k) {
    int i, sign = 1;
    *k = 0;
    for (i = 0; i < strlen(number); i++) {
        if (i == 0 && number[i] == '-') {
            sign = -1;
        }
        else if (number[i] >= '0' && number[i] < '9') {
            *k = *k * 10 + number[i] - '0';
        }
        else {
            // error handling
        }
    }
    *k = *k * sign;
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

void heapInsert(Heap *heap, HeapRec newRec) {
    long p, f;
    HeapRec tmpRec;
    heap->rec[++heap->length] = newRec;
    p = heap->length;
    while ((p >> 1) > 0) {
        f = p >> 1;
        if (heap->rec[f].dist < heap->rec[p].dist) {
            tmpRec = heap->rec[p];
            heap->rec[p] = heap->rec[f];
            heap->rec[f] = tmpRec;
        }
        p = f;
    }
}

void heapCover(Heap *heap, long node, HeapRec newRec) {
    long p = 1, s;
    HeapRec tmpRec;
    heap->rec[1] = newRec;
    while ((p << 1) <= heap->length) {
        s = p << 1;
        if ((s < heap->length) && (heap->rec[s].dist < heap->rec[s+1].dist))
            s++;
        if (heap->rec[p].dist < heap->rec[s].dist) {
            tmpRec = heap->rec[p];
            heap->rec[p] = heap->rec[s];
            heap->rec[s] = tmpRec;
        }
        p = s;
    }
}

int binarySearch(TupleDesc *tupdescObj, SPITupleTable *tuptableObj, long procObj, double qAlpha, int dim) {
    long l = 0, r = procObj - 1;
    ereport(INFO, (errmsg("qAlpha: %lf", qAlpha)));
    while (l <= r) {
        long mid = (l + r) >> 1;
        ereport(INFO, (errmsg("l: %ld, r: %ld, mid: %ld, midvalue: %lf", l, r, mid, string2double(SPI_getvalue(tuptableObj->vals[mid], *tupdescObj, dim + 2)))));
        if (string2double(SPI_getvalue(tuptableObj->vals[mid], *tupdescObj, dim + 2)) > qAlpha) {
            r = mid - 1;
            continue;
        }
        else if (string2double(SPI_getvalue(tuptableObj->vals[mid], *tupdescObj, dim + 2)) < qAlpha) {
            l = mid + 1;
            continue;
        }
        else
            return mid;
    }
    ereport(INFO, (errmsg("l: %ld, r: %ld", l, r)));
    if (r == -1)
        return 0;
    if (l == procObj)
        return procObj - 1;
    while ((l < procObj - 1) && (fabs(string2double(SPI_getvalue(tuptableObj->vals[l], *tupdescObj, dim + 2)) - qAlpha) > fabs(string2double(SPI_getvalue(tuptableObj->vals[l + 1], *tupdescObj, dim + 2)) - qAlpha)))
        l++;
    while ((l > 0) && (fabs(string2double(SPI_getvalue(tuptableObj->vals[l], *tupdescObj, dim + 2)) - qAlpha) > fabs(string2double(SPI_getvalue(tuptableObj->vals[l - 1], *tupdescObj, dim + 2)) - qAlpha)))
        l--;
    return l;
}

Datum
iknnLP(PG_FUNCTION_ARGS) {
    char *iknnQuery;
    char *tbl;
    char getBuckets[1024], getFields[1024], getObjects[1024];
    char **qFnames;
    int *qValues;
    int nQueryFields = 0;
    int k, i, j, dim;  // dim: dimention
    int ret;
    long procBkt, procFld;
    double qAlpha = 0;
    queryObj *qObj;
    Heap res;

    /* get arguments, convert command to C string */
    iknnQuery = text_to_cstring(PG_GETARG_TEXT_P(0));
    for (i = strchr(iknnQuery, '(') - iknnQuery + 1; i < strchr(iknnQuery, ')') - iknnQuery - 1; i++)
        if (iknnQuery[i] == ',')
            nQueryFields++;
    qFnames = (char**)palloc0(sizeof(char*) * (++nQueryFields));
    qValues = (int*)palloc0(sizeof(int) * nQueryFields);
    for (i = 0; i < nQueryFields; i++)
        qFnames[i] = (char*)palloc0(sizeof(char) * 256);
    tbl = (char*)palloc0(sizeof(char) * 256);
    digestQuery(iknnQuery, tbl, qFnames, qValues, &k, nQueryFields);
    
    /* initialize heap */
    res.size = 1;
    while (res.size < k + 1)
        res.size <<= 1;
    res.rec = (HeapRec*)palloc0(sizeof(HeapRec) * res.size);
    res.length = 0;

    // open internal connection 
    SPI_connect();

// format query object
    // construct field name fetching command
    strcpy(getFields, "SELECT column_name FROM information_schema.columns WHERE table_name = '");
    strcat(getFields, tbl);
    strcat(getFields, "';");
    // run the SQL command 
    ret = SPI_exec(getFields, 4294967296); 
    // save the number of rows
    procFld = SPI_processed;
    if (ret > 0 && SPI_tuptable != NULL) {
        TupleDesc tupdesc = SPI_tuptable->tupdesc;
        SPITupleTable *tuptable = SPI_tuptable;
        dim = procFld - 4;
        qObj = (queryObj*)palloc0(sizeof(queryObj) * dim);
        for (i = 0; i < dim; i++)
            qObj[i].fieldName = (char*)palloc0(sizeof(char) * 256);
        for (i = 0; i < procFld; i++) {
            HeapTuple tuple = tuptable->vals[i];
            if (strcmp(SPI_getvalue(tuple, tupdesc, 1), "lp_id") == 0) break;
            if (strcmp(SPI_getvalue(tuple, tupdesc, 1), "ncomplete") == 0) break;
            if (strcmp(SPI_getvalue(tuple, tupdesc, 1), "alphavalue") == 0) break;
            if (strcmp(SPI_getvalue(tuple, tupdesc, 1), "ibitmap") == 0) break;
            strcpy(qObj[i].fieldName, SPI_getvalue(tuple, tupdesc, 1));
        }
        for (i = 0; i < dim; i++) {
            for (j = 0; j < nQueryFields; j++) {
                if (strcmp(qFnames[j], qObj[i].fieldName) == 0) {
                    qObj[i].value = qValues[j];
                    qObj[i].complete = 1;
                    break;
                }
            }
            if (j >= nQueryFields)
                qObj[i].complete = 0;
        }
    }
    else {
        // error handle
    }

    // construct bucket fetching command
    strcpy(getBuckets, "SELECT bucketid FROM ");
    strcat(getBuckets, tbl);
    strcat(getBuckets, "_latmp ORDER BY latticeid;");
    // run the SQL command 
    ret = SPI_exec(getBuckets, 4294967296); 
    // save the number of rows
    procBkt = SPI_processed;
    // If some rows were fetched, print them via elog(INFO).
    if (ret > 0 && SPI_tuptable != NULL) {
        TupleDesc tupdescBkt = SPI_tuptable->tupdesc;
        SPITupleTable *tuptableBkt = SPI_tuptable;
        long procObj;
        long qPos;
        int Iseto;

        for (i = 0; i < procBkt; i++) {
            HeapTuple tuple = tuptableBkt->vals[i];
            char *bitmap;

            // get the bitmap value
            bitmap = (char*)palloc0(sizeof(char) * (strlen(SPI_getvalue(tuple, tupdescBkt, 1)) + 1));
            strcpy(bitmap, SPI_getvalue(tuple, tupdescBkt, 1));

            // calculate sum for qalpha
            qAlpha = 0;
            Iseto = 0;
            for (j = 0; j < dim; j++)
                if (bitmap[j] == '1') {
                    qAlpha += qObj[j].value;
                    Iseto++;
                }

            // construct a string representing the tuple
            strcpy(getObjects, "SELECT * FROM lp_");
            strcat(getObjects, tbl);
            strcat(getObjects, "_");
            strcat(getObjects, bitmap);
            strcat(getObjects, " ORDER BY alphavalue;");

            ret = SPI_exec(getObjects, 4294967296);
            procObj = SPI_processed;
            if (ret > 0 && SPI_tuptable != NULL) {
                TupleDesc tupdescObj = SPI_tuptable->tupdesc;
                SPITupleTable *tuptableObj = SPI_tuptable;
                qAlpha /= Iseto;
                ereport(INFO, (errmsg("=================%s===========binarySearch", bitmap)));
                qPos = binarySearch(&tupdescObj, tuptableObj, procObj, qAlpha, dim);
                ereport(INFO, (errmsg("qAlpha: %lf", qAlpha)));
                ereport(INFO, (errmsg("qPos_%s: %ld", bitmap, qPos)));
            }
        }
    }
    else {
        // error handle
    }
    SPI_finish(); //pfree(command);
    PG_RETURN_INT32(procBkt);
}

/*
            for (i = 1, buf[0] = 0; i <= tupdesc->natts; i++)
                snprintf(buf + strlen (buf), sizeof(buf) - strlen(buf), "%s(%s::%s)%s", SPI_fname(tupdesc, i), SPI_getvalue(tuple, tupdesc, i), SPI_gettype(tupdesc, i),(i == tupdesc->natts) ? " " : " |");
*/
