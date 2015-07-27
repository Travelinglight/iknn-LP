#include "postgres.h"
#include "fmgr.h"
#include "executor/spi.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "funcapi.h"

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(iknnLP);

typedef struct {
    char *fieldName;
    int value;
    char complete;
}queryObj;

typedef struct {
    int *vals;
    char *isnull;
    double dist;
}HeapRec;

typedef struct {
    HeapRec *rec;
    long size;
    long length;
}Heap;

/*typedef struct {
    int *data;
    double dist;
}retdata;*/

int dim;
Heap res;

double fabs(double n);
double string2double(char *number);
void string2int(char *number, int *K);
void chopQueryFieldNames(char *fieldNames, char **qFnames);
void chopQueryValues(char *queryValues, int *qValues, int nQueryFields);
void digestQuery(char *iknnQuery, char *tbl, char **qFnames, int *qValues, int *K, int nQueryFields);
void heapInsert(Heap *heap, double dist, SPITupleTable *tuptable, long pt, TupleDesc *tupdesc, char *bitmap);
void heapCover(Heap *heap, long node, double dist, SPITupleTable *tuptable, long pt, TupleDesc *tupdesc, char *bitmap);
int binarySearch(TupleDesc *tupdescObj, SPITupleTable *tuptableObj, long procObj, double qAlpha, int dim);
double calcDist(TupleDesc *tupdesc, SPITupleTable *tuptable, long pt, queryObj *qObj, int dim, int Iseto, char* bitmap, double tao);
void extractVals(SPITupleTable *tuptable, long pt, TupleDesc *tupdesc, HeapRec *rec, char *bitmap);

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

void string2int(char *number, int *K) {
    int i, sign = 1;
    *K = 0;
    for (i = 0; i < strlen(number); i++) {
        if (i == 0 && number[i] == '-') {
            sign = -1;
        }
        else if (number[i] >= '0' && number[i] < '9') {
            *K = *K * 10 + number[i] - '0';
        }
        else {
            // error handling
        }
    }
    *K = *K * sign;
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
    char *number;
    number = (char*)palloc0(sizeof(char) * 256);

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

void digestQuery(char *iknnQuery, char *tbl, char **qFnames, int *qValues, int *K, int nQueryFields) {
//    find 3 nearest neighbour of (a, b, c)(1, 2, 3) from hash
    char *number;
    char *tmpQuery;
    char *fieldNames;
    char *queryValues;
    tmpQuery = (char*)palloc0(strlen(iknnQuery) * sizeof(char));

    // get K
    strcpy(tmpQuery, strchr(iknnQuery, ' ') + 1);
    while (tmpQuery[0] == ' ')
        strcpy(tmpQuery, tmpQuery + 1);
    number = (char*)palloc0((strchr(tmpQuery, ' ') - tmpQuery + 1) * sizeof(char));
    strncpy(number, tmpQuery, strchr(tmpQuery, ' ') - tmpQuery);
    string2int(number, K);
    
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

void heapInsert(Heap *heap, double dist, SPITupleTable *tuptable, long pt, TupleDesc *tupdesc, char *bitmap) {
    long p, f;
    HeapRec tmpRec;

    tmpRec.vals = (int*)palloc0(sizeof(int) * dim);
    tmpRec.isnull = (char*)palloc0(sizeof(char) * dim);

    heap->rec[++heap->length].dist = dist;
    extractVals(tuptable, pt, tupdesc, &heap->rec[heap->length], bitmap); 

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

void heapCover(Heap *heap, long node, double dist, SPITupleTable *tuptable, long pt, TupleDesc *tupdesc, char *bitmap) {
    long p = 1, s;
    HeapRec tmpRec;

    tmpRec.vals = (int*)palloc0(sizeof(int) * dim);
    tmpRec.isnull = (char*)palloc0(sizeof(char) * dim);

    heap->rec[1].dist = dist;
    extractVals(tuptable, pt, tupdesc, &heap->rec[1], bitmap);

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
    while (l <= r) {
        long mid = (l + r) >> 1;
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

double calcDist(TupleDesc *tupdesc, SPITupleTable *tuptable, long pt, queryObj *qObj, int dim, int Iseto, char* bitmap, double tao) {
    int i;
    double sum = 0;
    double tmp = 0;
    double dif;
    HeapTuple tuple;
    tuple = tuptable->vals[pt];

    tmp = tao * Iseto / dim;
    for (i = 1; i <= dim; i++) {
        if (bitmap[i - 1] == '1') {
            dif = string2double(SPI_getvalue(tuple, *tupdesc, i)) - qObj[i - 1].value;
            sum += dif * dif;
            if ((tao >= 0) && (sum > tmp))
                return -1;
        }
    }
    return sum * dim / Iseto;
}

void extractVals(SPITupleTable *tuptable, long pt, TupleDesc *tupdesc, HeapRec *rec, char *bitmap) {
    int i;
    HeapTuple tuple = tuptable->vals[pt];

    strcpy(rec->isnull, bitmap);
    for (i = 0; i < dim; i++)
        if (bitmap[i] == '1')
            rec->vals[i] = (int)string2double(SPI_getvalue(tuple, *tupdesc, i + 1));
}

Datum
iknnLP(PG_FUNCTION_ARGS) {
    char *iknnQuery;
    char *tbl;
    char getBuckets[1024], getFields[1024], getObjects[1024];
    char getOutFields[65536] = "select ";
    char **qFnames;
    int *qValues;
    int nQueryFields = 0;
    int K, i, j;  // dim: dimention
    int ret;
    long procBkt, procFld;
    double qAlpha = 0;
    queryObj *qObj;

    int call_cntr;
    int max_calls;
    AttInMetadata *attinmeta;
    FuncCallContext *funcctx;
    TupleDesc tupdescOut, tupdescHeap;

    if (SRF_IS_FIRSTCALL()) {
        MemoryContext oldcontext;
        /* create a function context for cross-call persistence */
        funcctx = SRF_FIRSTCALL_INIT();
        /* switch to memory context appropriate for multiple function calls */
        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

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
        digestQuery(iknnQuery, tbl, qFnames, qValues, &K, nQueryFields);
        
        /* initialize heap */
        res.size = 1;
        while (res.size < K + 1)
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
            for (i = 0; i < res.size; i++) {
                res.rec[i].vals = (int*)palloc0(sizeof(int) * dim);
                res.rec[i].isnull = (char*)palloc(sizeof(char) * dim);
            }

            for (i = 0; i < procFld; i++) {
                HeapTuple tuple = tuptable->vals[i];
                if (strcmp(SPI_getvalue(tuple, tupdesc, 1), "lp_id") == 0) continue;
                if (strcmp(SPI_getvalue(tuple, tupdesc, 1), "ncomplete") == 0) continue;
                if (strcmp(SPI_getvalue(tuple, tupdesc, 1), "alphavalue") == 0) {
                    strcat(getOutFields, "alphavalue as distance from ");
                    strcat(getOutFields, tbl);
                    strcat(getOutFields, " LIMIT 1;");
                    continue;
                }
                if (strcmp(SPI_getvalue(tuple, tupdesc, 1), "ibitmap") == 0) continue;
                strcpy(qObj[i].fieldName, SPI_getvalue(tuple, tupdesc, 1));
                strcat(getOutFields, qObj[i].fieldName);
                strcat(getOutFields, ", ");
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
            long pPrev, pNext;
            int Iseto;
            double Ua, La;
    
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
   
                    tupdescHeap = tupdescObj; 
                    qAlpha /= Iseto;
                    pPrev = binarySearch(&tupdescObj, tuptableObj, procObj, qAlpha, dim);
                    pNext = pPrev + 1;
                    for (;pPrev > 0; pPrev--) {
                        if (res.length < K) {
                            double dist;
                            dist = calcDist(&tupdescObj, tuptableObj, pPrev, qObj, dim, Iseto, bitmap, -1);
                            heapInsert(&res, dist, tuptableObj, pPrev, &tupdescObj, bitmap);
                            if (res.length == K) {
                                Ua = qAlpha + res.rec[1].dist / dim;
                                La = qAlpha - res.rec[1].dist / dim;
                            }
                        }
                        else {
                            if (La < string2double(SPI_getvalue(tuptableObj->vals[pPrev], tupdescObj, dim + 2))) {
                                double dist;
                                dist = calcDist(&tupdescObj, tuptableObj, pPrev, qObj, dim, Iseto, bitmap, res.rec[1].dist);
                                if (dist >= 0) {
                                    heapCover(&res, 1, dist, tuptableObj, pPrev, &tupdescObj, bitmap);
                                    Ua = qAlpha + res.rec[1].dist / dim;
                                    La = qAlpha - res.rec[1].dist / dim;
                                }
                            }
                            else
                                break;
                        }
                    }
                    for (;pNext < procObj; pNext++) {
                        if (res.length < K) {
                            double dist;
                            dist = calcDist(&tupdescObj, tuptableObj, pNext, qObj, dim, Iseto, bitmap, -1);
                            heapInsert(&res, dist, tuptableObj, pNext, &tupdescObj, bitmap);
                            if (res.length == K) {
                                Ua = qAlpha + res.rec[1].dist / dim;
                                La = qAlpha - res.rec[1].dist / dim;
                            }
                        }
                        else {
                            if (La < string2double(SPI_getvalue(tuptableObj->vals[pNext], tupdescObj, dim + 2))) {
                                double dist;
                                dist = calcDist(&tupdescObj, tuptableObj, pNext, qObj, dim, Iseto, bitmap, res.rec[1].dist);
                                if (dist >= 0) {
                                    heapCover(&res, 1, dist, tuptableObj, pNext, &tupdescObj, bitmap);
                                    Ua = qAlpha + res.rec[1].dist / dim;
                                    La = qAlpha - res.rec[1].dist / dim;
                                }
                            }
                            else
                                break;
                        }
                    }
                }
            }
        }
        else {
            // error handle
        }
        
        // get tuple descriptor for output
        ret = SPI_exec(getOutFields, 1);
        tupdescOut = SPI_tuptable->tupdesc;

        SPI_finish(); //pfree(command);

        /* total number of tuples to be returned */
        funcctx->max_calls = res.length;
        attinmeta = TupleDescGetAttInMetadata(tupdescOut);
        funcctx->attinmeta = attinmeta;

        // restore memory context
        MemoryContextSwitchTo(oldcontext);
    }

    funcctx = SRF_PERCALL_SETUP();

    call_cntr = funcctx->call_cntr;
    max_calls = funcctx->max_calls;
    attinmeta = funcctx->attinmeta;

    if (call_cntr < max_calls) {
        char **values;
        HeapTuple tuple;
        Datum result;

        values = (char**)palloc((dim + 1) * sizeof(char*));
        for (i = 0; i < dim; i++) {
            values[i] = (char*)palloc(32 * sizeof(char));
            snprintf(values[i], 32, "%d", res.rec[call_cntr + 1].vals[i]);
        }
        values[dim] = (char*)palloc(32 * sizeof(char));
        snprintf(values[dim], 32, "%lf", res.rec[call_cntr + 1].dist);

        /* build a tuple */
        tuple = BuildTupleFromCStrings(attinmeta, values);
        /* make the tuple into a datum */
        result = HeapTupleGetDatum(tuple);

        for (i = 0; i <= dim; i++)
            pfree(values[i]);
        pfree(values);

        SRF_RETURN_NEXT(funcctx, result);
    }
    else {
        SRF_RETURN_DONE(funcctx);
    }
}
