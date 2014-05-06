/*
 *   Copyright 2012 Niko Usai <usai.niko@gmail.com>, http://mogui.it
 *
 *   this file is part of pyorient
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
 
#include "pyorientmodule.h"


// @TODO fix all the return values of the function where it makes sense return bool instead of an int
/*
 * main Exception *
 */
static PyObject *PyOrientModuleException;

/*
 * Debug calback function
 */
static PyObject *debug_callback = NULL;

/*
 * Connections Array pool
 */
pyorient_conn pool[PY_MAX_CONNECTIONS];

/*
 * Connection pool array index
 */
int p_index;


/*
 * Connection timer
 */
struct timeval tv;

/*
 * Debug level module wide defaults to silent
 */
int debug_level = ORIENT_SILENT;


 /* Helpers internals functions ------------------------------------------------- */

/**
 * get_connection_index
 * 
 * give you a connection index in the pool until there are free
 */
int get_connection_index( void ) {
   // prima connession
   int ret = p_index;
   // incrementa p_index
   p_index++;

   if (p_index == 0){
        return 0;
   }
   
   if (p_index >= PY_MAX_CONNECTIONS){
        int i;
        for (i=0;i<= PY_MAX_CONNECTIONS;i++){
            if (pool[i].oh == NULL){
                return i;
            }
        }
        return -1;
   }
   
   return ret;
}

/*
 *  is_db_opened_on_connection
 *  
 *  returns 0 if all is ok
 *  returns negative number error code otherwise
 */

int is_db_opened_on_connection(int connection){
    if (pool[connection].oh != NULL){
        if (!pool[connection].dbopened) {
            // database not opened error code
            return -20; 
        }
    } else {
        // wrong connection index error code 
        return -15; 
    }
    
    return O_OK;
}

/**
 * custom_debug()
 *
 *
 */

void custom_debug(const char *message) {
    if (debug_callback != NULL) {
        // call the debug callback defined from python
        PyObject *arglist;
        PyObject *result;

        arglist = Py_BuildValue("(s)", message);
        result = PyObject_CallObject(debug_callback, arglist);
        Py_DECREF(arglist);

        if (result == NULL){
            PyErr_SetString(PyOrientModuleException,"Error in logging callback function.");
        }
    }
    // else do nothing
}


/* MODULE CONFIG METHODS ---------------------------------------------------------- 
 * 
 * methods that config the module behaviour they don't need any connection to be active
 *
 */

/*
 * set debug level module wide
 */
static PyObject *pyorient_setDebugLevel(PyObject *self, PyObject *args, PyObject *kwargs) {

    int ok = PyArg_ParseTuple(args, "i", &debug_level);
    if (!ok){
        PyErr_SetString(PyOrientModuleException,"Error setting debug level");
        return NULL;
    }
    
    Py_RETURN_NONE;
}

/*
 * Set debug callback
 */
static PyObject *pyorient_setLoggingCallback(PyObject *self, PyObject *args, PyObject *kwargs) {
    PyObject *result = NULL;
    PyObject *temp;

    if (PyArg_ParseTuple(args, "O", &temp)) {
        if (!PyCallable_Check(temp)) {
            PyErr_SetString(PyExc_TypeError,"Parameter must be callable");
            return NULL;
        }
        Py_XINCREF(temp);
        Py_XDECREF(debug_callback); // release previous callback
        debug_callback = temp;

        Py_INCREF(Py_None);
        result = Py_None;    
    }

    return result;
}

/* ROOT BINARY METHODS ----------------------------------------------------------------- */

/**
 * connection to database
 */
static PyObject *pyorient_connect(PyObject *self, PyObject *args){
    char *host;
    char *port;
    char *user;
    char *pwd;
    
    orientdb *oh;
    orientdb_con *oc;
    int rc;
    int connection_index;

    PyArg_ParseTuple(args, "ssss", &host, &port, &user, &pwd);
    
    connection_index = get_connection_index();
    
    if( connection_index == -1 ) {
        PyErr_SetString(PyOrientModuleException, "No available connection in pool max connection exceeded");
        return NULL; 
    }
    oh =  o_new();

    // setting debug level 
    o_debug_setlevel(oh, debug_level);
    
    // setting hook to debug function
    o_debug_sethook(oh, &custom_debug);
    
    
    rc = o_prepare_connection(oh, ORIENT_PROTO_BINARY, host, port);


    if (rc != O_OK) {
        o_free(oh);
        PyErr_SetString(PyOrientModuleException,"Problems in preparing connection");
        return NULL;
    }
    rc = o_prepare_user(oh, ORIENT_ADMIN, user,pwd);
    rc = o_prepare_user(oh, ORIENT_USER, user,pwd);
    tv.tv_sec = 5;
    tv.tv_usec = 0;

    oc = o_connect(oh, &tv, 0);
   
    if (!oc) {
        o_free(oh);
        PyErr_SetString(PyOrientModuleException,"Not connected");
        return NULL; 
    }
    
    pool[connection_index].oh = oh;
    pool[connection_index].oc = oc;
    pool[connection_index].connected = TRUE;
    pool[connection_index].dbopened = FALSE;
    return Py_BuildValue("i", connection_index);
}

static PyObject *pyorient_dbcreate(PyObject *self, PyObject *args, PyObject *keywds){
    int rc;
    int conn = 0;
    char *dbname;
    char *kwlist[] = {"dbname","conn", NULL};

    if (!PyArg_ParseTupleAndKeywords(args,keywds, "s|i",kwlist, &dbname, &conn)){
        rc = o_bin_connect(pool[conn].oh, pool[conn].oc, &tv, 0);
        return NULL;
    }
        

    if( !pool[conn].connected ){
        PyErr_SetString(PyOrientModuleException, "Not connected");
        return NULL; 
    } 

    rc = o_bin_connect(pool[conn].oh, pool[conn].oc, &tv, 0);
    if (rc != O_OK) {
        o_free(pool[conn].oh);
        PyErr_SetString(PyOrientModuleException,"Not bin connected");
        return NULL;
    }

    rc = o_bin_dbcreate(pool[conn].oh, pool[conn].oc, &tv, 0, dbname, O_DBTYPE_LOCAL);
    
    if (rc != O_OK) {
        PyErr_SetString(PyOrientModuleException,"Database not created");
        return NULL;
    }

    return Py_BuildValue("i", O_OK);
}

static PyObject *pyorient_dbexists(PyObject *self, PyObject *args, PyObject *keywds){
    int rc;
    int conn = 0;
    char *dbname;
    char *kwlist[] = {"dbname","conn", NULL};
    
    PyArg_ParseTupleAndKeywords(args, keywds, "s|i", kwlist, &dbname, &conn);
    if( !pool[conn].connected ){
        PyErr_SetString(PyExc_RuntimeError, "Not connected");
        return NULL; 
    } 

    rc = o_bin_connect(pool[conn].oh, pool[conn].oc, &tv, 0);
    if (rc != O_OK) {
        o_free(pool[conn].oh);
        PyErr_SetString(PyOrientModuleException,"Not bin connected");
        return NULL;
    }

    rc = o_bin_dbexists(pool[conn].oh, pool[conn].oc, &tv, 0, dbname);
    return Py_BuildValue("i", rc);
}

static PyObject *pyorient_dbdelete(PyObject *self, PyObject *args, PyObject *keywds){
    int rc;
    int conn = 0;
    char *dbname;
    char *kwlist[] = {"dbname","conn", NULL};
        
    PyArg_ParseTupleAndKeywords(args, keywds, "s|i", kwlist, &dbname, &conn);
    if( !pool[conn].connected ){
        PyErr_SetString(PyExc_RuntimeError, "Not connected");
        return NULL;
    } 

    rc = o_bin_connect(pool[conn].oh, pool[conn].oc, &tv, 0);
    if (rc != O_OK) {
        o_free(pool[conn].oh);
        PyErr_SetString(PyOrientModuleException,"Not bin connected");
        return NULL;
    }

    rc = o_bin_dbdelete(pool[conn].oh, pool[conn].oc, &tv, 0, dbname);
    if (rc != O_OK) {
        PyErr_SetString(PyOrientModuleException, "DB not deleted");
    }

    return Py_BuildValue("i", O_OK);
}


static PyObject *pyorient_close(PyObject *self, PyObject *args, PyObject *keywds){
    int conn = 0;
    char *kwlist[] = {"conn", NULL};    
    PyArg_ParseTupleAndKeywords(args, keywds, "|i", kwlist, &conn);
    o_close(pool[conn].oh, pool[conn].oc);
    o_free(pool[conn].oh);
    pool[conn].connected = FALSE;
    pool[conn].oh = NULL;
    pool[conn].oc = NULL;
    pool[conn].dbopened = FALSE;
    if (p_index > 0 ) p_index--;
    return Py_BuildValue("i", O_OK);
}

static PyObject *pyorient_shutdown(PyObject *self, PyObject *args, PyObject *keywds){
    int rc;
    int conn = 0;
    char *kwlist[] = {"conn", NULL};

    if (!PyArg_ParseTupleAndKeywords(args,keywds, "|i",kwlist, &conn)){
        return NULL;
    }
        
    if( !pool[conn].connected ){
        PyErr_SetString(PyOrientModuleException, "Not connected");
        return NULL; 
    }
    rc = o_bin_shutdown(pool[conn].oh, pool[conn].oc, &tv, 0);
    pyorient_close(self, args, keywds);
    return Py_BuildValue("i", O_OK);
}

/* USER BINARY METHODS ----------------------------------------------------------------- */

/*
 * dbopen(database_name,  [connection, user, password])
 *
 * open a database if passed user and password that will be used insted of the ones passsed in connection method
 */
static PyObject *pyorient_dbopen(PyObject *self, PyObject *args, PyObject *keywds){
    int rc;
    int conn = 0;
    char *dbname;
    char *user=NULL;
    char *pwd=NULL;
    char str[O_ERR_MAXLEN];
    char *kwlist[] = {"dbname", "user", "pwd", "conn", NULL};
    PyArg_ParseTupleAndKeywords(args, keywds, "s|ssi", kwlist, &dbname, &user, &pwd, &conn);
    
    if( !pool[conn].connected ){
        PyErr_SetString(PyOrientModuleException, "Not connected");
        return NULL; 
    } 

    if(user != NULL && pwd!=NULL){
        // if it was passed user and password we use that instaed of the former one
        rc = o_prepare_user(pool[conn].oh, ORIENT_USER, user,pwd);
        if (rc != O_OK) {
            o_strerr(rc, str, O_ERR_MAXLEN);
            PyErr_SetString(PyOrientModuleException,"User not prepared");
            return NULL;
        }
    }
    rc = o_bin_dbopen(pool[conn].oh, pool[conn].oc, &tv, 0, dbname); 
    if (rc != O_OK) {
        o_strerr(rc, str, O_ERR_MAXLEN);
        PyErr_SetString(PyOrientModuleException,"not connected to db, could be wrong credential");
        return NULL;
    }
    pool[conn].dbopened = TRUE;
    return Py_BuildValue("i",rc);
}

/*
 * dbsize()
 *
 * returns the curren opened database size
 */
static PyObject *pyorient_dbsize(PyObject *self, PyObject *args, PyObject *keywds) {
    long size;
    int conn = 0;
    int rc;
    char *kwlist[] = {"conn", NULL};
    PyArg_ParseTupleAndKeywords(args, keywds, "|i", kwlist, &conn);
    
    rc = is_db_opened_on_connection(conn);
    if (rc != O_OK) {
        // database not opened
        PyErr_SetString(PyOrientModuleException,"Database not opened on connection");
        return NULL;
    }

    tv.tv_sec = 15;
    tv.tv_usec = 0;
    size = o_bin_dbsize(pool[conn].oh, pool[conn].oc, &tv, 0);
    return Py_BuildValue("l", size);
}

/*
 * dbreload()
 *
 * reload the current opened db
 */
static PyObject *pyorient_dbreload(PyObject *self, PyObject *args, PyObject *keywds) {
    int rc;
    int conn = 0;
    char *kwlist[] = {"conn", NULL};
    
    PyArg_ParseTupleAndKeywords(args, keywds, "|i", kwlist, &conn);
    
    rc = is_db_opened_on_connection(conn);
    if (rc != O_OK) {
        // database not opened
        PyErr_SetString(PyOrientModuleException,"Database not opened on connection");
        return NULL;
    }
    rc = o_bin_dbreload(pool[conn].oh, pool[conn].oc, &tv, 0);
    return Py_BuildValue("i", rc);
}


/*
 * dbcountrecords()
 *
 * count current opened db record number
 */
static PyObject *pyorient_dbcountrecords(PyObject *self, PyObject *args, PyObject *keywds) {
    long records;
    int rc;
    int conn = 0;
    char *kwlist[] = {"conn", NULL};
    
    PyArg_ParseTupleAndKeywords(args, keywds, "|i", kwlist, &conn);
    records = o_bin_dbcountrecords(pool[conn].oh, pool[conn].oc, &tv, 0);
    
    rc = is_db_opened_on_connection(conn);
    if (rc != O_OK) {
        // database not opened
        PyErr_SetString(PyOrientModuleException,"Database not opened on connection");
        return NULL;
    }
    
    return Py_BuildValue("l", records);
}

/*
 * dbclose()
 *
 * close current db
 */
static PyObject *pyorient_dbclose(PyObject *self, PyObject *args, PyObject *keywds) {
    int rc;
    int conn = 0;
    char *kwlist[] = {"conn", NULL};
    
    PyArg_ParseTupleAndKeywords(args, keywds, "|i", kwlist, &conn);
    
    rc = is_db_opened_on_connection(conn);
    if (rc != O_OK) {
        // database not opened
        PyErr_SetString(PyOrientModuleException,"Database not opened on connection");
        return NULL;
    }
    rc = o_bin_dbclose(pool[conn].oh, pool[conn].oc, &tv, 0);
    return Py_BuildValue("i", rc);
}



/*
 * dataclusteradd(type, name, filename, [initial size, connection])
 *
 * adds a datacluster with specified type and name to currently opened database
 */
static PyObject *pyorient_dataclusteradd(PyObject *self, PyObject *args, PyObject *keywds) {
    int clid;
    int rc;
    int conn = 0;
    int initial_size = O_CLUSTER_DEFAULT_SIZE;
    char *type;
    char *name;
    char *filename;
    char *kwlist[] = {"type", "name", "filename", "initial_size", "conn", NULL};
    
    PyArg_ParseTupleAndKeywords(args, keywds, "sss|ii", kwlist, &type, &name, &filename, &initial_size,  &conn);
    
    rc = is_db_opened_on_connection(conn);
    if (rc != O_OK) {
        // database not opened
        PyErr_SetString(PyOrientModuleException,"Database not opened on connection");
        return NULL;
        return Py_BuildValue("i", rc);
    }
    
    clid = o_bin_dataclusteradd(pool[conn].oh, pool[conn].oc, &tv, 0, type, name, filename, initial_size);
    return Py_BuildValue("i", clid);
}

/*
 * dataclusterremove(cluster_id)
 *
 * remove given cluster
 */
static PyObject *pyorient_dataclusterremove(PyObject *self, PyObject *args, PyObject *keywds) {
    int clid;
    int conn = 0;
    int rc;
    char *kwlist[] = {"clid", "conn", NULL};
    
    PyArg_ParseTupleAndKeywords(args, keywds, "i|i", kwlist, &clid,  &conn);
    
    rc = is_db_opened_on_connection(conn);
    if (rc != O_OK) {
        // database not opened
        PyErr_SetString(PyOrientModuleException,"Database not opened on connection");
        return NULL;
        return Py_BuildValue("i", rc);
    }
    
    rc = (int) o_bin_dataclusterremove(pool[conn].oh, pool[conn].oc, &tv, 0, clid);
    return (rc == 1) ? Py_True : Py_False;
}
/*
 * dataclustercount(cluster_id)
 * 
 * return dimension of given datacluster id
 */
static PyObject *pyorient_dataclustercount(PyObject *self, PyObject *args, PyObject *keywds) {
    int clid;
    int rc;
    int conn = 0;
    long count;
    char *kwlist[] = {"clid", "conn", NULL};
    
    PyArg_ParseTupleAndKeywords(args, keywds, "i|i", kwlist, &clid,  &conn);
    
    rc = is_db_opened_on_connection(conn);
    if (rc != O_OK) {
        // database not opened
        PyErr_SetString(PyOrientModuleException,"Database not opened on connection");
        return NULL;
    }
    // @BUG this seems buggy returns always 0
    count =  o_bin_dataclustercount(pool[conn].oh, pool[conn].oc, &tv, 0, clid);
    return Py_BuildValue("l", count);
}

/*
 * dataclusterrange(cluster_id)
 *
 * returns the range of the cluster
 */
static PyObject *pyorient_dataclusterdatarange(PyObject *self, PyObject *args, PyObject *keywds) {
    int clid;
    int rc;
    int conn = 0;
    long begin, end;
    char *kwlist[] = {"clid", "conn", NULL};
    
    PyArg_ParseTupleAndKeywords(args, keywds, "i|i", kwlist, &clid,  &conn);
    
    rc = is_db_opened_on_connection(conn);
    if (rc != O_OK) {
        // database not opened
        PyErr_SetString(PyOrientModuleException,"Database not opened on connection");
        return NULL;
    }
    
    rc = o_bin_dataclusterdatarange(pool[conn].oh, pool[conn].oc, &tv, 0, clid, &begin, &end);
    if (rc != O_OK){
        PyErr_SetString(PyOrientModuleException,"errors in retrieving data range in cluster, maybe wrong cluster");
        return NULL;
    }
    return Py_BuildValue("(ll)",begin, end);
}

/*
 * command 
 *
 * execute a command return a raw string to parse 
 */
static PyObject *pyorient_command(PyObject *self, PyObject *args, PyObject *keywds) {
    int rc;
    int conn = 0;
    char *command;
    char *command_type = O_CMD_QUERYSYNC;
    int limit = 20;
    char *fetchplan = "*:-1";
    const char *raw_result;
    ODOC_OBJECT *odoc;
    PyObject *result;
    int i;
    char *kwlist[] = {"command", "limit", "command_type", "fetchplan", "conn", NULL};
    
    PyArg_ParseTupleAndKeywords(args, keywds, "s|issi", kwlist, &command, &limit, &command_type, &fetchplan,  &conn);
    
    rc = is_db_opened_on_connection(conn);
    if (rc != O_OK) {
        // database not opened
        PyErr_SetString(PyOrientModuleException,"Database not opened on connection");
        return NULL;
    }
    
    odoc = o_bin_command(pool[conn].oh, pool[conn].oc, &tv, 0, command_type, command, limit, fetchplan);

    /* @TODO manage this situation now it isn't working properly in liborient we cannot know if a query fails or it is empty
    if (odoc == NULL){
        PyErr_SetString(PyOrientModuleException,"COMMAND: problem getting record");
        return NULL;
    }*/
    raw_result = odoc_getraw(odoc, NULL);

    result = PyList_New(0);
	
	for (i=0; i<odoc_getnumrecords(odoc); i++) {
        PyObject *item = Py_BuildValue("s", odoc_fetchrawrecord(odoc, NULL, i));
        PyList_Append(result, item);
        Py_XDECREF(item);
	}

    ODOC_FREE_DOCUMENT(odoc);
    return result;
}


/*
 * record create 
 *
 * create a record 
 */
static PyObject *pyorient_recordcreate(PyObject *self, PyObject *args, PyObject *keywds) {
    int rc;
    int conn = 0;
    int cluster_id;
    char *raw_record;
    long position;
    char *kwlist[] = {"cluster_id", "raw_record", "conn", NULL};
    
    ODOC_OBJECT *odoc;

    PyArg_ParseTupleAndKeywords(args, keywds, "is|i", kwlist, &cluster_id, &raw_record,  &conn);
    
    rc = is_db_opened_on_connection(conn);
    if (rc != O_OK) {
        // database not opened
        PyErr_SetString(PyOrientModuleException,"Database not opened on connection");
        return NULL;
    }
    ODOC_INITIALIZE_DOCUMENT(odoc, NULL);
    
    if (!odoc){
        PyErr_SetString(PyOrientModuleException,"COMMAND: problem getting record");
        return NULL;
    }
    odoc_setraw(odoc, raw_record, (int) strlen(raw_record));
    
    position = o_bin_recordcreate(pool[conn].oh, pool[conn].oc, &tv, 0,cluster_id, odoc, O_RECORD_DOC, O_RECSYNC);
    
    ODOC_FREE_DOCUMENT(odoc);
    return Py_BuildValue("l",position);
}


/*
 * recordload 
 *
 * load a record
 */
static PyObject *pyorient_recordload(PyObject *self, PyObject *args, PyObject *keywds) {
    int rc;
    int conn = 0;
    int cluster_id;
    long cluster_position;
    char *fetchplan = "*:-1";
    PyObject *ret;

    const char *raw_result;
    ODOC_OBJECT *odoc;
    
    char *kwlist[] = {"cluster_id", "cluster_position", "fetchplan", "conn", NULL};

    PyArg_ParseTupleAndKeywords(args, keywds, "il|si", kwlist, &cluster_id, &cluster_position, &fetchplan,  &conn);
    
    rc = is_db_opened_on_connection(conn);
    if (rc != O_OK) {
        // database not opened
        PyErr_SetString(PyOrientModuleException,"Database not opened on connection");
        return NULL;
    }
    
    odoc = o_bin_recordload(pool[conn].oh, pool[conn].oc, &tv, 0, cluster_id, cluster_position, fetchplan);
    
    if (!odoc){
        PyErr_SetString(PyOrientModuleException,"COMMAND: problem getting record");
        return NULL;
    }
    raw_result = odoc_getraw(odoc, NULL);

    ret = Py_BuildValue("s",raw_result);
    ODOC_FREE_DOCUMENT(odoc);
    return ret;
}


/* MODULE INIT ------------------------------------------------ */
static PyMethodDef _pyorient_methods[] = {

    { "setDebugLevel", (PyCFunction)pyorient_setDebugLevel, METH_VARARGS | METH_KEYWORDS, NULL },
    { "setLoggingCallback", (PyCFunction)pyorient_setLoggingCallback, METH_VARARGS | METH_KEYWORDS, NULL },

    { "connect", (PyCFunction)pyorient_connect, METH_VARARGS, NULL },
    { "shutdown", (PyCFunction)pyorient_shutdown, METH_VARARGS | METH_KEYWORDS, NULL },
    { "dbcreate", (PyCFunction)pyorient_dbcreate, METH_VARARGS | METH_KEYWORDS, NULL },
    { "dbexists", (PyCFunction)pyorient_dbexists, METH_VARARGS | METH_KEYWORDS, NULL },
    { "dbdelete", (PyCFunction)pyorient_dbdelete, METH_VARARGS | METH_KEYWORDS, NULL },
    { "close", (PyCFunction)pyorient_close, METH_VARARGS | METH_KEYWORDS, NULL },

    { "dbopen", (PyCFunction)pyorient_dbopen, METH_VARARGS | METH_KEYWORDS, NULL },
    { "dbsize", (PyCFunction)pyorient_dbsize, METH_VARARGS | METH_KEYWORDS, NULL },
    { "dbreload", (PyCFunction)pyorient_dbreload, METH_VARARGS | METH_KEYWORDS, NULL },
    { "dbcountrecords", (PyCFunction)pyorient_dbcountrecords, METH_VARARGS| METH_KEYWORDS, NULL },
    { "dbclose", (PyCFunction)pyorient_dbclose, METH_VARARGS | METH_KEYWORDS, NULL },
    
    { "dataclusteradd", (PyCFunction)pyorient_dataclusteradd, METH_VARARGS | METH_KEYWORDS, NULL },
    { "dataclusterremove", (PyCFunction)pyorient_dataclusterremove, METH_VARARGS | METH_KEYWORDS, NULL },
    { "dataclustercount", (PyCFunction)pyorient_dataclustercount, METH_VARARGS | METH_KEYWORDS, NULL },
    { "dataclusterdatarange", (PyCFunction)pyorient_dataclusterdatarange, METH_VARARGS | METH_KEYWORDS, NULL },
    
    { "command", (PyCFunction)pyorient_command, METH_VARARGS | METH_KEYWORDS, NULL },
    { "recordcreate", (PyCFunction)pyorient_recordcreate, METH_VARARGS | METH_KEYWORDS, NULL },
    { "recordload", (PyCFunction)pyorient_recordload, METH_VARARGS | METH_KEYWORDS, NULL },
    {NULL, NULL, 0, NULL}
};

PyMODINIT_FUNC init_pyorient( void )
{
    PyObject *m;
    m = Py_InitModule3("_pyorient", _pyorient_methods, "Py OrientDB c binding");

    // main exception
    PyOrientModuleException = PyErr_NewException("_pyorient.PyOrientModuleException", NULL, NULL);
    Py_INCREF(PyOrientModuleException);
    PyModule_AddObject(m, "PyOrientModuleException", PyOrientModuleException);

    // cluster constants
    PyModule_AddStringConstant(m, "CLUSTER_PHYSICAL",O_CLUSTER_PHYSICAL );
    PyModule_AddStringConstant(m, "CLUSTER_LOGICAL",O_CLUSTER_LOGICAL );
    PyModule_AddStringConstant(m, "CLUSTER_MEMORY",O_CLUSTER_MEMORY );
    PyModule_AddIntConstant(m, "CLUSTER_DEFAULT_SIZE",O_CLUSTER_DEFAULT_SIZE );
    
    // command costants
    PyModule_AddStringConstant(m, "QUERY_SYNC",O_CMD_QUERYSYNC );
    PyModule_AddStringConstant(m, "QUERY_ASYNC",O_CMD_QUERYASYNC );
    
    // debug levels constants
    PyModule_AddIntConstant(m, "PARANOID", ORIENT_PARANOID);
    PyModule_AddIntConstant(m, "DEBUG", ORIENT_DEBUG);
    PyModule_AddIntConstant(m, "NOTICE", ORIENT_NOTICE);
    PyModule_AddIntConstant(m, "INFO", ORIENT_INFO);
    PyModule_AddIntConstant(m, "NORMAL", ORIENT_NORMAL);
    PyModule_AddIntConstant(m, "WARNING", ORIENT_WARNING);
    PyModule_AddIntConstant(m, "CRITICAL", ORIENT_CRITICAL);
    PyModule_AddIntConstant(m, "FATAL", ORIENT_FATAL);
    PyModule_AddIntConstant(m, "SILENT", ORIENT_SILENT);

    p_index = 0;
}
