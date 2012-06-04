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
 
#include <Python.h>
#include <liborient/liborient.h>

#define PY_MAX_CONNECTIONS 20
#define TRUE 1
#define FALSE 0


typedef struct 
{
    orientdb *oh;
    orientdb_con *oc;
    int connected;
    int dbopened;
} pyorient_conn;
