#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include "graphdb.h"
#include "graph_algorithms.h"

/* ------------------------------------------------------------------ */
/*  PyGraph object                                                      */
/* ------------------------------------------------------------------ */

typedef struct {
    PyObject_HEAD
    Graph *g;
} PyGraph;

/* ------------------------------------------------------------------ */
/*  Lifecycle                                                           */
/* ------------------------------------------------------------------ */

static int PyGraph_init(PyGraph *self, PyObject *args, PyObject *kwargs)
{
    const char *path;
    static char *kwlist[] = { "path", NULL };

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "s", kwlist, &path))
        return -1;

    self->g = graph_open(path);
    if (!self->g) {
        PyErr_SetString(PyExc_RuntimeError, "failed to open graph database");
        return -1;
    }
    return 0;
}

static void PyGraph_dealloc(PyGraph *self)
{
    if (self->g) {
        graph_close(self->g);
        self->g = NULL;
    }
    Py_TYPE(self)->tp_free((PyObject *)self);
}

/* ------------------------------------------------------------------ */
/*  Node methods                                                        */
/* ------------------------------------------------------------------ */

static PyObject *PyGraph_create_node(PyGraph *self, PyObject *args, PyObject *kwargs)
{
    const char *labels    = "";
    const char *props     = "{}";
    static char *kwlist[] = { "labels", "properties", NULL };

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "|ss", kwlist, &labels, &props))
        return NULL;

    int64_t id;
    int rc = graph_create_node(self->g, labels, props, &id);
    if (rc != GRAPHDB_OK) {
        PyErr_SetString(PyExc_RuntimeError, graph_last_error(self->g));
        return NULL;
    }
    return PyLong_FromLongLong((long long)id);
}

static PyObject *PyGraph_get_node(PyGraph *self, PyObject *args)
{
    long long id;
    if (!PyArg_ParseTuple(args, "L", &id)) return NULL;

    Node n;
    int rc = graph_get_node(self->g, (int64_t)id, &n);
    if (rc == GRAPHDB_NOT_FOUND) {
        Py_RETURN_NONE;
    }
    if (rc != GRAPHDB_OK) {
        PyErr_SetString(PyExc_RuntimeError, graph_last_error(self->g));
        return NULL;
    }

    return Py_BuildValue("{s:L,s:s,s:s}",
                         "id",         (long long)n.id,
                         "labels",     n.labels,
                         "properties", n.properties);
}

static PyObject *PyGraph_set_node_property(PyGraph *self, PyObject *args)
{
    long long   id;
    const char *key;
    const char *value_json;

    if (!PyArg_ParseTuple(args, "Lss", &id, &key, &value_json)) return NULL;

    int rc = graph_set_node_property(self->g, (int64_t)id, key, value_json);
    if (rc != GRAPHDB_OK) {
        PyErr_SetString(PyExc_RuntimeError, graph_last_error(self->g));
        return NULL;
    }
    Py_RETURN_NONE;
}

static PyObject *PyGraph_remove_node_property(PyGraph *self, PyObject *args)
{
    long long   id;
    const char *key;

    if (!PyArg_ParseTuple(args, "Ls", &id, &key)) return NULL;

    graph_remove_node_property(self->g, (int64_t)id, key);
    Py_RETURN_NONE;
}

static PyObject *PyGraph_add_label(PyGraph *self, PyObject *args)
{
    long long   id;
    const char *label;
    if (!PyArg_ParseTuple(args, "Ls", &id, &label)) return NULL;
    graph_add_label(self->g, (int64_t)id, label);
    Py_RETURN_NONE;
}

static PyObject *PyGraph_remove_label(PyGraph *self, PyObject *args)
{
    long long   id;
    const char *label;
    if (!PyArg_ParseTuple(args, "Ls", &id, &label)) return NULL;
    graph_remove_label(self->g, (int64_t)id, label);
    Py_RETURN_NONE;
}

static PyObject *PyGraph_delete_node(PyGraph *self, PyObject *args, PyObject *kwargs)
{
    long long id;
    int detach = 0;
    static char *kwlist[] = { "id", "detach", NULL };

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "L|p", kwlist, &id, &detach))
        return NULL;

    int rc = graph_delete_node(self->g, (int64_t)id, detach);
    if (rc != GRAPHDB_OK) {
        PyErr_SetString(PyExc_RuntimeError, graph_last_error(self->g));
        return NULL;
    }
    Py_RETURN_NONE;
}

/* ------------------------------------------------------------------ */
/*  Relationship methods                                                */
/* ------------------------------------------------------------------ */

static PyObject *PyGraph_create_rel(PyGraph *self, PyObject *args, PyObject *kwargs)
{
    long long   src, dst;
    const char *type;
    const char *props     = "{}";
    static char *kwlist[] = { "src", "dst", "type", "properties", NULL };

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "LLs|s", kwlist,
                                     &src, &dst, &type, &props))
        return NULL;

    int64_t id;
    int rc = graph_create_rel(self->g, (int64_t)src, (int64_t)dst, type, props, &id);
    if (rc != GRAPHDB_OK) {
        PyErr_SetString(PyExc_RuntimeError, graph_last_error(self->g));
        return NULL;
    }
    return PyLong_FromLongLong((long long)id);
}

static PyObject *PyGraph_get_rel(PyGraph *self, PyObject *args)
{
    long long id;
    if (!PyArg_ParseTuple(args, "L", &id)) return NULL;

    Relationship r;
    int rc = graph_get_rel(self->g, (int64_t)id, &r);
    if (rc == GRAPHDB_NOT_FOUND) { Py_RETURN_NONE; }
    if (rc != GRAPHDB_OK) {
        PyErr_SetString(PyExc_RuntimeError, graph_last_error(self->g));
        return NULL;
    }

    return Py_BuildValue("{s:L,s:L,s:L,s:s,s:s}",
                         "id",         (long long)r.id,
                         "src_id",     (long long)r.src_id,
                         "dst_id",     (long long)r.dst_id,
                         "type",       r.type,
                         "properties", r.properties);
}

static PyObject *PyGraph_set_rel_property(PyGraph *self, PyObject *args)
{
    long long   id;
    const char *key, *value_json;
    if (!PyArg_ParseTuple(args, "Lss", &id, &key, &value_json)) return NULL;
    graph_set_rel_property(self->g, (int64_t)id, key, value_json);
    Py_RETURN_NONE;
}

static PyObject *PyGraph_delete_rel(PyGraph *self, PyObject *args)
{
    long long id;
    if (!PyArg_ParseTuple(args, "L", &id)) return NULL;
    graph_delete_rel(self->g, (int64_t)id);
    Py_RETURN_NONE;
}

/* ------------------------------------------------------------------ */
/*  Merge                                                               */
/* ------------------------------------------------------------------ */

static PyObject *PyGraph_merge_node(PyGraph *self, PyObject *args, PyObject *kwargs)
{
    const char *labels        = "";
    const char *match_props   = "{}";
    const char *on_create     = NULL;
    const char *on_match      = NULL;
    static char *kwlist[] = { "labels", "match_properties",
                               "on_create", "on_match", NULL };

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "|sszz", kwlist,
                                     &labels, &match_props,
                                     &on_create, &on_match))
        return NULL;

    int64_t id;
    int rc = graph_merge_node(self->g, labels, match_props,
                              on_create, on_match, &id);
    if (rc != GRAPHDB_OK) {
        PyErr_SetString(PyExc_RuntimeError, graph_last_error(self->g));
        return NULL;
    }
    return PyLong_FromLongLong((long long)id);
}

/* ------------------------------------------------------------------ */
/*  Query                                                               */
/* ------------------------------------------------------------------ */

static PyObject *PyGraph_query(PyGraph *self, PyObject *args)
{
    const char *cypher;
    if (!PyArg_ParseTuple(args, "s", &cypher)) return NULL;

    ResultSet *rs = graph_query(self->g, cypher);
    if (!rs) {
        PyErr_SetString(PyExc_RuntimeError, "query returned null result set");
        return NULL;
    }

    if (rs->error[0]) {
        PyErr_SetString(PyExc_RuntimeError, rs->error);
        resultset_free(rs);
        return NULL;
    }

    PyObject *list = PyList_New(rs->count);
    if (!list) { resultset_free(rs); return NULL; }

    for (int i = 0; i < rs->count; i++) {
        ResultRow *row = &rs->rows[i];
        PyObject  *item;

        switch (row->type) {
            case ROW_NODE:
                item = Py_BuildValue("{s:L,s:s,s:s}",
                                     "id",         (long long)row->node.id,
                                     "labels",     row->node.labels,
                                     "properties", row->node.properties);
                break;
            case ROW_REL:
                item = Py_BuildValue("{s:L,s:L,s:L,s:s,s:s}",
                                     "id",         (long long)row->rel.id,
                                     "src_id",     (long long)row->rel.src_id,
                                     "dst_id",     (long long)row->rel.dst_id,
                                     "type",       row->rel.type,
                                     "properties", row->rel.properties);
                break;
            default:
                item = PyUnicode_FromString(row->scalar);
                break;
        }

        if (!item) {
            Py_DECREF(list);
            resultset_free(rs);
            return NULL;
        }
        PyList_SET_ITEM(list, i, item);
    }

    resultset_free(rs);
    return list;
}

/* ------------------------------------------------------------------ */
/*  Shortest path                                                       */
/* ------------------------------------------------------------------ */

static PyObject *PyGraph_shortest_path(PyGraph *self, PyObject *args, PyObject *kwargs)
{
    long long   src, dst;
    const char *rel_type  = NULL;
    static char *kwlist[] = { "src", "dst", "rel_type", NULL };

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "LL|z", kwlist,
                                     &src, &dst, &rel_type))
        return NULL;

    Path *path = graph_shortest_path(self->g, (int64_t)src, (int64_t)dst, rel_type);
    if (!path) Py_RETURN_NONE;

    PyObject *list = PyList_New(0);
    PathNode *cur  = path->head;
    while (cur) {
        PyObject *item = Py_BuildValue("{s:L,s:L}",
                                       "node_id", (long long)cur->node_id,
                                       "rel_id",  (long long)cur->rel_id);
        PyList_Append(list, item);
        Py_DECREF(item);
        cur = cur->next;
    }
    path_free(path);
    return list;
}

/* ------------------------------------------------------------------ */
/*  close                                                               */
/* ------------------------------------------------------------------ */

static PyObject *PyGraph_close(PyGraph *self, PyObject *Py_UNUSED(ignored))
{
    if (self->g) {
        graph_close(self->g);
        self->g = NULL;
    }
    Py_RETURN_NONE;
}

/* ------------------------------------------------------------------ */
/*  Algorithm bindings                                                  */
/* ------------------------------------------------------------------ */

static PyObject *PyGraph_is_connected(PyGraph *self, PyObject *args, PyObject *kwargs)
{
    const char *rel_type  = NULL;
    static char *kwlist[] = { "rel_type", NULL };
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "|z", kwlist, &rel_type))
        return NULL;
    int result = algo_is_connected(self->g, rel_type);
    return PyBool_FromLong(result);
}

static PyObject *PyGraph_connected_components(PyGraph *self, PyObject *args, PyObject *kwargs)
{
    const char *rel_type  = NULL;
    static char *kwlist[] = { "rel_type", NULL };
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "|z", kwlist, &rel_type))
        return NULL;

    ComponentResult *r = algo_connected_components(self->g, rel_type);
    if (!r) { PyErr_SetString(PyExc_RuntimeError, "failed to compute components"); return NULL; }

    /* return dict: {node_id: component_index} */
    PyObject *d = PyDict_New();
    for (int i = 0; i < r->node_count; i++) {
        PyObject *key = PyLong_FromLongLong((long long)r->node_ids[i]);
        PyObject *val = PyLong_FromLong(r->component[i]);
        PyDict_SetItem(d, key, val);
        Py_DECREF(key); Py_DECREF(val);
    }

    /* attach component_count as metadata via tuple: (dict, count) */
    PyObject *count = PyLong_FromLong(r->component_count);
    PyObject *result = PyTuple_Pack(2, d, count);
    Py_DECREF(d); Py_DECREF(count);

    component_result_free(r);
    return result;
}

static PyObject *PyGraph_find_bridges(PyGraph *self, PyObject *args, PyObject *kwargs)
{
    const char *rel_type  = NULL;
    static char *kwlist[] = { "rel_type", NULL };
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "|z", kwlist, &rel_type))
        return NULL;

    IdList *l = algo_find_bridges(self->g, rel_type);
    if (!l) { PyErr_SetString(PyExc_RuntimeError, "failed to find bridges"); return NULL; }

    PyObject *lst = PyList_New(l->count);
    for (int i = 0; i < l->count; i++)
        PyList_SET_ITEM(lst, i, PyLong_FromLongLong((long long)l->ids[i]));

    idlist_free(l);
    return lst;
}

static PyObject *PyGraph_find_articulation_points(PyGraph *self, PyObject *args, PyObject *kwargs)
{
    const char *rel_type  = NULL;
    static char *kwlist[] = { "rel_type", NULL };
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "|z", kwlist, &rel_type))
        return NULL;

    IdList *l = algo_find_articulation_points(self->g, rel_type);
    if (!l) { PyErr_SetString(PyExc_RuntimeError, "failed to find articulation points"); return NULL; }

    PyObject *lst = PyList_New(l->count);
    for (int i = 0; i < l->count; i++)
        PyList_SET_ITEM(lst, i, PyLong_FromLongLong((long long)l->ids[i]));

    idlist_free(l);
    return lst;
}

static PyObject *PyGraph_n1_is_critical(PyGraph *self, PyObject *args)
{
    long long rel_id;
    if (!PyArg_ParseTuple(args, "L", &rel_id)) return NULL;
    int result = algo_n1_is_critical_v2(self->g, (int64_t)rel_id);
    return PyBool_FromLong(result);
}

static PyObject *PyGraph_dijkstra(PyGraph *self, PyObject *args, PyObject *kwargs)
{
    long long   src;
    const char *rel_type   = NULL;
    const char *weight_key = NULL;
    static char *kwlist[]  = { "src", "rel_type", "weight_key", NULL };

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "L|zz", kwlist,
                                     &src, &rel_type, &weight_key))
        return NULL;

    DijkstraResult *r = algo_dijkstra(self->g, (int64_t)src, rel_type, weight_key);
    if (!r) { PyErr_SetString(PyExc_RuntimeError, "dijkstra failed"); return NULL; }

    /* return list of dicts sorted by node_id */
    PyObject *lst = PyList_New(r->count);
    for (int i = 0; i < r->count; i++) {
        DijkstraNode *dn = &r->nodes[i];
        PyObject *item = Py_BuildValue(
            "{s:L, s:L, s:L, s:d}",
            "node_id",      (long long)dn->node_id,
            "prev_node_id", (long long)dn->prev_node_id,
            "prev_rel_id",  (long long)dn->prev_rel_id,
            "dist",         dn->dist
        );
        PyList_SET_ITEM(lst, i, item);
    }

    dijkstra_result_free(r);
    return lst;
}

static PyObject *PyGraph_degree(PyGraph *self, PyObject *args, PyObject *kwargs)
{
    const char *rel_type  = NULL;
    static char *kwlist[] = { "rel_type", NULL };
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "|z", kwlist, &rel_type))
        return NULL;

    DegreeResult *r = algo_degree(self->g, rel_type);
    if (!r) { PyErr_SetString(PyExc_RuntimeError, "degree failed"); return NULL; }

    PyObject *d = PyDict_New();
    for (int i = 0; i < r->count; i++) {
        PyObject *key = PyLong_FromLongLong((long long)r->entries[i].node_id);
        PyObject *val = PyLong_FromLong(r->entries[i].degree);
        PyDict_SetItem(d, key, val);
        Py_DECREF(key); Py_DECREF(val);
    }

    degree_result_free(r);
    return d;
}

/* ------------------------------------------------------------------ */
/*  Context manager                                                     */
/* ------------------------------------------------------------------ */

static PyObject *PyGraph_enter(PyGraph *self, PyObject *Py_UNUSED(args))
{
    Py_INCREF(self);
    return (PyObject *)self;
}

static PyObject *PyGraph_exit(PyGraph *self, PyObject *args)
{
    (void)args;
    PyGraph_close(self, NULL);
    Py_RETURN_FALSE;
}

/* ------------------------------------------------------------------ */
/*  Method table                                                        */
/* ------------------------------------------------------------------ */

static PyMethodDef PyGraph_methods[] = {
    { "create_node",          (PyCFunction)PyGraph_create_node,
      METH_VARARGS | METH_KEYWORDS,
      "create_node(labels='', properties='{}') -> int id" },

    { "get_node",             (PyCFunction)PyGraph_get_node,
      METH_VARARGS,
      "get_node(id) -> dict or None" },

    { "set_node_property",    (PyCFunction)PyGraph_set_node_property,
      METH_VARARGS,
      "set_node_property(id, key, value_json)" },

    { "remove_node_property", (PyCFunction)PyGraph_remove_node_property,
      METH_VARARGS,
      "remove_node_property(id, key)" },

    { "add_label",            (PyCFunction)PyGraph_add_label,
      METH_VARARGS,
      "add_label(id, label)" },

    { "remove_label",         (PyCFunction)PyGraph_remove_label,
      METH_VARARGS,
      "remove_label(id, label)" },

    { "delete_node",          (PyCFunction)PyGraph_delete_node,
      METH_VARARGS | METH_KEYWORDS,
      "delete_node(id, detach=False)" },

    { "create_rel",           (PyCFunction)PyGraph_create_rel,
      METH_VARARGS | METH_KEYWORDS,
      "create_rel(src, dst, type, properties='{}') -> int id" },

    { "get_rel",              (PyCFunction)PyGraph_get_rel,
      METH_VARARGS,
      "get_rel(id) -> dict or None" },

    { "set_rel_property",     (PyCFunction)PyGraph_set_rel_property,
      METH_VARARGS,
      "set_rel_property(id, key, value_json)" },

    { "delete_rel",           (PyCFunction)PyGraph_delete_rel,
      METH_VARARGS,
      "delete_rel(id)" },

    { "merge_node",           (PyCFunction)PyGraph_merge_node,
      METH_VARARGS | METH_KEYWORDS,
      "merge_node(labels, match_properties, on_create=None, on_match=None) -> int id" },

    { "query",                (PyCFunction)PyGraph_query,
      METH_VARARGS,
      "query(cypher) -> list of dicts/scalars" },

    { "shortest_path",        (PyCFunction)PyGraph_shortest_path,
      METH_VARARGS | METH_KEYWORDS,
      "shortest_path(src, dst, rel_type=None) -> list of {node_id, rel_id}" },

    { "close",                (PyCFunction)PyGraph_close,
      METH_NOARGS,
      "close() - release database handle" },

    { "is_connected",             (PyCFunction)PyGraph_is_connected,
      METH_VARARGS | METH_KEYWORDS,
      "is_connected(rel_type=None) -> bool" },

    { "connected_components",     (PyCFunction)PyGraph_connected_components,
      METH_VARARGS | METH_KEYWORDS,
      "connected_components(rel_type=None) -> ({node_id: component_idx}, n_components)" },

    { "find_bridges",             (PyCFunction)PyGraph_find_bridges,
      METH_VARARGS | METH_KEYWORDS,
      "find_bridges(rel_type=None) -> [rel_id, ...]" },

    { "find_articulation_points", (PyCFunction)PyGraph_find_articulation_points,
      METH_VARARGS | METH_KEYWORDS,
      "find_articulation_points(rel_type=None) -> [node_id, ...]" },

    { "n1_is_critical",           (PyCFunction)PyGraph_n1_is_critical,
      METH_VARARGS,
      "n1_is_critical(rel_id) -> bool" },

    { "dijkstra",                 (PyCFunction)PyGraph_dijkstra,
      METH_VARARGS | METH_KEYWORDS,
      "dijkstra(src, rel_type=None, weight_key=None) -> [{node_id, prev_node_id, prev_rel_id, dist}]" },

    { "degree",                   (PyCFunction)PyGraph_degree,
      METH_VARARGS | METH_KEYWORDS,
      "degree(rel_type=None) -> {node_id: degree}" },

    { "__enter__",            (PyCFunction)PyGraph_enter, METH_NOARGS, NULL },
    { "__exit__",             (PyCFunction)PyGraph_exit,  METH_VARARGS, NULL },

    { NULL, NULL, 0, NULL }
};

/* ------------------------------------------------------------------ */
/*  Type definition                                                     */
/* ------------------------------------------------------------------ */

static PyTypeObject PyGraphType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    .tp_name      = "graphdb.Graph",
    .tp_basicsize = sizeof(PyGraph),
    .tp_dealloc   = (destructor)PyGraph_dealloc,
    .tp_flags     = Py_TPFLAGS_DEFAULT,
    .tp_doc       = "Graph database handle backed by SQLite",
    .tp_methods   = PyGraph_methods,
    .tp_init      = (initproc)PyGraph_init,
    .tp_new       = PyType_GenericNew,
};

/* ------------------------------------------------------------------ */
/*  Module definition                                                   */
/* ------------------------------------------------------------------ */

static PyModuleDef graphdb_module = {
    PyModuleDef_HEAD_INIT,
    "graphdb",
    "High-performance graph database with Cypher query support",
    -1,
    NULL
};

PyMODINIT_FUNC PyInit_graphdb(void)
{
    if (PyType_Ready(&PyGraphType) < 0) return NULL;

    PyObject *m = PyModule_Create(&graphdb_module);
    if (!m) return NULL;

    Py_INCREF(&PyGraphType);
    if (PyModule_AddObject(m, "Graph", (PyObject *)&PyGraphType) < 0) {
        Py_DECREF(&PyGraphType);
        Py_DECREF(m);
        return NULL;
    }

    PyModule_AddIntConstant(m, "OK",        GRAPHDB_OK);
    PyModule_AddIntConstant(m, "ERR",       GRAPHDB_ERR);
    PyModule_AddIntConstant(m, "NOT_FOUND", GRAPHDB_NOT_FOUND);
    PyModule_AddStringConstant(m, "VERSION", GRAPHDB_VERSION);

    return m;
}
