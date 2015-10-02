from .property import (
        Property
        , PropertyEncoder
        , LinkedClassProperty
        , LinkedProperty
    )
from .exceptions import ReservedWordError
from .vertex import Vertex
from .edge import Edge
from .broker import get_broker
from .query import Query

import pyorient
import json
from collections import namedtuple

ServerVersion = namedtuple('orientdb_version', ['major', 'minor', 'build'])

class Graph(object):
    def __init__(self, config, user=None, cred=None):
        """Connect to OrientDB graph database, creating the database if
        non-existent.

        :param config: Information on database to which to connect
        :param user: (Optional) Username by which to use database
        :param cred: (Optional) Credential for database username

        :note: user only meaningful when cred also provided.
        """

        self.client = pyorient.OrientDB(config.host, config.port)
        self.client.connect(config.user, config.cred)

        self.config = config

        if config.initial_drop:
            self._last_db = self._last_user = self._last_cred = None
            self.drop()

        db_name = config.db_name
        if db_name:
            self.open(db_name, config.storage, user, cred)

        self.registry = {}
        # Maps property dict from database to added class's property dict
        self.props_from_db = {}

        self.scripts = config.scripts or pyorient.Scripts()

    def open(self, db_name, storage, user=None, cred=None):
        """Open a graph on currently-connected database.

        :param storage: Either 'plocal' or 'memory'
        """
        config = self.config
        config.set_database(db_name, storage)

        # NOTE Odd behaviour; if storage not passed to db_exists, db_create
        # ignores storage
        if not self.client.db_exists(db_name, storage):
            self.client.db_create(db_name
                                  , pyorient.DB_TYPE_GRAPH
                                  , storage)

        if not (user and cred):
            user = config.user
            cred = config.cred
        self._last_user = user
        self._last_cred = cred
        self._last_db = db_name

        cluster_map = self.client.db_open(db_name, user, cred)


        self.server_version = ServerVersion(
            self.client.version.major, self.client.version.minor, self.client.version.build)

        return cluster_map

    def drop(self, db_name=None, storage=None):
        """Drop entire database."""
        config = self.config
        self.client.connect(config.user, config.cred)
        try:
            dropped_db = db_name or config.db_name
            self.client.db_drop(db_name or config.db_name
                                   , storage or config.storage)
        except:
            return False
        finally:
            last_db = self._last_db
            if last_db and last_db is not dropped_db:
                # In case we aren't dropping the currently-configured database,
                # ensure we are still able to use it.
                self.client.db_open(last_db
                                    , self._last_user, self._last_cred)
        return True

    def include(self, registry):
        """Update Graph's registry, when database schema already exists.

        Faster than a full create_all() when it's not required.
        """
        for cls in registry.values():
            db_to_element = {}

            props = sorted([(k,v) for k,v in cls.__dict__.items()
                            if isinstance(v, Property)]
                           , key=lambda p:p[1].instance_idx)
            for prop_name, prop_value in props:
                value_name = prop_value.name
                if value_name:
                    db_to_element[value_name] = prop_name
                    prop_name = value_name
                else:
                    db_to_element[prop_name] = prop_name

                self.guard_reserved_words(prop_name, cls)

            self.props_from_db[cls] = self.create_props_mapping(db_to_element)
            self.init_broker_for_class(cls)
            self.registry[cls.registry_name] = cls

    def create_class(self, cls):
        """Add vertex or edge class to database.

        :param cls: Subclass of type returned by declarative_node
            /declarative_relationship
        """

        cls_name = cls.registry_name

        bases = [base for base in cls.__bases__ if self.valid_element_base(base)]
        if not bases:
            raise TypeError(
                'Unexpected base class(es) in Graph.create_class'
                ' - try the declarative bases')

        extends = None
        if bases[0] is bases[0].decl_root:
            extends = ['V', 'E'][bases[0].decl_type]
        else:
            extends = ','.join([base.registry_name for base in bases])

        #if not self.client.command(
        #    'SELECT FROM ( SELECT expand( classes ) FROM metadata:schema ) WHERE name = "{}"'
        #        .format(cls_name)):
        try:
            self.client.command(
                'CREATE CLASS {0} EXTENDS {1}'.format(cls_name, extends))
        except pyorient.PyOrientSchemaException:
            # Class already exists
            pass

        db_to_element = {}

        props = sorted([(k,v) for k,v in cls.__dict__.items()
                        if isinstance(v, Property)]
                       , key=lambda p:p[1].instance_idx)
        for prop_name, prop_value in props:
            value_name = prop_value.name
            if value_name:
                db_to_element[value_name] = prop_name
                prop_name = value_name
            else:
                db_to_element[prop_name] = prop_name

            self.guard_reserved_words(prop_name, cls)

            class_prop = '{0}.{1}'.format(cls_name, prop_name)

            linked_to = None
            if isinstance(prop_value, LinkedClassProperty):
                type_linked_to = prop_value.linked_to

                linked_to = getattr(type_linked_to, 'registry_name', None)
                if not linked_to:
                    link_bases = getattr(type_linked_to, '__bases__', None)
                    if link_bases and \
                            isinstance(prop_value, LinkedProperty) and \
                            link_bases[0] is Property:
                        linked_to = type_linked_to.__name__

            try:
                self.client.command(
                    'CREATE PROPERTY {0} {1} {2}'
                        .format(class_prop
                                , type(prop_value).__name__
                                , linked_to or ''))
            except pyorient.PyOrientCommandException:
                # Property already exists
                pass

            if prop_value.default is not None:
                if self.server_version >= (2,1,0):
                    self.client.command(
                        'ALTER PROPERTY {0} DEFAULT {1}'
                            .format(class_prop,
                                    PropertyEncoder.encode(prop_value.default)))

            self.client.command(
                    'ALTER PROPERTY {0} NOTNULL {1}'
                        .format(class_prop
                                , str(not prop_value.nullable).lower()))

            self.client.command(
                    'ALTER PROPERTY {} MANDATORY {}'
                        .format(class_prop
                                , str(prop_value.mandatory).lower()))

            self.client.command(
                    'ALTER PROPERTY {} READONLY {}'
                        .format(class_prop
                                , str(prop_value.readonly).lower()))

            # TODO Add support for composite indexes
            if prop_value.indexed:
                try:
                    self.client.command(
                        'CREATE INDEX {0} {1}'
                            .format(class_prop
                                    , 'UNIQUE' if prop_value.unique
                                      else 'NOTUNIQUE'))
                except pyorient.PyOrientIndexException:
                    # Index already exists
                    pass

        self.props_from_db[cls] = self.create_props_mapping(db_to_element)
        self.init_broker_for_class(cls)
        self.registry[cls_name] = cls

    def drop_class(self, cls, ignore_instances=False):
        """
        Drop vertex or edge class from database.

        :param cls: Subclass of type returned by declarative_node
            /declarative_relationship
        :param ignore_instances: Don't throw if class has instances;
        *will* still throw if subclassed.
        """
        if ignore_instances:
            self.client.command(
                'DROP CLASS {} UNSAFE'.format(cls.registry_name))
        else:
            self.client.command(
                'DROP CLASS {}'.format(cls.registry_name))

    def create_all(self, registry):
        """Create classes in database for all classes in registry.

        :param registry: Ordered collection of classes to create, bases first.
        """
        for cls in registry.values():
            self.create_class(cls)

    def drop_all(self, registry):
        """Drop all registry classes from database.

        :param registry: Ordered collection of classes to drop, bases first.
        """
        # Drop subclasses first
        for cls in reversed(list(registry.values())):
            self.drop_class(cls, ignore_instances=True)

    def create_vertex(self, vertex_cls, **kwargs):
        class_name = vertex_cls.registry_name

        if kwargs:
            db_props = self.props_to_db(vertex_cls, kwargs)
            set_clause = ' SET {}'.format(
                ','.join('{}={}'.format(k,PropertyEncoder.encode(v))
                         for k,v in db_props.items()))
        else:
            set_clause = ''

        result = self.client.command(
            'CREATE VERTEX {}{}'.format(class_name, set_clause))[0]

        props = result.oRecordData
        return vertex_cls.from_graph(self, result._rid,
                                     self.props_from_db[vertex_cls](props))

    def create_edge(self, edge_cls, from_vertex, to_vertex, **kwargs):
        class_name = edge_cls.registry_name

        if kwargs:
            db_props = self.props_to_db(vertex_cls, kwargs)
            set_clause = ' SET {}'.format(
                ','.join('{}={}'.format(k,PropertyEncoder.encode(v))
                         for k,v in db_props.items()))
        else:
            set_clause = ''

        result = self.client.command(
            'CREATE EDGE {} FROM {} TO {}{}'.format(
                class_name, from_vertex._id, to_vertex._id, set_clause))[0]

        return self.edge_from_record(result, edge_cls)

    def get_vertex(self, vertex_id):
        record = self.client.command('SELECT FROM {}'.format(vertex_id))
        return self.vertex_from_record(record[0]) if record else None

    def get_edge(self, edge_id):
        record = self.client.command('SELECT FROM {}'.format(edge_id))
        return self.edge_from_record(record[0]) if record else None

    def get_element(self, elem_id):
        record = self.client.command('SELECT FROM {}'.format(elem_id))
        return self.element_from_record(record[0]) if record else None

    def save_element(self, element_class, props, elem_id):
        """:returns: True if successful, False otherwise"""
        if isinstance(element_class, str):
            name = element_class
            element_class = self.registry.get(element_class)
            if not element_class:
                raise KeyError(
                    'Class \'{}\' not registered with graph.'.format(name))

        if props:
            db_props = self.props_to_db(element_class, props)
            set_clause = ' SET {}'.format(
                ','.join('{}={}'.format(k,PropertyEncoder.encode(v))
                         for k,v in db_props.items()))
        else:
            set_clause = ''

        result = self.client.command('UPDATE {}{}'.format(elem_id, set_clause))
        return result and result[0] == b'1'

    def query(self, first_entity, *entities):
        return Query(self, (first_entity,) + entities)

    def gremlin(self, script, args=None, namespace=None):
        script_body = self.scripts.script_body(script, args, namespace)
        if script_body:
            response = self.client.gremlin(script_body)
        else:
            response = self.client.gremlin(script)
        return self.elements_from_records(response)

    # Vertex-centric functions
    def outE(self, from_, *edge_classes):
        """Get outgoing edges from vertex or class.

        :param from_: Vertex id, class, or class name
        :param edge_classes: Filter by these edges
        """
        records = self.client.command('SELECT outE({0}) FROM {1}'
            .format(','.join(self.coerce_class_names(edge_classes))
                    , self.coerce_class_names(from_)))
        return [self.get_edge(e) for e in records[0].oRecordData['outE']] \
            if records else []

    def inE(self, to, *edge_classes):
        """Get edges incoming to vertex or class.

        :param to: Vertex id, class, or class name
        :param edge_classes: Filter by these edges
        """
        records = self.client.command('SELECT inE({0}) FROM {1}'
            .format(','.join(self.coerce_class_names(edge_classes))
                    , self.coerce_class_names(to)))
        return [self.get_edge(e) for e in records[0].oRecordData['inE']] \
            if records else []

    def bothE(self, from_to, *edge_classes):
        """Get outgoing/incoming edges from/to vertex or class.

        :param from_to: Vertex id, class, or class name
        :param edge_classes: Filter by these edges
        """
        records = self.client.command('SELECT bothE({0}) FROM {1}'
            .format(','.join(self.coerce_class_names(edge_classes))
                    , self.coerce_class_names(from_to)))
        return [self.get_edge(e) for e in records[0].oRecordData['bothE']] \
            if records else []

    def out(self, from_, *edge_classes):
        """Get adjacent outgoing vertexes from vertex or class.

        :param from_: Vertex id, class, or class name
        :param edge_classes: Filter by these edges
        """
        records = self.client.command('SELECT out({0}) FROM {1}'
            .format(','.join(self.coerce_class_names(edge_classes))
                    , self.coerce_class_names(from_)))
        return [self.get_vertex(v) for v in records[0].oRecordData['out']] \
            if records else []

    def in_(self, to, *edge_classes):
        """Get adjacent incoming vertexes to vertex or class.

        :param to: Vertex id, class, or class name
        :param edge_classes: Filter by these edges
        """
        records = self.client.command('SELECT in({0}) FROM {1}'
            .format(','.join(self.coerce_class_names(edge_classes))
                    , self.coerce_class_names(to)))
        return [self.get_vertex(v) for v in records[0].oRecordData['in']] \
            if records else []

    def both(self, from_to, *edge_classes):
        """Get adjacent vertexes to vertex or class.

        :param from_to: Vertex id, class, or class name
        :param edge_classes: Filter by these edges
        """
        records = self.client.command('SELECT both({0}) FROM {1}'
            .format(','.join(self.coerce_class_names(edge_classes))
                    , self.coerce_class_names(from_to)))
        return [self.get_vertex(v) for v in records[0].oRecordData['both']] \
            if records else []

    # The following mostly intended for internal use

    def vertex_from_record(self, record, vertex_cls = None):
        if not vertex_cls:
            vertex_cls = self.registry.get(record._class)

        props = record.oRecordData
        return vertex_cls.from_graph(self
             , record._rid
             , self.props_from_db[vertex_cls](props)) if vertex_cls \
            else Vertex.from_graph(self, record._rid, props)

    def vertexes_from_records(self, records):
        return [self.vertex_from_record(record) for record in records]

    def edge_from_record(self, record, edge_cls = None):
        props = record.oRecordData

        in_hash = None
        in_prop = props['in']
        # Currently it is possible to override 'in' and 'out' with custom
        # properties, which breaks inV()/outV()
        if type(in_prop) is pyorient.OrientRecordLink:
            in_hash = in_prop.get_hash()

        out_hash = None
        out_prop = props['out']
        if type(out_prop) is pyorient.OrientRecordLink:
            out_hash = out_prop.get_hash()

        if not edge_cls:
            edge_cls = self.registry.get(record._class)
        return edge_cls.from_graph(self, record._rid
           , in_hash, out_hash
           , self.props_from_db[edge_cls](props)) if edge_cls \
            else Edge.from_graph(self, record._rid, in_hash, out_hash, props)

    def edges_from_records(self, records):
        return [self.edge_from_record(record) for record in records]

    def element_from_record(self, record):
        if not isinstance(record, pyorient.OrientRecord):
            return record

        record_data = record.oRecordData
        try:
            if isinstance(record_data['in'], pyorient.OrientRecordLink) and \
                    isinstance(record_data['out'],
                               pyorient.OrientRecordLink):
                return self.edge_from_record(record)
            else:
                return self.vertex_from_record(record)
        except:
            return self.vertex_from_record(record)

    def elements_from_records(self, records):
        return [self.element_from_record(record) for record in records]

    @staticmethod
    def valid_element_base(cls):
        try:
            return cls.decl_root is not None and cls.decl_type is not None
        except AttributeError:
            return False

    @staticmethod
    def guard_reserved_words(word, cls):
        reserved_words = [[],['in', 'out']][cls.decl_type]
        if word in reserved_words:
            # Should the class also be dropped from the database?
            raise ReservedWordError(
                "'{0}' as a property name will render"
                " class '{1}' unusable.".format(word,
                                                cls.registry_name))

    @staticmethod
    def create_props_mapping(db_to_element):
        return lambda db_props: {
            db_to_element[k]:v for k,v in db_props.items()
                if k in db_to_element }

    @staticmethod
    def props_to_db(element_class, props):
        db_props = {}
        for k, v in props.items():
            prop = element_class.__dict__.get(k)
            if prop:
                db_props[prop.name or k] = v
        return db_props

    @staticmethod
    def coerce_class_names(classes):
        """Get class name(s) for vertexes/edges.

        :param classes: String/class object or list of strings/class objects
        :returns: List if input is iterable, string otherwise
        """
        return [getattr(val, 'registry_name', val) for val in classes] \
            if hasattr(classes, '__iter__') and not isinstance(classes, str) \
            else getattr(classes, 'registry_name', classes)

    def init_broker_for_class(self, cls):
        broker = get_broker(cls)
        if broker:
            broker.init(self, cls)
        else:
            broker = cls.Broker(self, cls)
            setattr(cls, 'objects', broker)

        # Graph will only be assigned the broker for this element class if a
        # 'registry_plural' is set.
        #
        # Otherwise, only the class itself will get a Broker.
        broker_name = getattr(cls, 'registry_plural', None)
        if broker_name and not getattr(cls, 'no_graph_broker', False):
            if hasattr(self, broker_name):
                raise RuntimeError(
                    'Attempt to use a broker name reserved by Graph. '
                    'Could use a different name, or set the \'no_graph_broker\''
                    ' attribute to True for this element class.')
            setattr(self, broker_name, broker)


