from .property import *
from .exceptions import ReservedWordError
from .declarative import DeclarativeMeta, DeclarativeType
from .vertex import Vertex
from .edge import Edge
from .broker import get_broker
from .query import Query
from .traverse import Traverse
from .query_utils import ArgConverter
from .expressions import ExpressionMixin
from .update import Update
from .batch import Batch
from .commands import VertexCommand, CreateEdgeCommand
from ..utils import to_unicode
from .sequence import Sequences
from .mapping import MapperConfig, Decorate
from .what import QT

import pyorient
from collections import namedtuple
from os.path import isfile

import warnings

ServerVersion = namedtuple('orientdb_version', ['major', 'minor', 'build'])

class Graph(object):
    def __init__(self, config, user=None, cred=None, *args, **kwargs):
        """Connect to OrientDB graph database, creating the database if
        non-existent.

        :param config: Information on database to which to connect
        :param user: (Optional) Username by which to use database
        :param cred: (Optional) Credential for database username
        :param args: If supplied, must be MapperConfig instance
        :param kwargs: See MapperConfig

        :note: user only meaningful when cred also provided.
        """

        self.client = pyorient.OrientDB(config.host, config.port, config.serialization_type)
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

        self._scripts = config.scripts
        self._sequences = None

        self._element_cache = lambda cache: None
        mapper_conf = (args and args[0]) or MapperConfig(**kwargs)
        if not mapper_conf.decorate:
            self._prop_decorator = None
            self._generic_props_mapping =  lambda db_props, _: db_props
        else:
            decorate = mapper_conf.decorate

            if decorate == Decorate.Elements:
                self._prop_decorator = decorator = Decorate.iterable
                self._element_cache = lambda cache: cache
            elif decorate == Decorate.Properties:
                self._prop_decorator = decorator = Decorate.property

            self._generic_props_mapping = lambda db_props, cache: {
                k:decorator(v, cache) for k,v in db_props.items() } \
                    if cache is not None else db_props

        self._strict = mapper_conf.strict

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

    def close(self):
        """Close database and destroy the connection."""
        self.client.db_close()

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

    def include(self, registry, *registries):
        """Update Graph's registry, when database schema already exists.

        Faster than a full create_all() when it's not required.
        """
        for reg in (registry,) + registries:
            for cls in reg.values():
                db_to_element = Graph.compute_all_properties(cls)
                self.props_from_db[cls] = Graph.create_props_mapping(db_to_element, self._prop_decorator)
                self.init_broker_for_class(cls)
                self.registry[cls.registry_name] = cls

    def build_mapping(self, vertex, edge, auto_plural=False):
        """Use database schema to dynamically build mapping classes.

        Returns a registry suitable for passing to include()

        :param vertex: Base class for vertexes. Always pass new declarative_node.
        :param edge: Base class for edges. Always pass new declarative_relationship.
        :param auto_plural: If True, will automatically set registry_plural
        on classes. For convenience when include() should set brokers.
        """

        registry = {}

        schema = self.client.command(
            'SELECT FROM (SELECT expand(classes) FROM metadata:schema)'
            ' WHERE name NOT IN [\'ORole\', \'ORestricted\', \'OTriggered\','
            ' \'ORIDs\', \'OUser\', \'OIdentity\', \'OSchedule\', \'OFunction\','
            ' \'OSequence\', \'_studio\']')

        def resolve_class(name, registries):
            for r in registries:
                if name in r:
                    return r[name]
            return None

        def extract_properties(property_schema, is_edge):
            props = {}
            for p in property_schema:
                linked_class = None
                if 'linkedClass' in p:
                    linked_class = resolve_class(p['linkedClass'], registries)

                prop_name = p['name']
                # Special-case in property, mainly on edges
                if is_edge:
                    if prop_name == 'in':
                        prop_name = 'in_'
                    elif prop_name == 'out':
                        prop_name = 'out_'

                props[prop_name] = Graph.property_from_schema(p, linked_class)
            return props

        def expand_properties(class_record):
            # TODO FIXME Integrate this expansion into the above SELECT to avoid round-trips
            class_record['properties'] = [
                self.client.command('SELECT FROM {}'.format(prop))[0].oRecordData if isinstance(prop, pyorient.OrientRecordLink)
                else prop for prop in class_record['properties']
            ]
            return class_record

        if auto_plural:
            def set_vertex_props(props, class_name):
                props['element_plural'] = class_name
                props['registry_plural'] = class_name
                props['element_type'] = class_name
        else:
            def set_vertex_props(props, class_name):
                props['element_type'] = class_name

        def set_edge_props(props, class_name):
            props['label'] = class_name
            props['registry_name'] = class_name

        BASE_PROPS = [(set_vertex_props, False), (set_edge_props, True)]

        # We need to topologically sort classes, since we cannot rely on any ordering
        # in the database. In particular defaultClusterId is set to -1 for all abstract
        # classes. Additionally, superclass(es) can be changed post-create, changing the
        # dependency ordering.
        schema = Graph.toposort_classes([expand_properties(c.oRecordData) for c in schema])
        registries = [registry, self.registry]
        # We will keep properties of non-graph types here, just in case vertex/edge
        # types derive from them.
        non_graph_properties = {}

        for class_def in schema:
            class_name = class_def['name']
            props = {
                # Slightly odd construct ensures class_fields is never None
                'class_fields': class_def.get('customFields', None) or {}
                , 'abstract': class_def.get('abstract', False)
            }
            # Resolve all of the base classes
            base_names = Graph.list_superclasses(class_def)
            bases = []
            is_edge = False

            if base_names:
                base_iter = iter(base_names)
                first_base = next(base_iter)

                if first_base == 'V':
                    bases.append(vertex)
                    props['decl_type'] = vertex.decl_type
                    set_base_props = set_vertex_props
                elif first_base == 'E':
                    bases.append(edge)
                    props['decl_type'] = edge.decl_type
                    is_edge = True
                    set_base_props = set_edge_props
                else:
                    base = resolve_class(first_base, registries)
                    if base:
                        bases.append(base)
                        decl_type = base.decl_type
                        props['decl_type'] = decl_type
                        set_base_props, is_edge = BASE_PROPS[decl_type]
                    else:
                        # Worst-case scenario -- the base is not a graph type
                        props.update(non_graph_properties.get(base_name, {}))
                set_base_props(props, class_name)

                for base_name in base_iter:
                    base = resolve_class(base_name, registries)
                    if base:
                        bases.append(base)
                    else:
                        # Worst-case scenario -- the base is not a graph type
                        props.update(non_graph_properties.get(base_name, {}))

                # Create class for the graph type.
                definitions = registry
                # Shouldn't always assume DeclarativeMeta metaclass when constructing the OGM class
                # inheritance is passed through bases
                define_class = lambda props: type(bases[0])(class_name, tuple(bases), props) 
            else:
                # Otherwise preserve the properties in case a graph type derives from it.
                definitions = non_graph_properties
                define_class = lambda props: props

            props.update(extract_properties(class_def['properties'], is_edge))

            definitions[class_name] = define_class(props)

        return registry

    _GROOVY_GET_DB = \
    '''def db = new com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx("remote:{}:{}/{}");
    db.open("{}", "{}");
    '''

    _GROOVY_NULL_LISTENER = \
    '''def listener = new com.orientechnologies.orient.core.command.OCommandOutputListener() {
        @Override
        public void onMessage(String iText) {}
    };
    '''

    _GROOVY_TRY = \
    '''try {{
    {}
    {}
    }} finally {{
        db.close();
    }}
    '''

    def populate(self, load_path, fmt=None
                 , preserve_cluster_ids=None, delete_rid_mapping=None
                 , merge=None, migrate_links=None, rebuild_indexes=None):
        """Populate graph from a database export file.
           :param fmt: One of 'orientdb', 'graphml', 'graphson'
           :param preserve_cluster_ids: bool. Otherwise temporary cluster ids
           can sometimes fail. Only valid for plocal storage.
           :param delete_rid_mapping: bool. Preserve dictionary mapping old to
           new rids.
           :param merge: bool. Merge with current data, or overwrite.
           :param migrate_links: bool. Update references from old links to new
           rids.
           :param rebuild_indexes: bool. Only disable when import doesn't
           affect indexes.
        """
        if not isfile(load_path):
            return

        import_optionals = ''

        if preserve_cluster_ids is not None:
            import_optionals += 'dbImport.setPreserveClusterIDs({});'.format('true' if preserve_cluster_ids else 'false')
        if delete_rid_mapping is not None:
            import_optionals += 'dbImport.setDeleteRIDMapping({});'.format('true' if delete_rid_mapping else 'false')
        if self.server_version >= (1, 6, 1):
            if merge is not None:
                import_optionals += 'dbImport.setMerge({});'.format('true' if merge else 'false')
            if migrate_links is not None:
                import_optionals += 'dbImport.setMigrateLinks({});'.format('true' if migrate_links else 'false')
            if rebuild_indexes is not None:
                import_optionals += 'dbImport.setRebuildIndexes({});'.format('true' if rebuild_indexes else 'false')

        def_import = \
            'def dbImport = new com.orientechnologies.orient.core.db.tool.ODatabaseImport(db, "{}", listener);'

        import_commands = \
        '''{}
        {}
        dbImport.importDatabase();
        dbImport.close();
        '''.format(def_import.format(load_path), import_optionals)

        config = self.config
        import_groovy = \
            self._GROOVY_GET_DB.format(config.host, config.port, config.db_name, config.user, config.cred) + \
            self._GROOVY_TRY.format(self._GROOVY_NULL_LISTENER, import_commands)

        self.client.gremlin(import_groovy)

    def export(self, save_path, exclude_all=None, include_classes=None
               , exclude_classes=None, include_clusters=None, exclude_clusters=None
               , include_info=None, cluster_definitions=None, schema=None
               , security=None, records=None, index_defs=None
               , manual_indexes=None, compression_level=None, buffer_size=None):
        """Export graph to a (compressed zip) file, without locking the database.
           :param exclude_all: bool. Blacklist everything from export, unless
           included by later parameters.
           :param include_classes: List of strings naming exported classes
           :param exclude_classes: List of strings naming skipped classes
           :param include_clusters: List of strings exported clusters
           :param exclude_clusters: List of strings nameing skipped clusters
           :param include_info: bool. Whether export includes database info
           :param cluster_definitions: bool. Whether export defines clusters
           :param schema: bool. Whether export includes graph schema.
           :param security: bool. Whether export includes DB security params
           :param records: bool. Whether export includes record contents
           :param index_defs: bool. Whether export includes indes definitions
           :param manual_indexes: bool. Whether export includes manual index
           contents
           :param compression_level: In range [0,9], min to max compression.
           :param buffer_size: Compression buffer size, in bytes.
        """
        export_optionals = ''
        options_str = ''
        if exclude_all:
            options_str += ' -excludeAll'

        if include_classes is not None:
            ic = ' '.join(['ic.add("{}");'.format(c) for c in include_classes])
            export_optionals += 'def ic = new HashSet<String>(); {} export.setIncludeClasses(ic);'.format(ic)
        if exclude_classes is not None:
            ec = ' '.join(['ec.add("{}");'.format(c) for c in exclude_classes])
            export_optionals += 'def ec = new HashSet<String>(); {} export.setExcludeClusters(ec);'.format(ec)
        if include_clusters is not None:
            ic = ' '.join(['icx.add("{}");'.format(c) for c in include_clusters])
            export_optionals += 'def icx = new HashSet<String>(); {} export.setIncludeClusters(icx);'.format(ic)
        if exclude_clusters is not None:
            ec = ' '.join(['ecx.add("{}");'.format(c) for c in exclude_clusters])
            export_optionals += 'def ecx = new HashSet<String>(); {} export.setExcludeClusters(ecx);'.format(ec)
        if include_info is not None:
            export_optionals += 'export.setIncludeInfo({});'.format('true' if include_info else 'false')
        if cluster_definitions is not None:
            export_optionals += 'export.setIncludeClusterDefinitions({});'.format('true' if cluster_definitions else 'false')
        if schema is not None:
            export_optionals += 'export.setIncludeSchema({});'.format('true' if schema else 'false')
        if security is not None:
            export_optionals += 'export.setIncludeSecurity({});'.format('true' if security else 'false')
        if records is not None:
            export_optionals += 'export.setIncludeRecords({});'.format('true' if records else 'false')
        if index_defs is not None:
            export_optionals += 'export.setIncludeIndexDefinitions({});'.format('true' if index_defs else 'false')
        if manual_indexes is not None:
            export_optionals += 'export.setIncludeManualIndexes({});'.format('true' if manual_indexes else 'false')
        if self.server_version >= (1, 7, 6):
            # No mutators for these, an attempt to dissuade usage?
            if compression_level is not None:
                options_str += ' -compressionLevel={}'.format(compression_level)
            if buffer_size is not None:
                options_str += ' -compressionBuffer={}'.format(buffer_size)

        def_export = \
            'def export = new com.orientechnologies.orient.core.db.tool.ODatabaseExport(db, "{}", listener);'

        export_commands = \
        '''{}
        {} {}
        export.exportDatabase();
        export.close();
        '''.format(def_export.format(save_path),
                   'export.setOptions("{}");'.format(options_str) if options_str else '',
                   export_optionals)

        config = self.config
        export_groovy = \
            self._GROOVY_GET_DB.format(config.host, config.port, config.db_name, config.user, config.cred) + \
            self._GROOVY_TRY.format(self._GROOVY_NULL_LISTENER, export_commands)

        self.client.gremlin(export_groovy)

    # TODO FIXME
    # def restore(self, load_path):
    #     """Restore graph from backup.
    #        :param load_path: Pass directory path to restore incremental backups
    #     """
    #     pass

    # def backup(self, save_path, incremental=False, compression_level=None, buffer_size=None):
    #     """Backup opened graph to (compressed zip) file.
    #        During a backup, the database remains in read-only mode.
    #
    #        :param incremental: If true, backups changes since the last backup.
    #        :param compression_level: In range [0,9], min to max compression
    #        :param buffer_size: Compression buffer size, in bytes
    #     """
    #     pass


    def clear_registry(self):
        """Clear the registry and associated brokers.

           Useful in preparation for reloading mapping classes from the database
        """
        # Start by removing broker classes from attrs (reverse of init_broker_for_class)
        for k, cls in self.registry.items():
            broker_name = getattr(cls, 'registry_plural', None)
            if (broker_name and not getattr(cls, 'no_graph_broker', False) and
                    hasattr(self, broker_name)):
                delattr(self, broker_name)

        self.registry = {}
        self.props_from_db = {}


    def create_class(self, cls, skip_define=False):
        """Add vertex or edge class to database.

        :param cls: Subclass of type returned by declarative_node
            /declarative_relationship
        """

        cls_name = self.define_class(cls) if not skip_define else cls.registry_name

        pre_ops = [(k, v) for k,v in cls.__dict__.items() if isinstance(v, PreOp)]
        for attr, pre_op in pre_ops:
            pre_op(self, attr)

        props = sorted([(k,v) for k,v in cls.__dict__.items()
                        if isinstance(v, Property)]
                       , key=lambda p:p[1]._instance_idx)
        guard_reserved_words = Graph.get_reserved_validator(cls)
        for prop_name, prop_value in props:
            prop_name = prop_value._name or prop_name

            guard_reserved_words(prop_name)

            # Special case in_ and out_ properties for edges
            if cls.decl_type == DeclarativeType.Edge:
                if prop_name == 'in_':
                    prop_name = 'in'
                elif prop_name == 'out_':
                    prop_name = 'out'

            class_prop = cls_name + '.' + prop_name

            linked_to = None
            if isinstance(prop_value, LinkedClassProperty) and prop_value._linked_to is not None:
                type_linked_to = prop_value._linked_to

                # For now, in case type_linked_to is a Property,
                # need to bypass __getattr__()
                if type_linked_to.__dict__.get('registry_name', None):
                    linked_to = type_linked_to.registry_name
                elif isinstance(prop_value, LinkedProperty):
                    link_bases = type_linked_to.__bases__
                    if link_bases[0] is Property:
                        linked_to = type_linked_to.__name__

            try:
                self.client.command(
                    'CREATE PROPERTY {} {} {}'
                        .format(class_prop
                                , type(prop_value).__name__
                                , linked_to or ''))
            except pyorient.PyOrientCommandException:
                # Property already exists
                pass

            if prop_value._default is not None:
                if self.server_version >= (2,1,0):
                    self.client.command(
                        'ALTER PROPERTY {} DEFAULT {}'
                            .format(class_prop,
                                    ArgConverter.convert_to(ArgConverter.Value,
                                                            prop_value._default,
                                                            ExpressionMixin())))

            if not prop_value._nullable:
                self.client.command(
                        'ALTER PROPERTY {} NOTNULL {}'
                            .format(class_prop
                                    , str(not prop_value._nullable).lower()))

            if prop_value._mandatory:
                self.client.command(
                        'ALTER PROPERTY {} MANDATORY {}'
                            .format(class_prop
                                    , str(prop_value._mandatory).lower()))

            if prop_value._readonly:
                self.client.command(
                        'ALTER PROPERTY {} READONLY {}'
                            .format(class_prop
                                    , str(prop_value._readonly).lower()))

            # TODO Add support for composite indexes
            if prop_value._indexed:
                try:
                    self.client.command(
                        'CREATE INDEX {} {}'
                            .format(class_prop
                                    , 'UNIQUE' if prop_value._unique
                                      else 'NOTUNIQUE'))
                except pyorient.PyOrientIndexException:
                    # Index already exists
                    pass

        # We store all of the properties for the reverse mapping, not only
        # those that are defined directly on the class
        db_to_element = Graph.compute_all_properties(cls)
        self.props_from_db[cls] = Graph.create_props_mapping(db_to_element, self._prop_decorator)
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
                'DROP CLASS ' + cls.registry_name + ' UNSAFE')
        else:
            self.client.command('DROP CLASS ' + cls.registry_name)

    def define_class(self, cls):
        cls_name = cls.registry_name

        bases = [base for base in cls.__bases__ if Graph.valid_element_base(base)]
        try:
            if bases[0] is bases[0].decl_root:
                extends = ['V', 'E'][bases[0].decl_type]
            else:
                extends = ','.join([base.registry_name for base in bases])
        except IndexError:
            raise TypeError(
                'Unexpected base class(es) in Graph.create_class'
                ' - try the declarative bases')

        #if not self.client.command(
        #    'SELECT FROM ( SELECT expand( classes ) FROM metadata:schema ) WHERE name = "{}"'
        #        .format(cls_name)):

        # TODO Batch class/property creation statements?
        try:
            self.client.command(
                'CREATE CLASS {} EXTENDS {}{}'.format(
                    cls_name, extends,
                    " ABSTRACT" if getattr(cls, 'abstract', False) else ''))
        except pyorient.PyOrientSchemaException:
            # Class already exists
            pass

        return cls_name

    def create_all(self, registry, *registries):
        """Create classes in database for all classes in registry.

        :param registry: Ordered collection of classes to create, bases first.
        :param registries: More ordered class collections to create
        """
        for reg in (registry,) + registries:
            for cls in reg.values():
                # Handle case of circular dependencies between class properties
                self.define_class(cls)

        for reg in (registry,) + registries:
            for cls in reg.values():
                self.create_class(cls, skip_define=True)

    def drop_all(self, registry, *registries):
        """Drop all registry classes from database.

        :param registry: Ordered collection of classes to drop, bases first.
        """
        # Drop subclasses first
        for reg in (registry,) + registries:
            for cls in reversed(list(reg.values())):
                self.drop_class(cls, ignore_instances=True)

    def create_vertex(self, vertex_cls, *args, **kwargs):
        result = self.client.command(
            to_unicode(self.create_vertex_command(vertex_cls, *args, **kwargs)))[0]

        props = result.oRecordData
        return vertex_cls.from_graph(self, result._rid,
                                     self.props_from_db[vertex_cls](props, None))

    def create_vertex_command(self, vertex_cls, *args, **kwargs):
        class_name = vertex_cls.registry_name

        expressions = ExpressionMixin()
        return VertexCommand(
            u'CREATE VERTEX {}{}'.format(class_name, self.create_content_clause(vertex_cls, expressions, *args, **kwargs)))

    def delete_vertex(self, vertex, where = None, limit=None, batch=None):
        # TODO FIXME Parse delete result
        result = self.client.command(to_unicode(self.delete_vertex_command(vertex, where, limit, batch)))

    def delete_vertex_command(self, vertex, where=None, limit=None, batch=None):
        vertex_clause = getattr(vertex, 'registry_name', None) or vertex

        delete_clause = ''
        if where is not None:
            where_clause = ''
            if isinstance(where, dict):
                where_clause = u' and '.join(u'{}={}'
                    .format(PropertyEncoder.encode_name(k)
                            , PropertyEncoder.encode_value(v, ExpressionMixin()))
                    for k,v in where.items())
            else:
                where_clause = ExpressionMixin.filter_string(where)

            delete_clause += ' WHERE ' + where_clause
        if limit is not None:
            delete_clause += ' LIMIT ' + str(limit)
        if batch is not None:
            delete_clause += ' BATCH ' + str(batch)

        return VertexCommand(
            u'DELETE VERTEX ' + vertex_clause + delete_clause)

    def create_edge(self, edge_cls, from_vertex, to_vertex, *args, **kwargs):
        result = self.client.command(
            to_unicode(self.create_edge_command(edge_cls
                                     , from_vertex
                                     , to_vertex
                                     , *args
                                     , **kwargs)))[0]

        return self.edge_from_record(result, edge_cls)

    def create_edge_command(self, edge_cls, from_vertex, to_vertex, *args, **kwargs):
        class_name = edge_cls.registry_name

        expressions = ExpressionMixin()
        return CreateEdgeCommand(
            u'CREATE EDGE {} FROM {} TO {}{}'.format(
                class_name,
                ArgConverter.convert_to(ArgConverter.Vertex, from_vertex, expressions),
                ArgConverter.convert_to(ArgConverter.Vertex, to_vertex, expressions),
                self.create_content_clause(edge_cls, expressions, *args, **kwargs)))

    def create_function(self, name, code, parameters=None, idempotent=False, language='javascript'):
        parameter_str = ' PARAMETERS [' + ','.join(parameters) + ']' if parameters else ''

        self.client.command(
            u'CREATE FUNCTION {} \'{}\' {} IDEMPOTENT {} LANGUAGE {}'.format(
                name, code, parameter_str, 'true' if idempotent else 'false', language))

    def get_vertex(self, vertex_id):
        record = self.client.command('SELECT FROM ' + vertex_id)
        try:
            return self.vertex_from_record(record[0])
        except:
            return None

    def get_edge(self, edge_id):
        record = self.client.command('SELECT FROM ' + edge_id)
        try:
            return self.edge_from_record(record[0])
        except:
            return None

    def get_element(self, elem_id):
        record = self.client.command('SELECT FROM ' + elem_id)
        try:
            return self.element_from_record(record[0])
        except:
            return None

    def load_element(self, element_class, elem_id, cache):
        record = self.client.command('SELECT FROM ' + elem_id)
        try:
            return self.props_from_db[element_class](record[0].oRecordData, cache)
        except:
            return {}

    def load_edge(self, element_class, elem_id, cache):
        record = self.client.command('SELECT FROM ' + elem_id)
        try:
            props = record[0].oRecordData
            # Need to heed changes to in/out from UPDATE EDGE
            return self.edge_hashes(props) + (self.props_from_db[element_class](props, cache),)
        except:
            return None

    def save_element(self, element_class, props, elem_id):
        """:return: True if successful, False otherwise"""
        if isinstance(element_class, str):
            name = element_class
            element_class = self.registry.get(element_class)
            if not element_class:
                raise KeyError(
                    'Class \'{}\' not registered with graph.'.format(name))

        if props:
            db_props = Graph.props_to_db(element_class, props, self._strict, skip_if='_readonly')
            set_clause = u' SET ' + \
                u','.join(u'{}={}'.format(
                    PropertyEncoder.encode_name(k), PropertyEncoder.encode_value(v, ExpressionMixin()))
                    for k, v in db_props.items())
        else:
            set_clause = ''

        result = self.client.command(u'UPDATE ' + elem_id + set_clause)
        return result and (result[0] == 1 or result[0] == b'1')

    def query(self, first_entity, *entities):
        return Query(self, (first_entity,) + entities)

    def traverse(self, target, *what):
        return Traverse(self, target, *what)

    def update(self, entity):
        return Update(self, entity)

    def update_edge(self, entity):
        return Update.edge(self, entity)

    def batch(self, isolation_level=Batch.READ_COMMITTED, cache=None, compile=False):
        return Batch(self, isolation_level, cache, compile)

    def gremlin(self, script, args=None, namespace=None):
        script_body = self.scripts.script_body(script, args, namespace)
        if script_body:
            response = self.client.gremlin(script_body)
        else:
            response = self.client.gremlin(script)
        return self.elements_from_records(response)

    @property
    def scripts(self):
        if self._scripts is None:
            self._scripts = pyorient.Scripts()
        return self._scripts

    @property
    def sequences(self):
        if self._sequences is None:
            self._sequences = Sequences(self)
        return self._sequences

    # Vertex-centric functions
    def outE(self, from_, *edge_classes):
        """Get outgoing edges from vertex or class.

        :param from_: Vertex id, class, or class name
        :param edge_classes: Filter by these edges
        """
        records = self.client.query('SELECT EXPAND( outE({}) ) FROM {}'
            .format(','.join(Graph.coerce_class_names_to_quoted(edge_classes))
                    , self.coerce_class_names(from_)), -1)
        return [self.edge_from_record(r) for r in records] \
            if records else []

    def inE(self, to, *edge_classes):
        """Get edges incoming to vertex or class.

        :param to: Vertex id, class, or class name
        :param edge_classes: Filter by these edges
        """
        records = self.client.query('SELECT EXPAND( inE({}) ) FROM {}'
            .format(','.join(Graph.coerce_class_names_to_quoted(edge_classes))
                    , self.coerce_class_names(to)), -1)
        return [self.edge_from_record(r) for r in records] \
            if records else []

    def bothE(self, from_to, *edge_classes):
        """Get outgoing/incoming edges from/to vertex or class.

        :param from_to: Vertex id, class, or class name
        :param edge_classes: Filter by these edges
        """
        records = self.client.query('SELECT EXPAND( bothE({}) ) FROM {}'
            .format(','.join(Graph.coerce_class_names_to_quoted(edge_classes))
                    , self.coerce_class_names(from_to)), -1)
        return [self.edge_from_record(r) for r in records] \
            if records else []

    def out(self, from_, *edge_classes):
        """Get adjacent outgoing vertexes from vertex or class.

        :param from_: Vertex id, class, or class name
        :param edge_classes: Filter by these edges
        """
        records = self.client.query('SELECT EXPAND( out({}) ) FROM {}'
            .format(','.join(Graph.coerce_class_names_to_quoted(edge_classes))
                    , self.coerce_class_names(from_)), -1)
        return [self.vertex_from_record(v) for v in records] \
            if records else []

    def in_(self, to, *edge_classes):
        """Get adjacent incoming vertexes to vertex or class.

        :param to: Vertex id, class, or class name
        :param edge_classes: Filter by these edges
        """
        records = self.client.query('SELECT EXPAND( in({}) ) FROM {}'
            .format(','.join(Graph.coerce_class_names_to_quoted(edge_classes))
                    , self.coerce_class_names(to)), -1)
        return [self.vertex_from_record(v) for v in records] \
            if records else []

    def both(self, from_to, *edge_classes):
        """Get adjacent vertexes to vertex or class.

        :param from_to: Vertex id, class, or class name
        :param edge_classes: Filter by these edges
        """
        records = self.client.query('SELECT EXPAND( both({}) )FROM {}'
            .format(','.join(Graph.coerce_class_names_to_quoted(edge_classes))
                    , self.coerce_class_names(from_to)), -1)
        return [self.vertex_from_record(v) for v in records] \
            if records else []

    # The following mostly intended for internal use

    def create_content_clause(self, element_cls, expressions, *args, **kwargs):
        if args:
            content = args[0]
            # Problematic to accept JSON strings directly, as workflow may put
            # this function's output through str.format()
            if isinstance(content, QT):
                content = expressions.build_token(content)
            else:
                raise TypeError("Token expected for content argument")
            return u' CONTENT ' + content
        elif kwargs:
            db_props = Graph.props_to_db(element_cls, kwargs, self._strict)
            return u' SET {}'.format(
                u','.join(u'{}={}'.format(
                    PropertyEncoder.encode_name(k),
                    ArgConverter.convert_to(ArgConverter.Vertex, v, expressions))
                    for k, v in db_props.items()))
        else:
            return u''


    def vertex_from_record(self, record, vertex_cls=None, cache=None):
        if not vertex_cls:
            vertex_cls = self.registry.get(record._class)

        props = record.oRecordData
        return vertex_cls.from_graph(self
             , record._rid
             , self.props_from_db[vertex_cls](props, cache)
             , self._element_cache(cache)) if vertex_cls \
            else Vertex.from_graph(self, record._rid,
                                   self._generic_props_mapping(props, cache),
                                   self._element_cache(cache))

    def vertexes_from_records(self, records, cache=None):
        return [self.vertex_from_record(record, None, cache) for record in records]

    def edge_from_record(self, record, edge_cls=None, cache=None):
        props = record.oRecordData

        if not edge_cls:
            edge_cls = self.registry.get(record._class)
        in_hash, out_hash = self.edge_hashes(props)
        return edge_cls.from_graph(self, record._rid
           , in_hash, out_hash
           , self.props_from_db[edge_cls](props, cache)
           , self._element_cache(cache)) if edge_cls \
            else Edge.from_graph(self, record._rid, in_hash, out_hash,
                                 self._generic_props_mapping(props, cache),
                                 self._element_cache(cache))

    def edge_hashes(self, props):
        try:
            in_hash, out_hash = props['in'].get_hash(), props['out'].get_hash()
        except AttributeError:
            return None, None
        else:
            return in_hash, out_hash

    def edges_from_records(self, records, cache=None):
        return [self.edge_from_record(record, none, cache) for record in records]

    def element_from_record(self, record, cache=None):
        if not isinstance(record, pyorient.OrientRecord):
            return record

        record_data = record.oRecordData
        try:
            if isinstance(record_data['in'], pyorient.OrientRecordLink) and \
                    isinstance(record_data['out'],
                               pyorient.OrientRecordLink):
                return self.edge_from_record(record, None, cache)
            else:
                return self.vertex_from_record(record, None, cache)
        except:
            return self.vertex_from_record(record, None, cache)

    def elements_from_records(self, records, cache=None):
        return [self.element_from_record(record, cache) for record in records]

    def element_from_link(self, link, cache=None):
        if cache is not None:
            try:
                return cache[link]
            except KeyError:
                warnings.warn('Cache miss on link ' + link.get_hash(), RuntimeWarning)
        if link.is_temporary():
            return link
        return self.get_element(link.get_hash())

    def elements_from_links(self, links, cache=None):
        return [self.element_from_link(link, cache) for link in links]

    def parse_record_prop(self, prop, cache):
        # Invariant: Input and output collection types match
        if isinstance(prop, list):
            if len(prop) > 0 and isinstance(prop[0], pyorient.OrientRecordLink):
                return self.elements_from_links(prop, cache) 
        elif isinstance(prop, pyorient.OrientRecordLink):
            return self.element_from_link(prop, cache)
        return prop

    PROPERTY_TYPES = {
        0:Boolean
        , 1:Integer
        , 2:Short
        , 3:Long
        , 4:Float
        , 5:Double
        , 6:DateTime
        , 7:String
        , 8:Binary
        , 9:Embedded
        , 10:EmbeddedList
        , 11:EmbeddedSet
        , 12:EmbeddedMap
        , 13:Link
        , 14:LinkList
        , 15:LinkSet
        , 16:LinkMap
        , 17:Byte
        , 19:Date
        , 21:Decimal
    }

    @classmethod
    def property_from_schema(cls, prop_def, linked_class=None):
        prop_type = cls.PROPERTY_TYPES.get(prop_def['type'], Property)

        property_params = {
            'nullable': not prop_def['notNull'],
            'default': prop_def.get('defaultValue', None),
            'indexed': False, # FIXME
            'unique': False, # FIXME
            'mandatory': prop_def['mandatory'],
            'readonly': prop_def['readonly']
        }

        if linked_class:
            property_params['linked_to'] = linked_class

        return prop_type(**property_params)


    @staticmethod
    def valid_element_base(cls):
        try:
            return cls.decl_root is not None and cls.decl_type is not None
        except AttributeError:
            return False

    @staticmethod
    def guard_reserved_words(word):
        if word == 'in' or word == 'out':
            # Should the class also be dropped from the database?
            raise ReservedWordError(
                "'{0}' as a property name will render"
                " class '{1}' unusable.".format(word,
                                                cls.registry_name))

    @staticmethod
    def get_reserved_validator(cls):
        if cls.decl_type == DeclarativeType.Edge:
            return Graph.guard_reserved_words
        else:
            return lambda word: None

    @staticmethod
    def create_props_mapping(db_to_element, wrapper):
        def mapped_key(db_props):
            for k,v in db_props.items():
                mapped = db_to_element.get(k, None)
                if mapped is not None:
                    yield mapped,v

        if wrapper is not None:
            return lambda db_props, cache: {
                k:wrapper(v, cache) for k,v in mapped_key(db_props) } \
                    if cache is not None else dict(mapped_key(db_props))
        else:
            return lambda db_props, _: dict(mapped_key(db_props))

    @staticmethod
    def props_to_db(element_class, props, strict, skip_if=None):
        db_props = {}
        for k, v in props.items():
            # sanitize the property name -- this line
            # will raise an error if the name is invalid
            PropertyEncoder.encode_name(k)

            if hasattr(element_class, k):
                prop = getattr(element_class, k)
                if skip_if is not None and getattr(prop, skip_if, False):
                    continue
                db_props[prop._name or k] = v
            elif strict:
                raise AttributeError('Class {} has no property {}'.format(
                    element_class, k))
            # if we are not in the strict mode, swallow missing properties
        return db_props

    @staticmethod
    def compute_all_properties(cls):
        """Compute all properties (including the inherited ones) for the class """
        all_properties = {}

        props = []
        # Get all of the properties of the class including inherited ones
        for m in dir(cls):
            p = getattr(cls, m)
            if isinstance(p, Property):
                props.append((m, p))

        props = sorted(props, key=lambda p:p[1]._instance_idx)

        guard_reserved_words = Graph.get_reserved_validator(cls)
        for prop_name, prop_value in props:
            value_name = prop_value._name or prop_name
            all_properties[value_name] = prop_name
            guard_reserved_words(value_name)

        return all_properties

    @staticmethod
    def coerce_class_names(classes):
        """Get class name(s) for vertexes/edges.

        :param classes: String/class object or list of strings/class objects
        :return: List if input is iterable, string otherwise
        """
        return [getattr(val, 'registry_name', val) for val in classes] \
            if hasattr(classes, '__iter__') and not isinstance(classes, str) \
            else getattr(classes, 'registry_name', classes)

    @staticmethod
    def coerce_class_names_to_quoted(classes):
        """Get the quoted class name(s) for vertexes/edges. Useful when passing them to some operators"""
        names = Graph.coerce_class_names(classes)
        return [repr(name) for name in names] if names else []

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

    @staticmethod
    def toposort_classes(classes):
        """Sort class metadatas so that a superclass is always before the subclass"""
        def get_class_topolist(class_name, name_to_class, processed_classes, current_trace, superclass=True):
            """Return a topologically sorted list of this class's dependencies and class itself

            :param class_name: name of the class to process
            :param name_to_class: a map from class name to the descriptor
            :param processed_classes: a set of classes that have already been processed
            :param current_trace: list of classes traversed during the recursion.

            :return: element of classes list sorted in topological order
            """
            # Check if this class has already been handled
            if class_name in processed_classes:
                return []

            if class_name in current_trace:
                if superclass:
                    raise AssertionError(
                        'Encountered self-reference in dependency chain of {}'.format(class_name))
                return []

            cls = name_to_class[class_name]

            class_list = []
            # Recursively process class dependencies
            # These are bases...
            current_trace.add(class_name)
            for dependency in Graph.list_superclasses(cls):
                class_list.extend(get_class_topolist(
                    dependency, name_to_class, processed_classes, current_trace))

            # ...and classes from linked edges
            properties = cls['properties'] if 'properties' in cls else []
            for prop in properties:
                if 'linkedClass' in prop:
                    class_list.extend(get_class_topolist(
                        prop['linkedClass'], name_to_class, processed_classes, current_trace, superclass=False))
            current_trace.remove(class_name)
            # Do the bookkeeping
            class_list.append(name_to_class[class_name])
            processed_classes.add(class_name)

            return class_list

        # Map names to classes
        class_map = {c['name']: c for c in classes}
        seen_classes = set()

        toposorted = []
        for name in class_map.keys():
            toposorted.extend(get_class_topolist(name, class_map, seen_classes, set([])))
        return toposorted

    @staticmethod
    def list_superclasses(class_def):
        superclasses = class_def.get('superClasses', [])
        if superclasses:
            # Make sure to duplicate the list
            return list(superclasses)

        sup = class_def.get('superClass', None)
        if sup:
            return [sup]
        else:
            return []

