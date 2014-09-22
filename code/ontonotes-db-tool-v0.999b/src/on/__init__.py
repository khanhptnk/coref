#!/usr/bin/env python

# COPYRIGHT  2007-2010 BY BBN TECHNOLOGIES CORP.

# BY USING THIS SOFTWARE THE USER EXPRESSLY AGREES: (1) TO BE BOUND BY
# THE TERMS OF THIS AGREEMENT; (2) THAT YOU ARE AUTHORIZED TO AGREE TO
# THESE TERMS ON BEHALF OF YOURSELF AND YOUR ORGANIZATION; (3) IF YOU OR
# YOUR ORGANIZATION DO NOT AGREE WITH THE TERMS OF THIS AGREEMENT, DO
# NOT CONTINUE.  RETURN THE SOFTWARE AND ALL OTHER MATERIALS, INCLUDING
# ANY DOCUMENTATION TO BBN TECHNOLOGIES CORP.

# BBN GRANTS A NONEXCLUSIVE, ROYALTY-FREE RIGHT TO USE THIS SOFTWARE
# KNOWN AS THE OntoNotes DB Tool v. 0.9 (HEREINAFTER THE "SOFTWARE")
# SOLELY FOR RESEARCH PURPOSES. PROVIDED, YOU MUST AGREE TO ABIDE BY THE
# LICENSE AND TERMS STATED HEREIN. TITLE TO THE SOFTWARE AND ITS
# DOCUMENTATION AND ALL APPLICABLE COPYRIGHTS, TRADE SECRETS, PATENTS
# AND OTHER INTELLECTUAL RIGHTS IN IT ARE AND REMAIN WITH BBN AND SHALL
# NOT BE USED, REVEALED, DISCLOSED IN MARKETING OR ADVERTISEMENT OR ANY
# OTHER ACTIVITY NOT EXPLICITLY PERMITTED IN WRITING.

# NO WARRANTY. THE SOFTWARE IS PROVIDED "AS IS" WITHOUT WARRANTY OF ANY
# KIND.  THE SOFTWARE IS PROVIDED for RESEARCH PURPOSES ONLY. AS SUCH,
# IT MAY CONTAIN ERRORS, WHICH COULD CAUSE FAILURES OR LOSS OF DATA. TO
# THE MAXIMUM EXTENT PERMITTED BY LAW, BBN MAKES NO WARRANTIES, EXPRESS
# OR IMPLIED AS TO THE SOFTWARE, ITS CAPABILITIES OR FUNCTIONALITY,
# INCLUDING WITHOUT LIMITATION THE IMPLIED WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE, NONINFRINGEMENT, OR
# ANY USE OF THE SOFTWARE. THE USER ASSUMES THE ENTIRE COST OF ALL
# NECESSARY REPAIR OR CORRECTION, EVEN IF BBN HAS BEEN ADVISED OF THE
# POSSIBILITY OF SUCH A DEFECT OR DAMAGES. BBN MAKES NO WARRANTY THAT
# THE SOFTWARE WILL MEET THE USER REQUIREMENTS, OR WILL BE
# UNINTERRUPTED, TIMELY, SECURE, OR ERROR-FREE.

# LIMITATION OF LIABILITY. THE ENTIRE RISK AS TO THE RESULTS AND
# PERFORMANCE OF THE SOFTWARE IS ASSUMED BY THE USER. TO THE MAXIMUM
# EXTENT PERMITTED BY APPLICABLE LAW, BBN SHALL NOT BE LIABLE WITH
# RESPECT TO ANY SUBJECT MATTER OF THIS AGREEMENT UNDER ANY CONTRACT,
# NEGLIGENCE, STRICT LIABILITY OR OTHER THEORY FOR ANY DIRECT,
# CONSEQUENTIAL, RELIANCE, INCIDENTAL, SPECIAL, DIRECT OR INDIRECT
# DAMAGES WHATSOEVER (INCLUDING WITHOUT LIMITATION, DAMAGES FOR LOSS OF
# BUSINESS PROFITS, OR BUSINESS INFORMATION, OR FOR BUSINESS
# INTERRUPTION, PERSONAL INJURY OR ANY OTHER LOSSES) RELATING TO (A)
# LOSS OR INACCURACY OF DATA OR COST OF PROCUREMENT OF SUBSTITUTE
# SYSTEM, SERVICES OR TECHNOLOGY, (B) THE USE OR INABILITY TO USE THE
# SOFTWARE; (C) UNAUTHORIZED ACCESS TO OR ALTERATION OF YOUR
# TRANSMISSIONS OR DATA; (D) ANY PERSONAL INJURY OR INJURY TO PROPERTY;
# OR (E) ANY OTHER USE OF THE SOFTWARE EVEN IF BBN HAS BEEN FIRST
# ADVISED OF THE POSSIBILITY OF ANY SUCH DAMAGES OR LOSSES.

# WITHOUT LIMITATION OF THE FOREGOING, THE USER AGREES TO COMMIT NO ACT
# WHICH, DIRECTLY OR INDIRECTLY, WOULD VIOLATE ANY U.S. LAW, REGULATION,
# OR TREATY, OR ANY OTHER INTERNATIONAL TREATY OR AGREEMENT TO WHICH THE
# UNITED STATES ADHERES OR WITH WHICH THE UNITED STATES COMPLIES,
# RELATING TO THE EXPORT OR RE-EXPORT OF ANY COMMODITIES, SOFTWARE, OR
# TECHNICAL DATA.

"""
-----------------------------
:mod:`on` -- Top level module
-----------------------------

See:

 - :class:`on.ontonotes`
 - :mod:`on.common`
 - :mod:`on.corpora`
 - :mod:`on.tools`

Class:

  .. autoclass:: on.ontonotes

"""
#---- standard python imports ----#
from __future__ import with_statement

import operator
import os.path
import string
import sys
import re
import exceptions
import math
import os
import time
import getopt
import zlib
import gzip
import codecs
import commands
import traceback
import weakref

from xml.etree import ElementTree
import xml.etree.cElementTree as ElementTree


VERSION="v0.999b r6778 (4.0 release)"

#---- custom package imports ----
import on

import on.corpora
import on.corpora.tree
import on.corpora.proposition
import on.corpora.coreference
import on.corpora.name
import on.corpora.sense
import on.corpora.parallel
import on.corpora.speaker
import on.corpora.ontology


import on.common.util
from on.common.util import NotInConfigError
from on.common.util import listdir_full, listdir
from on.common.util import insert_ignoring_dups, register_config

import on.common.log

try:
    import MySQLdb
except ImportError:
    on.common.log.warning("Unable to import MySQLdb.  Will not be able to write"
                          + " or read from a database, but otherwise should be"
                          + " functional.")

class ontonotes:
    """ This is the main OntoNotes class that serves as a wrapper
    class for all the OntoNotes sub-components such as Propositions,
    Syntactic Trees, Coreference Names, Word Senses. etc.

    There is generally one top-level ontonotes object.  It contains
    one or more :class:`on.corpora.subcorpus` instances.  Each of those
    holds a :class:`on.corpora.tree.treebank` and
    :class:`on.corpora.document_bank` as well as possibly other banks.

    To create an ontonotes object, pass in a config object (the output
    of one of :func:`on.common.util.load_config`,
    :func:`on.common.util.load_options`, or
    :func:`on.common.util.FancyConfigParser` ).

    If you have an ontonotes object and want to iterate over the
    subcorpora it contains, use code like:

    .. code-block:: python

       for a_subcorpus in a_ontonotes:
          pass

    This will load the banks for each subcorpus in turn, allowing each
    to be garbage collected when there are no more references and so
    not using up all your memory.  If you have lots of memory and
    either don't want to load each bank repeatedly or need to do
    something that works with all the subcorpora at once, you could do
    something like:

    .. code-block:: python

       a_subcorpus_list = []
       for a_subcorpus in a_ontonotes:
          a_subcorpus_list.append(a_subcorpus)

       compute_cross_subcorpus_statistics(a_subcorpus_list)

    By keeping references to the subcorpora in ``a_subcorpus_list`` as
    we load them, we keep them from being garbage collected.

    You can also pull out single subcorpus if you have it's id:

    .. code-block:: python

       a_subcorpus = a_ontonotes.get_subcorpus(a_subcorpus_id, banks_loaded=True)

    If you had instead set ``banks_loaded`` to ``False`` you would
    have a_subcorpus that represented just a simple colection of
    files without even the trees loaded to memory.


    Methods:

      .. automethod:: get_subcorpus
      .. automethod:: get_db_cursor
      .. automethod:: initialize_db
      .. automethod:: write_type_tables_to_db
      .. automethod:: write_to_db

    """

    database = ""

    @register_config("corpus", "data_in")
    @register_config("corpus", "load")
    @register_config("corpus", "banks")
    @register_config("corpus", "prefix")
    @register_config("corpus", "suffix")
    @register_config("corpus", "granularity", allowed_values=["source", "section", "file"])
    @register_config("corpus", "max_files")
    @register_config("corpus", "hide_errors", allowed_values=["true", "false"])
    @register_config("db", "host", required=True)
    @register_config("db", "db", required=True)
    @register_config("db", "user", required=True)
    def __init__(self, config, data_source="auto", hide_errors=None):
        """ data_source -- one of 'auto', 'files', or 'db'.  If 'auto', go by whether the config
                           file has 'corpus.data_in' or a 'db' section.

        """

        self.config = config

        self.id = self.config_opt("id", "on")
        self.subcorpus_id_list = []
        self.subcorpus_hash = {}

        self._loaded_subcorpora_cache = weakref.WeakValueDictionary() # used only by get_subcorpus
        self._dont_lose_subcorpora_list = [] # used only by get_subcorpus

        if data_source == "auto":
            if self.config_has_opt("data_in"):
                data_source = "files"
            elif self.config.has_section("db"):
                data_source = "db"
            else:
                raise Exception("Must specify either corpus.data_in or db.*")

        if hide_errors is None:
            hide_errors = (self.config_opt("hide_errors", default="false") == "true")


        if data_source == "files":
            self._from_files()
        elif data_source == "db":
            self._from_db()
        else:
            raise Exception("data_source must be 'files', 'db', or 'auto' -- given %r" % data_source)

        for a_subcorpus in self.subcorpus_hash.itervalues():
            a_subcorpus._hide_errors = hide_errors

    def config_has_opt(self, key):
        return self.config.has_option("corpus", key)

    def config_opt(self, key, default=None):
        if default is None or self.config_has_opt(key):
            return self.config.get("corpus", key)
        return default

    def __getitem__(self, index):
        subcorpus_index = self.subcorpus_id_list[index]
        try:
            a_subcorpus = self.get_subcorpus(subcorpus_index, banks_loaded=True)
        except IndexError:
            raise Exception(traceback.format_exc())
        return a_subcorpus

    def __len__(self):
        return len(self.subcorpus_id_list)

    def __repr__(self):
        return "ontonotes instance, id=%s, subcorpora:" % self.id + "\n" + on.common.util.repr_helper(enumerate(self.subcorpus_id_list))

    def add_subcorpus(self, a_subcorpus):
        """ Hold onto this subcorpus.

        Generally not needed.

        """

        if a_subcorpus.id not in self.subcorpus_hash:
            self.subcorpus_id_list.append(a_subcorpus.id)
            self.subcorpus_hash[a_subcorpus.id] = a_subcorpus
        else:
            raise Exception("Ontonotes object already contained a subcorpus with id %s; did you mean update_subcorpus?" % a_subcorpus.id)


    def update_subcorpus(self, a_subcorpus):
        """ Replace a previously added subcorpus with this new one.

        Generally not needed

        """

        try:
            self.subcorpus_hash[a_subcorpus.id] = a_subcorpus
        except KeyError:
            raise Exception("Could not find the subcorpus with id %s; did you mean add_subcorpus?" % a_subcorpus.id)



    def delete_subcorpus(self, a_subcorpus):
        """ Delete a subcorpus.

        Generally not needed

        """

        a_subcorpus_id = a_subcorpus
        if hasattr(a_subcorpus, "id"):
            a_subcorpus_id = a_subcorpus.id

        try:
            del self.subcorpus_hash[a_subcorpus_id]
            del self.subcorpus_id_list[self.subcorpus_id_list.index(a_subcorpus_id)]
        except KeyError:
            raise Exception("Could not find the subcorpus with id %s" % a_subcorpus_id)


    def get_subcorpus(self, a_subcorpus_id, banks_loaded=False, use_cache=True, save_this=False):
        """ Get the subcorpus with this id

        a_subcorpus_id:

         - the id of the desired subcorpus

        banks_loaded:

         - whether to load the banks for this subcorpus and enrich
           the treebank with the other banks

        use_cache:

         - whether to notice if we've returned this subcorpus before
           and if so not load it again.  If you've changed
           configuration values or want another copy of the same
           subcorpus you should unset this. Note that even if this is
           false, we still add to the cache.

        save_this:

         - Sometimes loading one subcorpus will trigger loading a
           different one.  This happens, currently only, when loading
           parallel banks.  In these cases, a subcorpus will be loaded
           to memory which should then stay in memory until next
           requested.  If set to ``True``, then, ``save_this`` will
           hold onto the created subcorpus until that subcorpus is
           next requested.  This variable has no effect if
           ``banks_loaded`` or ``use_cache`` is ``False``

        """

        try:
            a_subcorpus = self.subcorpus_hash[a_subcorpus_id]
        except KeyError:
            raise KeyError("Could not find the subcorpus with id %s" % a_subcorpus_id)

        assert a_subcorpus.id == a_subcorpus_id

        if not banks_loaded:
            return a_subcorpus

        a_subcorpus_copy = None
        if use_cache:
            try:
                a_subcorpus_copy = self._loaded_subcorpora_cache[a_subcorpus_id]
            except KeyError:
                pass

        if not a_subcorpus_copy:
            a_subcorpus_copy = a_subcorpus.copy()

            try:
                a_subcorpus_copy.load_banks(self.config)
            except Exception:
                if a_subcorpus._hide_errors:
                    on.common.log.report("loading", "hid error SERIOUS", id=a_subcorpus_id)
                else:
                    raise

        assert a_subcorpus_copy.id == a_subcorpus_id, "%s, %s" % (a_subcorpus_copy.id, a_subcorpus_id)

        self._loaded_subcorpora_cache[a_subcorpus_id] = a_subcorpus_copy

        if save_this:
            self._dont_lose_subcorpora_list.append(a_subcorpus_copy)
        else:
            if a_subcorpus_copy in self._dont_lose_subcorpora_list:
                self._dont_lose_subcorpora_list.remove(a_subcorpus_copy)

        return a_subcorpus_copy

    ## Get the database cursor
    #
    @staticmethod
    def db_cursor(config):
        """ calls get_db_cursor with db.{db,host,user} """

        return ontonotes.get_db_cursor(config["db", "db"],
                                       config["db", "host"],
                                       config["db", "user"])

    @staticmethod
    def _get_db_cursor(a_db, a_host, a_user, connections={}):
        """ don't use this -- use :meth:``db_cursor`` or
        :meth:``get_db_cursor`` instead

        Note that the connections hash persists between calls.  This
        is intentional and means we don't open identical connections
        """

        if not (a_db, a_host, a_user) in connections:
            try:
                connections[a_db, a_host, a_user] = MySQLdb.connect(host=a_host, db=a_db, user=a_user, charset="utf8")
            except MySQLdb.Error, e:
                on.common.log.error("%s\n%s%s\n%s%s" % (
                    "cannot connect to database server.",
                    "error code    : ", str(e.args[0]),
                    "error message : ", str(e.args[1])))
            on.common.log.debug("connected to %s database" % (a_db), on.common.log.DEBUG, on.common.log.MIN_VERBOSITY)

        return connections[a_db, a_host, a_user].cursor(MySQLdb.cursors.DictCursor)

    @classmethod
    def get_db_cursor(cls, a_db, a_host, a_user):
        """

        Parameters:

          - ``a_db`` -- The name of the database to connect to
          - ``a_host`` -- The name of the server the database is hosted on
          - ``a_user`` -- The username to use.  Needs to have no password.

        Returns a :class:`MySQLdb.cursors.DictCursor`.

        """

        return cls._get_db_cursor(a_db, a_host, a_user)

    #---- these are some database specific class variables -----#

    sql_table_name = "ontonotes"
    sql_create_statement = \
"""
 create table ontonotes
(
   id varchar(255) not null primary key

)
default character set utf8;
"""
    sql_insert_statement = \
"""
insert into ontonotes
(
  id
) values (%s)"""

    all_normal_classes = [on.corpora.subcorpus,
                          on.corpora.file, on.corpora.tree.tree,
                          on.corpora.tree.lemma,
                          on.corpora.coreference.coreference_chain,
                          on.corpora.coreference.coreference_link,
                          on.corpora.sense.on_sense,
                          on.corpora.proposition.predicate,
                          on.corpora.proposition.predicate_node,
                          on.corpora.proposition.argument,
                          on.corpora.proposition.argument_node,
                          on.corpora.proposition.link,
                          on.corpora.proposition.link_node,
                          on.corpora.name.name_entity,
                          on.corpora.proposition.argument_composition,
                          on.corpora.tree.syntactic_link,
                          on.corpora.tree.compound_function_tag,
                          on.corpora.document, on.corpora.sentence,
                          on.corpora.token, on.corpora.tree.treebank,
                          on.corpora.document_bank,
                          on.corpora.proposition.proposition_bank,
                          on.corpora.sense.sense_bank,
                          on.corpora.coreference.coreference_bank,
                          on.corpora.name.name_bank,
                          on.corpora.ontology.concept,
                          on.corpora.ontology.sense_pool,
                          on.corpora.proposition.proposition,
                          on.corpora.parallel.parallel_document,
                          on.corpora.parallel.parallel_sentence,
                          on.corpora.speaker.speaker_sentence]

    all_open_type_tables = [ on.corpora.sense.wn_sense_type,
                             on.corpora.sense.pb_sense_type,
                             on.corpora.sense.on_sense_type,
                             on.corpora.sense.on_sense_lemma_type,
                             on.corpora.tree.lemma_type ]

    all_closed_type_tables = [ on.corpora.language_type,

                               on.corpora.tree.phrase_type,
                               on.corpora.tree.pos_type,
                               on.corpora.tree.syntactic_link_type,
                               on.corpora.tree.function_tag_type,

                               on.corpora.proposition.predicate_type,
                               on.corpora.proposition.argument_type,
                               on.corpora.proposition.link_type,

                               on.corpora.name.name_entity_type,

                               on.corpora.coreference.coreference_link_type,
                               on.corpora.coreference.coreference_chain_type ]

    all_ontology_type_tables = [ on.corpora.ontology.concept_type,
                                 on.corpora.ontology.sense_pool_type,
                                 on.corpora.ontology.feature_type ]



    @classmethod
    def initialize_db(cls, cursor):
        """ Create all the required database tables using the sql
        statements specified in those classes.  Tables that are
        already present are wiped.

        Cursor should be the output of the function :func:`get_db_cursor`

        Currently, since the cursor can only execute one sql statement
        at a time, we cannot check and drop if a table exists, and then
        create it in one call.  As no one else is concurrently
        modifying the database this should not be an issue.

        """

        on.common.log.status("Initializing DB...")

        table_names = []

        for thing in [on.ontonotes] + cls.all_normal_classes + cls.all_open_type_tables + cls.all_closed_type_tables + cls.all_ontology_type_tables:
            if hasattr(thing, "sql_table_name"):
                cursor.execute("""drop table if exists %s;""" % thing.sql_table_name)

                try:
                    cursor.execute(thing.sql_create_statement)
                except Exception:
                    print thing.sql_create_statement
                    raise

                table_names.append(thing.sql_table_name)

        #---- create tables with inconsistent naming ----#

        for table_name, sql_create_stmt in \
            [("on_sense_type_pb_sense_type", on.corpora.sense.on_sense_type.pb_sql_create_statement),
             ("on_sense_type_wn_sense_type", on.corpora.sense.on_sense_type.wn_sql_create_statement),
             ("concept_pool_parent", on.corpora.ontology.concept.parent_sql_create_statement),
             ("concept_pool_relation", on.corpora.ontology.concept.relation_sql_create_statement),
             ("concept_pool_feature", on.corpora.ontology.concept.feature_sql_create_statement),
             ("pool_sense", on.corpora.ontology.sense_pool.sense_sql_create_statement)]:

            cursor.execute("""drop table if exists %s;""" % table_name)

            try:
                cursor.execute(sql_create_stmt)
            except Exception:
                print table_name
                raise

            table_names.append(table_name)

    @staticmethod
    def write_type_tables_to_db(a_cursor, write_closed_type_tables=False):
        """ Call this after loading everything to the database that
        you intend to load unless you're using

        write_closed_type_tables should be set to True if this is the
        first time you're calling this method.  If you're calling this
        method more than that, it's faster to set it to false.  It
        controls whether we populate the closed type tames (such as
        syntactic_link_type) with allowed values or just leave them as
        they are.

        """


        sys.stderr.write("writing the type tables to db...")
        on.corpora.abstract_type_table.write_to_db(a_cursor, write_closed_type_tables=write_closed_type_tables)
        sys.stderr.write("done.\n")


    ## Write the contents of this ontonotes object to the database
    #
    #  @param self
    #  @param cursor The database cursor to be used for processing
    def write_to_db(self, a_cursor):
        insert_ignoring_dups(self, a_cursor, self.id)

        #---- write contained objects to database ----#
        #---- write the subcorpus table to db ----#
        for a_subcorpus in self.subcorpus_hash.itervalues():
            a_subcorpus.write_to_db(a_cursor)

        self.write_type_tables_to_db(a_cursor)

    ## Dump the contents of the table that represents the ontonotes
    #  object
    #
    #  @param self
    #  @param cursor The database cursor to be used for processing
    #
    def dump_db_table(self, cursor):
        print "\n"*5
        print "-"*80
        print " "*5, "description and contents of the ontonotes table", " "*30
        print "-"*80
        #---- just check if the table was created ----#
        cursor.execute("""show tables;""")
        print cursor.fetchall()

        #---- and print its description ----#
        cursor.execute("""describe ontonotes;""")
        print cursor.fetchall()

        #---- and print its contents ----#
        cursor.execute("""select * from ontonotes;""")
        rows = cursor.fetchall()

        for row in rows:
            print "id: %s" % (row["id"])
            print "."*40
        print "number of rows returned: %s" % (cursor.rowcount)


    def _from_db(self):
        """ use :meth:``__init__`` instead (with ``db.*`` set) """

        a_cursor = self.db_cursor(self.config)

        subcorpora = self.config_opt("load", "").split()

        def make_id_with_ats(s):
            if "@" in s:
                return s
            bits = s.split("-")
            bits[0] = bits[0][:2] # language id
            bits.reverse()
            bits.append(self.id)
            return "@".join(bits)

        def accept_id(sc_id):
            for sc in subcorpora:
                if sc_id.endswith(make_id_with_ats(sc)):
                    return True
            return False

        prefix, suffix = self._interpret_affixes()

        for sc_id, sc_init_info in on.corpora.subcorpus.subcorpora_in_db(a_cursor, self.id):
            a_subcorpus = on.corpora.subcorpus(self, sc_init_info, cursor=a_cursor, old_id=sc_id)
            assert a_subcorpus.id == sc_id

            if any(a_subcorpus.id.endswith(make_id_with_ats(sc)) for sc in subcorpora):
                a_cursor.execute("select id from document where subcorpus_id = '%s'" % a_subcorpus.id)

                if any(on.common.util.matches_an_affix(row["id"], (prefix, suffix)) for row in a_cursor.fetchall()):
                    """ only read this subcorpus if some document in it matches the prefix/suffix """
                    self.add_subcorpus(a_subcorpus)

    def _interpret_affixes(self):
        prefix = []
        suffix = []

        for affix, affix_str in [(prefix, "prefix"),
                                 (suffix, "suffix")]:
            if self.config_has_opt(affix_str):
                affix += self.config_opt(affix_str).split()

        return prefix, suffix


    def _from_files(self):
        """ use :meth:``__init__`` instead (with ``corpus.data_in`` set) """


        data_in = self.config_opt("data_in")
        granularity = self.config_opt("granularity", "file")
        extensions = self.config_opt("banks", "parse").replace(",", " ").split()
        extensions = [x.split(":")[0] for x in extensions] # if an extension is like auto_coref:parse, use auto_coref
        load = self.config_opt("load").replace(",", " ").split()

        max_files = self.config_opt("max_files", "")

        def file_roots(fnames):
            """ given a list of filenames, return a list containing
            the paths to all the basenames of those files.  So we
            might have in there:
              .../cnn_0000.parse
              .../cnn_0000.sense

            and we just want to end up with
              .../cnn_0000
            """

            return [x for x in set([os.path.splitext(fn)[0] for fn in fnames]) if not os.path.exists(x)]

        if data_in.endswith(".parse"):
             data_in = data_in[:-len(".parse")]

        if os.path.isfile(data_in + ".parse"):
            a_subcorpus = on.corpora.subcorpus(self, data_in, strict_directory_structure=False, extensions=extensions)
            if a_subcorpus.file_hash:
                 self.add_subcorpus(a_subcorpus)
            else:
                raise Exception("File " + data_in + " failed to load")
            return

        if not os.path.exists(data_in):
            raise Exception("The path for data_in (%s) must exist" % data_in)

        if not os.path.isdir(data_in):
            raise Exception("The path for data_in (%s) must be a directory" % data_in)


        for raw_loader in load:
            language, a_genre, a_source = (raw_loader + "-ALL-ALL").split("-")[:3]

            lang_path = os.path.join(data_in, language)
            if not os.path.exists(lang_path):
                raise Exception("Interpreted '%s' to mean language='%s', but path %s did not exist" % (
                    raw_loader, language, lang_path))

            annotations_path = os.path.join(lang_path, "annotations")
            if not os.path.exists(annotations_path):
                raise Exception("No directory 'annotations' under %s, needed for %s" % (
                    lang_path, raw_loader))

            genres = [a_genre]
            if a_genre == "ALL":
                genres = [g for g in listdir(annotations_path) if os.path.isdir(os.path.join(annotations_path, g))]


            for genre in genres:
                genre_path = os.path.join(annotations_path, genre)

                if not os.path.exists(genre_path):
                    raise Exception("Interpreted '%s' to mean we should load genre %s under language %s, but path %s did not exist" % (
                        raw_loader, genre, language, genre_path))

                sources = [a_source]
                if a_source == "ALL":
                    sources = [s for s in listdir(genre_path) if os.path.isdir(os.path.join(genre_path, s))]

                for source in sources:
                    source_dir = os.path.join(genre_path, source)

                    if not os.path.exists(source_dir):
                        raise Exception("Interpreted '%s' to mean we should load source %s under %s-%s, but path %s did not exist" % (
                            raw_loader, source, language, genre, source_dir))

                    on.common.log.status("Loading", language, genre, source)
                    num_scs_in_source = 0

                    prefix, suffix = self._interpret_affixes()

                    if granularity == "source":
                        sources = [source_dir]
                    elif granularity == "section":
                        sources = [section for section in listdir_full(source_dir) if os.path.isdir(section)]
                    elif granularity == "file":
                        sources = []
                        for section in listdir_full(source_dir):
                            if os.path.isdir(section):
                                for fname in listdir_full(section):
                                    if on.common.util.matches_an_affix(fname, (prefix, suffix)):
                                        sources.append(fname)
                        sources = file_roots(sources)
                    else:
                        raise Exception("Unknown setting for granularity: '%s'; expected 'source', 'section', or 'file'" % (granularity))

                    if not sources:
                        sys.stderr.write("Warning: no sources found")

                    for s_dir_or_file in sources:
                        sys.stderr.write(".")
                        a_subcorpus = on.corpora.subcorpus(self, s_dir_or_file,
                                                           prefix=prefix, suffix=suffix,
                                                           lang=language, genre=genre, source=source,
                                                           strict_directory_structure=True,
                                                           extensions=extensions,
                                                           max_files=max_files if granularity=="source" else "")
                        if a_subcorpus.file_hash and (not max_files or num_scs_in_source < int(max_files)):
                            num_scs_in_source += 1
                            self.add_subcorpus(a_subcorpus)

                    sys.stderr.write("\n")


