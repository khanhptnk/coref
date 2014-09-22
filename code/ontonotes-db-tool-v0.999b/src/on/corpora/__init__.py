#!/usr/bin/env python

# COPYRIGHT  2007 BY BBN TECHNOLOGIES CORP.

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

# author: sameer pradhan

"""
-----------------------------------------------------------------
:mod:`on.corpora` -- classes for interpreting annotation
-----------------------------------------------------------------

.. autoclass:: subcorpus
.. autoclass:: abstract_bank
.. autoclass:: document_bank
.. autoclass:: file
.. autoclass:: document
.. autoclass:: sentence
.. autoclass:: token

.. automodule:: on.corpora.tree
.. automodule:: on.corpora.proposition
.. automodule:: on.corpora.sense
.. automodule:: on.corpora.coreference
.. automodule:: on.corpora.name
.. automodule:: on.corpora.ontology
.. automodule:: on.corpora.speaker
.. automodule:: on.corpora.parallel

"""

#---- standard python imports ----#
from __future__ import with_statement
import os
import os.path
import codecs
from difflib import SequenceMatcher
import itertools

try:
    import MySQLdb
except ImportError:
    pass

import string
import sys
import re
import exceptions
import getopt
import UserDict


#---- xml specific imports ----#
from xml.etree import ElementTree
import xml.etree.cElementTree as ElementTree





#---- custom package imports ----#
import on

import on.common.log
import on.common.util

from on.common.log import status


from collections import defaultdict
from on.common.util import insert_ignoring_dups, register_config, same_except_for_tokenization_and_hyphenization, PUNCT



class language_type:
    allowed = ['ar', 'en', 'ch']
    references = []

    sql_table_name = "language_type"
    sql_create_statement = \
"""
create table language_type
(
  language_id varchar(255) character set utf8 not null
)
default character set utf8;
"""


    sql_insert_statement = \
"""insert into language_type
(
  language_id
) values (%s)
"""



class abstract_type_table:
    """ deal with allowable values for types

    Type tables are counts of metadata examples.  Data like 'There are
    31422 instances of the tag ``NN`` applied to tree leaves'.

    For most type tables the allowed types are few and hard coded in
    the ``allowed`` class attribute.  For example, in
    :class:`language_type` , below, the only allowed types are 'en',
    'ch', and 'ar'.  The types are coded with reference to the
    documentation.  This allows for documentation thoroughness and
    consistency checking.

    A few tables are populated from the data.  These are the 'open
    type tables' defined below.

    Closed tables are not instantiated.  For these the type class is
    used only to store the list of allowed types and provide
    sql_insert_statement and sql_create_statment attributes that can
    be used by this class.

    """

    @classmethod
    def write_to_db(cls, a_cursor, write_closed_type_tables=False):

        for a_type_table in on.ontonotes.all_open_type_tables:
            try:
                a_type_table.write_to_db(a_cursor)
            except MySQLdb.Error:
                pass

            sys.stderr.write(".")

        if write_closed_type_tables:
            for a_type_table in on.ontonotes.all_closed_type_tables:
                db_table_name = a_type_table.__name__
                for value in a_type_table.allowed:
                    try:
                        a_cursor.execute("insert into %s values ('%s')" % (db_table_name, value))
                    except Exception:
                        on.common.log.report("populate_type_tables", "duplicate", db_table_name=db_table_name, a_value=value)
                sys.stderr.write(".")

class abstract_open_type_table:

    def __init__(self, a_id, data_pointer=None):
        self.id = a_id
        self.type_hash[self.id] += 1

    @classmethod
    def write_to_db(cls, cursor):
        for a_type in cls.type_hash.keys():
            insert_ignoring_dups(cls, cursor, a_type)

    @classmethod
    def __repr__(cls):
        return " ".join(cls.type_hash.keys())

    @classmethod
    def get_table(cls):
        try:
            return cls.sql_insert_statement.strip().split("\n")[0].split()[2]
        except Exception:
            return "unknown"


class abstract_bank(object):
    """ A superclass for all bank clsses

    All banks support the following psuedocode usage:

    .. code-block:: python

       if load_from_files:
          a_some_bank = some_bank(a_subcorpus, tag[, optional arguments])
       else: # load from db
          a_some_bank = some_bank.from_db(a_subcorpus, tag, a_cursor[, opt_args])

       # only if a_some_bank is not a treebank
       a_some_bank.enrich_treebank(a_treebank[, opt_args])

       for a_some_document in a_some_bank:
          # get the corresponding another document
          another_document = another_bank.get_document(a_some_document)

       if write_to_files:
          a_some_document.dump_view(a_cursor=None, out_dir)
       else: # write to db
          a_some_document.write_to_db(a_cursor)


    See:

     - :class:`on.corpora.tree.treebank`
     - :class:`on.corpora.sense.sense_bank`
     - :class:`on.corpora.proposition.proposition_bank`
     - :class:`on.corpora.coreference.coreference_bank`
     - :class:`on.corpora.name.name_bank`
     - :class:`on.corpora.speaker.speaker_bank`
     - :class:`on.corpora.parallel.parallel_bank`

    """

    def __init__(self, a_subcorpus, tag, extension):
        self._document_hash = {}
        self._document_id_list= []
        self.subcorpus = a_subcorpus
        self.tag = tag

        if tag != "gold":
            extension = tag + "_" + extension

        self.extension = extension

    @property
    def id(self):
        return '%s@%s' % (self.tag, self.subcorpus.id)

    def append(self, a_document):
        if a_document.document_id in self._document_id_list:
            raise Exception("Already contain " + a_document.document_id)

        self._document_id_list.append(a_document.document_id)
        self._document_id_list.sort()
        self._document_hash[a_document.document_id] = a_document


    def delete_document(self, a_document_id_or_instance):
        del self[self.index(a_document_id_or_instance)]

    def index(self, a_document_id_or_instance):
        return self._document_id_list.index(self.__document_id(a_document_id_or_instance))

    def get_document(self, a_document_id_or_instance):
        """ given either a document or a document_id, return the document with a matching id

        raise KeyError on absence

        """

        document_id = self.__document_id(a_document_id_or_instance)
        return self._document_hash[document_id]

    @staticmethod
    def __document_id(a_document_id_or_instance):
        if hasattr(a_document_id_or_instance, "document_id"):
            return a_document_id_or_instance.document_id
        return a_document_id_or_instance

    def __contains__(self, a_document_id_or_instance):
        """ given a document id or an instance, return whether the relevant document is present

        Returns true iff get_document would complete without a key_error
        """
        return self.__document_id(a_document_id_or_instance) in self._document_id_list

    def __delitem__(self, index):
        del self._document_hash[self._document_id_list[index]]
        del self._document_id_list[index]

    def __getitem__(self, index):
        return self._document_hash[self._document_id_list[index]]

    def __len__(self):
        return len( self._document_hash )

    def __repr__(self):
        return "%s instance, id=%s, documents:" % (self.info_name(), self.id) + "\n" + on.common.util.repr_helper(enumerate(self._document_id_list))

    @classmethod
    def info_name(cls):
        return cls.__name__

    def dump_view(self, a_cursor, out_dir="", **kwargs):
        #---- write the sense bank ----#

        if not self:
            status("dump view %s -- no documents" % self.extension)
        elif hasattr(self[0], "dump_view"):
            sys.stderr.write("dumping view %s...." % self.extension)
            for a_document in self:
                sys.stderr.write(".")
                a_document.dump_view(a_cursor, out_dir, **kwargs)
            sys.stderr.write("done.\n")

    @classmethod
    def from_db(cls, a_subcorpus, tag, a_cursor, affixes=None):
        raise NotImplementedError("from_db in " + cls.info_name())

    def enrich_treebank(self, a_treebank):
        todo = "%s with %s" % (a_treebank.extension, self.extension)

        if len(self) == 0:
            status("Not enriching %s because we have no documents" % todo)
            return

        status("Enriching %s ..." % todo)
        a_treebank.inform_enriched(self)

        for a_document in self:
            a_document.tree_document = a_treebank.get_document(a_document)


    def write_to_db(self, a_cursor):
        sys.stderr.write("writing %s to db..." % self.info_name())

        if hasattr(self, "sql_insert_statement"):
            insert_ignoring_dups(self, a_cursor, self.id, self.subcorpus.id, self.tag)

        for a_document in self:
            sys.stderr.write(".")
            a_document.write_to_db(a_cursor)

        sys.stderr.write("\n")




class subcorpus(UserDict.DictMixin):
    """A subcorpus represents an arbitrary collection of documents.

    Initializing

        The best way to deal with subcorpora is not to initialize them
        yourself at all.  Create an ontonotes object with the config
        file, then ask it about its subcorpora.  See :class:`on.ontonotes` .

        The following may be too much detail for your purposes.

        When you ``__init__`` a subcorpus, that's only telling it which
        documents to include.  It doesn't actually load any of them,
        just makes a list.  The :func:`load_banks` method does the
        actual file reading.

        Which collection of documents a subcorpus represents depends
        on how you load it. The main way to do this is to use the
        constructor of :class:`on.ontonotes` .

        Loading the subcorpus directly through its constructor is complex,
        but provides slightly more flexibility.  You need to first
        determine how much you current directory structure matches the one
        that ontonotes ships with.  If you left it in the format:

        .. code-block:: bash

          .../data/<lang>/annotations/<genre>/<source>/<section>/<files>

        Then all you need to do is initialize :class:`on.corpora.subcorpus`
        with:

        .. code-block:: python

            a_subcorpus = on.corpora.subcorpus(a_ontonotes, data_location)

        where ``data_location`` is as much of the data as you want to
        load, perhaps ``.../data/english/annotations/nw/wsj/03``.

        If you're not using the original directory structure, you need to
        specify lang, genre, and source (ex: ``'english'``, ``'nw'``, and ``'wsj'``)
        so that ids can be correctly determined.


        If you want to load some of the data under a directory node but
        not all, prefix and suffix let you choose to load only some files.
        All documents have a four digit numeric ID that identifies them
        given their language, genre, and source.  As in, the document
        ``.../data/english/annotations/nw/wsj/00/wsj_0012`` (which has
        multiple files (``.parse``, ``.name``, ``.sense``, ...)) has id
        0012.  Prefix and suffix are lists of strings that have to match
        these IDs.  If you set prefix to [``'0', '11', '313'``] then the only
        documents considered will be those with ids starting with ``'0',
        '11' or '313'``.  Similarly with suffix.  So:

        .. code-block:: python

          prefix = ['00', '01'], suffix = ['1', '2', '3', '4']

        means we'll load (for ``cnn``):

        .. code-block:: bash

          cnn_0001
          cnn_0002
          ...
          cnn_0004
          cnn_0011
          ...
          cnn_0094
          cnn_0101
          ...
          cnn_0194

        but no files whose ids end not in 1, 2, 3, or 4 or whose ids
        start with anything except '00' and '01'.

    Using

        A subcorpus that's been fully initialized always contains a
        treebank, and generally contains other banks.  To access a
        bank you can use ``[]`` syntax.  For example, to access the
        sense bank, you could do:

        .. code-block:: python

            a_sense_bank = a_subcorpus['sense']

        If you iterate over a subcorpus you get the names of all the
        loaded banks in turn.  So you could do something like:

        .. code-block:: python

            for a_bank_name, a_bank in a_subcorpus.iteritems():
                print 'I found a %s bank and it had %d %s_documents' % (
                     a_bank_name, len(a_bank))

    .. automethod:: load_banks
    .. automethod:: write_to_db
    .. automethod:: copy
    .. automethod:: backed_by
    .. automethod:: __getitem__
    .. automethod:: all_banks
    .. automethod:: get_unmapper

    """

    def __init__(self, a_ontonotes, physical_root_dir, cursor=None,
                 prefix=[], suffix=[], lang=None, source=None, genre=None,
                 strict_directory_structure=False,
                 extensions=["parse", "prop", "sense", "parallel", "coref", "name", "speaker"],
                 max_files="", old_id=""):

        self.ontonotes = a_ontonotes
        name_startswith = ""

        def loadfile(physical_root_dir, filename, filestem_re, num_loaded=[0]):
            on.common.log.debug(filename, on.common.log.DEBUG,
                                on.common.log.MAX_VERBOSITY)

            sys.stderr.write(".")

            #---- get the file extension ----#
            annotation_type = filestem_re.sub("", filename)
            on.common.log.debug(annotation_type, on.common.log.DEBUG,
                                on.common.log.MAX_VERBOSITY)

            if annotation_type not in extensions:
                on.common.log.status("skipping files with extension '.%s' %s" % (annotation_type, extensions))
                on.common.log.debug("skipping files with extension '.%s'"\
                                    % (annotation_type),
                                    on.common.log.DEBUG,
                                    on.common.log.MAX_VERBOSITY)
            else:

                if max_files:
                    if num_loaded[0] == int(max_files):
                        return
                    elif annotation_type.endswith("parse"): # only limit the parses
                        num_loaded[0] += 1

                if(not self.file_hash.has_key(annotation_type)):
                    self.file_hash[annotation_type] = []

                physical_filename = os.path.join(
                    os.path.normpath(physical_root_dir), filename)

                file_id = os.path.normpath(physical_filename)
                file_id = file_id.replace(self.base_dir, "")
                file_id = re.sub("^/", "", file_id)

                a_file = file(self.base_dir, file_id, self.id)

                self.file_hash[annotation_type].append(a_file)


        def loadfiles(root, filestem_re):
            def meets_requirements(child, child_fullname):
                def matches_directory_structure():
                    if not strict_directory_structure:
                        return True

                    bits = child_fullname.split(os.path.sep)
                    return (bits[-3] == source and
                            bits[-4] == genre and
                            bits[-5] == "annotations" and
                            bits[-6] == lang)

                return (matches_directory_structure()
                        and child.startswith(name_startswith)
                        and on.common.util.matches_an_affix( child, (prefix, suffix) ))

            for child in on.common.util.listdir(root):
                child_fullname = os.path.join(root, child)

                if os.path.isdir(child_fullname):
                    loadfiles(child_fullname, filestem_re)
                elif meets_requirements(child, child_fullname):
                    loadfile(root, child,  filestem_re)


        def parse_init_path():
            """ parses physical_root_dir into (top_dir, base_dir, source, genre, lang)

            physical_root_dir is any child dir or file root of
              .../data/
            such as
              .../data/english/annotations/bc/cnn/00/cnn_0000
              .../data/english/annotations/bc/cnn/00/
              .../data/english/
              .../data/

            but any of {source, genre, lang} that are not parsable from
            the init path need to be passed in to __init__ as arguments

            This is because we need to have ids in the form

              source/subdivision/source_{subdivision}{docindex}@source@medium@lang@on

            such as

              cnn/00/cnn_0000@bn@en@on

            If we let physical_root_dir be something higher than
            data/lang/annotations/genre/source and things are not
            otherwise specified then we have no way to figure out what
            medium and source we should be using.  We want ids not to
            depend on how much of the corpus is loaded at once.
            """

            # [..., data, english, annotations, bc, cnn, 00]
            bits = physical_root_dir.split(os.path.sep)

            data_index = len(bits)-(list(reversed(bits)).index("data") + 1)
            assert bits[data_index] == "data"

            if not bits[-1]:
                """ they might have passed it in as .../bc/cnn/00/ and we
                made an empty slot for the final dash """
                del bits[-1]

            if not bits[data_index:]:
                raise Exception("Invalid path to subcorpus.__init__: %s", physical_root_dir,)

            top_dir =  bits[:data_index+1]
            base_dir = bits[:data_index+1]

            m_lang = lang or bits[ data_index + 1 ]


            if os.path.exists(os.path.sep.join(top_dir + [m_lang])):
                top_dir += [m_lang]

            if os.path.exists(os.path.sep.join(base_dir + [m_lang, "annotations"])):
                base_dir += [m_lang, "annotations"]


            m_genre = genre or bits[ data_index + 3 ]

            m_source = source or bits[ data_index + 4 ]

            id_first_bit = bits[ data_index + 5 :]
            if not id_first_bit:
                id_first_bit = ["all"]

            root_dir = [m_genre, m_source]

            return os.path.sep.join(top_dir), os.path.sep.join(base_dir), \
                   os.path.sep.join(root_dir), m_lang, m_genre, m_source, \
                   "_".join(id_first_bit)

        physical_root_dir = os.path.normpath(physical_root_dir)

        self.top_dir, self.base_dir, self.root_dir, \
                      lang, genre, source, id_first_bit = parse_init_path()

        if not cursor and not os.path.exists(physical_root_dir):
            """ they passed us a link to a specific file's base (like
            .../cnn_0023) instead of a directory.  So set
            physical_root_dir to be the parent directory and require
            the specified file id.  This also means that we need to
            make the subcorpus id be more detailed, as otherwise it
            will fail to have a unique id"""

            name_startswith = os.path.split(physical_root_dir)[1]
            physical_root_dir = os.path.split(physical_root_dir)[0]
            id_first_bit = name_startswith.split("_")[-1]

        self.physical_root_dir = physical_root_dir
        on.common.log.debug(self.physical_root_dir, on.common.log.DEBUG, on.common.log.MAX_VERBOSITY)

        self.language_id = lang[:2]
        if self.language_id not in language_type.allowed:
            raise Exception("Invalid language %s to subcorpus.__init__.  Valid language are: %s" % (
                lang[:2], ", ".join(language_type.allowed)))
        self.source = source
        self.genre = genre
        self.encoding_id = "utf-8"

        if old_id:
            id_first_bit = old_id.split("@")[0]

        self.id = "@".join([id_first_bit, source, genre,
                            self.language_id, self.ontonotes_id])

        self.name = self.id # definitely unique now
        self.cursor = cursor

        #---- this has points to a list of files for the type of annotation represented by the key
        self.file_hash = {}

        self.banks = {}

        if not cursor:
            #---- reading from the filesystem ----#

            follow_symlinks = True
            filestem_re = re.compile("^.*\.")

            loadfiles(self.physical_root_dir, filestem_re)

            if self.file_hash:
                n = max([len(self.file_hash[extension]) for extension in self.file_hash])
                sys.stderr.write(
                    "\nfound %d %s %s%s%s%s%s%s%sin the subcorpus %s\n" % (
                    n, "file" if n == 1 else "files",
                    "starting with " if prefix else "",
                    "any of " if len(prefix) > 1 else "",
                    "%s " % (prefix[0] if len(prefix) == 1 else str(prefix))
                      if prefix else "",
                    "and " if prefix and suffix else "",
                    "ending with " if suffix else "",
                    "any of " if len(suffix) > 1 else "",
                    "%s " % (suffix[0] if len(suffix) == 1 else str(suffix))
                      if suffix else "",
                    self.id))

    @property
    def ontonotes_id(self):
        return self.ontonotes.id


    def backed_by(self):
        """ Returns either 'db' or 'fs'

        We can be pulling our data from the database or the filesystem
        depending on how we were created.  Note that even if we're
        reading from the file system, if the db is available we use it
        for sense inventory and frame lookups.

        """

        if self.file_hash:
            return "fs"
        return "db"

    def copy(self):
        """ make a duplicate of this subcorpus that represents the same documents

        Note: if you had already loaded some banks these are absent in the copy.

        """

        a_subcorpus = subcorpus(self.ontonotes, self.physical_root_dir, cursor="a fake cursor", old_id=self.id)

        if self.file_hash:
            for extension in self.file_hash:
                a_subcorpus.file_hash[extension] = self.file_hash[extension][:]

        return a_subcorpus

    @register_config("corpus", "banks",
                     doc="Any extension is allowed as long as it is standard or ends in " +
                         "'_' followed by one of the standard ones. " +
                         "For example, 'parse auto_parse sense:parse auto_sense:auto_parse' " +
                         "would be legal and would load the files with those extensions as " +
                         "treebanks and sense banks as appropriate.")
    @register_config("corpus", "wsd-indexing",
                     allowed_values=["word", "token", "nword_vtoken", "ntoken_vword"])
    @register_config("corpus", "name-indexing", allowed_values=["word", "token"])
    @register_config("corpus", "prop-ignore-errors", allowed_values=["true", "false"])
    @register_config("corpus", "ignore-inventories", allow_multiple=True,
                     allowed_values=["senses", "frames", "senses,frames", "frames,senses"])
    def load_banks(self, config):
        """ Load the individual bank data for the subcorpus to memory

        Once a subcorpus is initialized we know what documents it
        represent (as in cnn_0013) but we've not loaded the actual
        files (as in cnn_0013.parse, cnn_0013.sense, ...).  We often
        only want to load some of these, so specify which extensions
        (prop, parse, coref) you want with the corpus.banks config
        variable

        This code will, for each bank, load the files and then enrich
        the treebank with appropriate links.  For example, enriching
        the treebank with sense data sets the
        :attr:`on.corpora.tree.tree.on_sense` attribute of every
        tree leaf that's been sense tagged.  Once all enrichment has
        happened, one can go through the trees and be able to access
        all the annotation.

        (Minor exception: some name and coreference data is not
        currently fully aligned with the tree and is inaccessible in
        this manner)

        """

        def config_has_opt(key):
            return config.has_option("corpus", key)

        def config_opt(key, default=None):
            if default is None or config_has_opt(key):
                return config["corpus", key]
            return default

        def parse_extension(extension):
            if extension.startswith("."):
                extension = extension[1:]

            if "_" in extension:
                tag, standard_extension = extension.rsplit("_", 1)
                return tag, standard_extension
            return "gold", extension

        def make_extension(tag, std_ext):
            if tag == "gold":
                return std_ext
            return "%s_%s" % (tag, std_ext)

        extension_details = [] # extension, stdext, tag, align_tag (None for parses)
        for extension in config_opt("banks", "parse").replace(","," ").split():

            first_half = extension.split(":")[0]
            tag, ext = parse_extension(first_half)

            if ":" not in extension:
                extension += ":" + make_extension(tag, "parse")

            second_half = extension.split(":")[1]
            s_tag, s_ext = parse_extension(second_half)

            if s_ext != "parse":
                raise Exception("Invalid corpus.banks value %r -- after a colon only parses make sense" % extension)

            refer_tag = tag
            if ext == "parse":
                refer_tag = s_tag

            refer_extension = make_extension(refer_tag, ext)
            real_extension = make_extension(tag, ext)

            extension_details.append( (refer_extension, real_extension, ext, refer_tag, s_tag) )

        frame_set_hash = {}
        sense_inventory_hash = {}

        prefix, suffix = [], []
        if config_has_opt("prefix"):
            prefix = config_opt("prefix").split()
        if config_has_opt("suffix"):
            suffix = config_opt("suffix").split()
        affixes = (prefix, suffix)

        if config.has_section("db"):
            a_cursor = on.ontonotes.db_cursor(config)
            frame_set_hash = on.common.util.make_db_ref(a_cursor)
            sense_inventory_hash = on.common.util.make_db_ref(a_cursor)


        if "frames" in config_opt("ignore-inventories", "").replace(","," ").split():
            frame_set_hash = on.common.util.make_not_loaded()
        if "senses" in config_opt("ignore-inventories", "").replace(","," ").split():
            sense_inventory_hash = on.common.util.make_not_loaded()

        on.common.log.status("Loading banks for %s: %s ..." % (self.id, ", ".join([detail[0] for detail in extension_details])))

        for refer_extension, real_extension, stdext, tag, s_tag in extension_details:
            """ load all the treebanks first """

            if stdext != "parse":
                continue

            if refer_extension in self:
                raise Exception("Asked to load %r multiple times" % refer_extension)

            if self.backed_by() == "db":
                self[refer_extension] = on.corpora.tree.treebank.from_db(self, tag, a_cursor, affixes=affixes)
            else:
                self[refer_extension] = on.corpora.tree.treebank(self, tag, file_input_extension=real_extension)

            document_extension = refer_extension.replace("parse", "document")

            self[document_extension] = on.corpora.document_bank(
                self[refer_extension], tag, self.language_id, self.genre, self.source)


        for refer_extension, real_extension, stdext, tag, s_tag in extension_details:
            """ load all the other banks """

            if stdext == "parse":
                continue

            if refer_extension in self:
                raise Exception("Asked to load %r multiple times" % refer_extension)

            tree_extension = make_extension(s_tag, "parse")

            if tree_extension not in self:
                raise Exception("Can't enrich %r with %r because %r is not loaded.  (loaded: %r)" % (
                    tree_extension, refer_extension, tree_extension, self.banks.keys()))
            if not self[tree_extension]:
                continue

            a_bank_class = self.bank_class(stdext)
            enrich_treebank_kwargs = {}

            # load the banks to memory
            if self.backed_by() == "db":
                if self.exist_some(a_cursor, a_bank_class, affixes=affixes):
                    a_bank = a_bank_class.from_db(self, tag, a_cursor, affixes=affixes)
                else:
                    on.common.log.status("Did not find " + refer_extension + " in the db")
                    continue

            elif stdext == "sense":
                frame_set_hash_for_sense = frame_set_hash or on.common.util.make_not_loaded()
                a_bank = a_bank_class(self, tag, indexing=config_opt("wsd-indexing", "word"),
                                      a_sense_inv_hash=sense_inventory_hash,
                                      a_frame_set_hash=frame_set_hash_for_sense)
            elif stdext == "prop":
                a_bank = a_bank_class(self, tag, a_frame_set_hash=frame_set_hash)
                prop_ignore_errors = config_opt("prop-ignore-errors", "false")
                enrich_treebank_kwargs["ignore_errors"] = (prop_ignore_errors == "true")
            elif stdext == "name":
                a_bank = a_bank_class(self, tag, indexing=config_opt("name-indexing", "word"))
            elif stdext == "coref":
                a_bank = a_bank_class(self, tag, indexing=config_opt("coref-indexing", "token"),
                                      messy_muc_input=False)
            else:
                a_bank = a_bank_class(self, tag)

            a_bank.enrich_treebank(self[tree_extension], **enrich_treebank_kwargs)

            self[refer_extension] = a_bank

    def find_subcorpus_for_document_id(self, document_id):
        desired_subcorpus_id = "@".join(document_id.split("@")[-(self.id.count("@") + 1):])
        return self.ontonotes.get_subcorpus(desired_subcorpus_id, banks_loaded=True, save_this=True)

    def __len__(self):
        return len(self.banks)

    def __getitem__(self, key):
        """ The standard way to access individual banks is with [] notation.

        The keys are extensions.  To iterate over multiple banks in
        parallel, do something like:

        .. code-block:: python

          for a_tree_doc, a_sense_doc in zip(a_subcorpus['parse'], a_subcorpus['sense']):
             pass

        Note that this will not work if some parses do not have sense documents.

        """
        return self.banks[key]

    def most_recent_treebank(self):
        """

        uses the convention vNNNNN_parse where NNNNN is a version number

        returns [] if there is no such bank, the most recent bank otherwise
        empty treebanks are ignored

        """

        vX_parses = [x for x in self.keys()
                     if (x.startswith("v") and
                         x.endswith("_parse") and
                         self[x])]

        if not vX_parses:
            return []

        vX_parses.sort()

        return self[vX_parses[-1]] # the highest numbered one is the most recent



    def all_banks(self, standard_extension):
        """ The way to get a list of all banks of a type.

        For example, if you have::

          cnn_0000.no_traces_parse
          cnn_0000.auto_traces_parse
          cnn_0000.parse

        If you want to iterate over all trees in all treebanks, you could do:

        .. code-block:: python

          for a_treebank in a_subcorpus.all_banks('parse'):
             for a_tree_document in a_treebank:
                for a_tree in a_tree_document:
                   pass

        """

        return [self[a_bank_key] for a_bank_key in self.keys()
                if a_bank_key == standard_extension or a_bank_key.endswith("_" + standard_extension)]


    def __setitem__(self, key, value):
        self.banks[key] = value

    def keys(self):
        return self.banks.keys()

    def __repr__(self):
        bank_ext_with_id = ((a_bank_ext, a_bank.id) for a_bank_ext, a_bank in self.banks.iteritems())

        return "subcorpus instance, id=%s, banks:" % (self.id) + "\n" + on.common.util.repr_helper(bank_ext_with_id)


    # return the files for a given annotation_type in this subcorpus
    #
    # annotation_type can take values "coref" "parse" "sense" "prop" or "name"
    def get_files(self, annotation_type):
        if(self.file_hash.has_key(annotation_type)):
            return self.file_hash[annotation_type]
        else:
            on.common.log.status("keys: %s" % self.file_hash.keys())
            return {}

    def add_bank(self, bank_name, a_bank):
        if bank_name not in self.banks:
            self.banks[bank_name] = a_bank
        else:
            raise Exception("a %s bank (%s) is already present, don't want (%s)" % (bank_name, self.banks[bank_name].tag, a_bank.tag))

    def delete_bank(self, bank_name):
        try:
            del self.banks[bank_name]
        except KeyError:
            raise Exception("can't delete %s bank: not present" % bank_name)

    def get_bank(self, bank_name):
        try:
            return self.banks[bank_name]
        except KeyError:
            raise Exception("can't find %s bank: not present" % bank_name)


    sql_table_name = "subcorpus"
    sql_create_statement = \
"""
create table subcorpus
(
  id varchar(255) not null primary key,
  base_dir varchar(255) not null,
  top_dir varchar(255) not null,
  root_dir varchar(255) not null,
  language_id varchar(255) not null,
  encoding_id varchar(255) not null,
  ontonotes_id varchar(255) not null

)
default character set utf8;
"""
    sql_insert_statement = \
"""
insert into subcorpus
(
  id,
  base_dir,
  top_dir,
  root_dir,
  language_id,
  encoding_id,
  ontonotes_id
) values (%s, %s, %s, %s, %s, %s, %s)
"""

    def write_to_db(self, a_cursor, only_these_banks=[]):
        """ Write the subcorpus and all files and banks within to the database.

        Generally it's better to use :meth:`on.ontonotes.write_to_db`
        as that will write the type tables as well.  If you don't,
        perhaps for reasons of memory usage, write individual
        subcorpora to the database, you need to call
        :func:`on.ontonotes.write_type_tables_to_db` after the last time you
        call :func:`write_to_db`.

        Parameters:
         - a_cursor -- The ouput of :func:`on.ontonotes.get_db_cursor`
         - only_these_banks -- if set, load only these extensions to the db

        """

        #---- insert the value in the table ----#
        insert_ignoring_dups(self, a_cursor,
                             self.id, self.base_dir,
                             self.top_dir, self.root_dir,
                             self.language_id, self.encoding_id,
                             self.ontonotes_id)

        #---- also write the file table ----#
        for key in self.file_hash:
            for ffile in self.file_hash[key]:
                ffile.write_to_db(a_cursor)

        #---- write all the banks to the database ----#
        if not only_these_banks:
            only_these_banks = self.banks.keys()

        for a_bank in only_these_banks:
            self[a_bank].write_to_db(a_cursor)


    @staticmethod
    def bank_class(extension):
        """ Given a bank's standard extension, like 'coref', return it's class. """

        extension_to_class = {"coref":    on.corpora.coreference.coreference_bank,
                              "name":     on.corpora.name.name_bank,
                              "prop":     on.corpora.proposition.proposition_bank,
                              "sense":    on.corpora.sense.sense_bank,
                              "speaker":  on.corpora.speaker.speaker_bank,
                              "parallel": on.corpora.parallel.parallel_bank,
                              "parse":    on.corpora.tree.treebank}
        if extension not in extension_to_class:
            raise Exception("Unknown standard extension " + extension)

        return extension_to_class[extension]

    def exist_some(self, a_cursor, a_bank_class, affixes=None):
        """ is there any data for this bank_name in a_subcorpus matching the affixes?

        It usually only ever makes sense to try to do something
        with a table if there's anything in it.  So you use this
        function and if there isn't anything then it returns 0.
        Otherwise it returns the number of documents """

        def table_name():
            """ yield the name of a table which will have records for each
            document in this bank. """

            if hasattr(a_bank_class, 'sql_exists_table'):
                return a_bank_class.sql_exists_table
            return a_bank_class.sql_table_name

        def field_name():
            """ yield the name of the field in the table for this bank
            that will be shared for all entries that go under one document
            """

            if hasattr(a_bank_class, 'sql_exists_field'):
                return a_bank_class.sql_exists_field

            return "document_id"

        table = table_name()
        field = field_name()

        count = 0

        a_cursor.execute("""select id from document where subcorpus_id = '%s';""" % (self.id))
        document_rows = a_cursor.fetchall()

        for document_row in document_rows:
            a_document_id = document_row["id"]
            if not on.common.util.matches_an_affix(a_document_id, affixes):
                continue

            a_cursor.execute("""select count(id) from %s where %s = '%s';""" % (table, field, a_document_id))
            count += a_cursor.fetchall()[0]["count(id)"]

        return count

    @staticmethod
    def subcorpora_in_db(cursor, ontonotes_id):
        cursor.execute("""select id, base_dir, root_dir from subcorpus where ontonotes_id = %s""", (ontonotes_id))

        return [(row["id"], "%s/%s" % (row["base_dir"], row["root_dir"]))
                for row in cursor.fetchall()]

class token:
    """A token.  Just a word and a part of speech"""

    def __init__(self, a_leaf):
        self.id = a_leaf.id
        self.word = a_leaf.get_word()
        self.part_of_speech = a_leaf.part_of_speech



    def __repr__(self):
        return "<token object: id: %s; word: %s; part_of_speech: %s>" % (self.id,
                                                                         self.word,
                                                                         self.part_of_speech)

    sql_table_name = "token"
    sql_create_statement = \
"""
create table token
(
  id varchar(255) not null primary key,
  word varchar(255) not null,
  part_of_speech varchar(255) not null
)

default character set utf8;
"""


    sql_insert_statement = \
"""insert into token
(
  id,
  word,
  part_of_speech
) values (%s, %s, %s)
"""

    def write_to_db(self, cursor):
        data = []
        a_tuple = (self.id,
                   self.word,
                   self.part_of_speech)

        data.append(a_tuple)

        #---- insert the value in the table ----#
        cursor.executemany("%s" % (self.__class__.sql_insert_statement), data)







class sentence:
    """Represents a sentence; a list of tokens.  Generally working
    with :class:`on.corpora.tree.tree` objects is easier."""

    def __init__(self, a_tree):
        self.id = a_tree.id #---- sentence id and root tree ids are the same
        self.token_ids = []
        self.token_hash = {}
        self.string = ""
        self.no_trace_string = ""

        (self.index, self.document_id) = a_tree.id.split("@", 1)

        for a_leaf in a_tree.leaves():
            a_token = token(a_leaf)
            self.token_ids.append(a_token.id)
            self.token_hash[a_token.id] = a_token

            try:
                self.string = self.string + " " + a_token.word
            except Exception:
                on.common.log.error("need to take care of the stub tree where the TOP is returned as a leaf.  temporarily using a ? in place of the.", False)
                self.string = self.string + " " + "?"

            if(a_leaf.trace_type == None):
                try:
                    self.no_trace_string = self.no_trace_string + " " + a_leaf.get_word()
                except Exception:
                    on.common.log.error("need to take care of the stub tree where the TOP is returned as a leaf.  temporarily using a ? in place of the.", False)
                    self.no_trace_string = self.no_trace_string + " " + "?"


        self.string = self.string.strip()
        self.no_trace_string = self.no_trace_string.strip()

    def __repr__(self):
        return "<sentence object: id: %s; string: %s>" % (self.id, self.string)


    sql_table_name = "sentence"
    sql_create_statement = \
"""
create table sentence
(
  id varchar(255) not null primary key,
  sentence_index int not null,
  document_id varchar(255) not null,
  string longtext not null,
  no_trace_string longtext not null
)
default character set utf8;
"""


    sql_insert_statement = \
"""insert into sentence
(
  id,
  sentence_index,
  document_id,
  string,
  no_trace_string
) values (%s, %s, %s, %s, %s)
"""


    def write_to_db(self, cursor):
        data = []
        a_tuple = (self.id,
                   self.index,
                   self.document_id,
                   self.string,
                   self.no_trace_string)

        data.append(a_tuple)

        #---- insert the value in the table ----#
        cursor.executemany("%s" % (self.__class__.sql_insert_statement), data)

        for a_token_id in self.token_ids:
            a_token = self.token_hash[a_token_id]
            a_token.write_to_db(cursor)











class document:
    """The text of a document.  In current usage there is only ever
    one document per :class:`.file`, but there could in theory be more
    than one."""

    def __init__(self, a_tree_document, lang_id, genre, source):
        self.document_id = a_tree_document.document_id
        self.sentence_ids = []
        self.sentence_hash = {}
        self.text = ""
        self.no_trace_text = ""
        self.subcorpus_id = a_tree_document.subcorpus_id
        self.tree_document = a_tree_document

        self.lang_id = lang_id
        self.genre = genre
        self.source = source

        for a_tree_id in a_tree_document.tree_ids:

            a_sentence = sentence(a_tree_document.tree_hash[a_tree_id])
            self.sentence_ids.append(a_sentence.id)
            self.sentence_hash[a_sentence.id] = a_sentence

            self.text = self.text + "\n" + a_sentence.string
            self.no_trace_text = self.no_trace_text + "\n" + a_sentence.no_trace_string

        self.text = self.text.strip()
        self.no_trace_text = self.no_trace_text.strip()


    def __repr__(self):
        return "document instance, id=%s, %s sentences" % (self.document_id, len(self))

    def __len__(self):
        return len(self.sentence_ids)

    def __getitem__(self, idx):
        return self.sentence_hash(self.sentence_ids(idx))

    def onf(self):
        return self.tree_document.onf()


    sql_table_name = "document"
    sql_create_statement = \
"""
create table document
(
  id varchar(255) not null primary key,
  subcorpus_id varchar(255) not null,
  lang_id varchar(16) not null,
  genre varchar(16) not null,
  source varchar(16) not null,
  text longtext not null
)
default character set utf8;
"""


    sql_insert_statement = \
"""insert into document
(
  id,
  subcorpus_id,
  lang_id,
  genre,
  source,
  text
) values (%s, %s, %s, %s, %s, %s)
"""

    def write_to_db(self, cursor):

        #---- insert the value in the table ----#
        cursor.executemany("%s" % self.__class__.sql_insert_statement,
                           [(self.document_id, self.subcorpus_id,
                             self.lang_id, self.genre, self.source,
                             self.text)])

        for a_sentence_id in self.sentence_ids:
            a_sentence = self.sentence_hash[a_sentence_id]
            a_sentence.write_to_db(cursor)










class file:
    """A file.  Currently synonimous :class:`document`"""

    def __init__(self, base_dir, file_id, subcorpus_id):
        self.base_dir = base_dir
        self.id  = "%s@%s" % (file_id, subcorpus_id)
        self.subcorpus_id = subcorpus_id

        #---- derived attributes ----#
        self.physical_filename = "%s/%s" % (base_dir, file_id)
        self.document_id = file_id.rsplit(".", 1)[0]

        self.file_type = re.sub("^.*\.", "", self.id)
        self.physical_file_stem = os.path.splitext(self.physical_filename)[0]


    def __repr__(self):
        return self.id

    sql_table_name = "file"
    sql_create_statement = \
"""create table file
(
  id varchar(255) not null primary key,
  base_dir varchar(255) not null,
  physical_filename varchar(255) not null,
  document_id varchar(255) not null,
  file_type varchar(255) not null,
  subcorpus_id varchar(255) not null
)
default character set utf8;
"""
    sql_insert_statement = \
"""insert into file
(
  id,
  base_dir,
  physical_filename,
  document_id,
  file_type,
  subcorpus_id
) values (%s, %s, %s, %s, %s, %s)
"""

    def write_to_db(self, cursor):

        #---- enumerate the subcorpora into a list ----#
        data = [(self.id, self.base_dir, self.physical_filename, self.document_id, self.file_type, self.subcorpus_id)]

        #---- insert the value in the table ----#
        cursor.executemany("%s" % (self.__class__.sql_insert_statement), data)



class document_bank(abstract_bank):
    def __init__(self, a_treebank, tag, lang_id, genre, source):
        abstract_bank.__init__(self, a_treebank.subcorpus, tag, "document")

        for a_tree_document in a_treebank:
            self.append(document(a_tree_document, lang_id, genre, source) )

    sql_table_name = "document_bank"

    ## @var SQL create statement for the syntactic_link table
    #
    sql_create_statement = \
"""
create table document_bank
(
  id varchar(255) not null primary key,
  subcorpus_id varchar(255) not null,
  tag varchar (255) not null
)
default character set utf8;
"""



    ## @var SQL insert statement for the syntactic_link table
    #
    sql_insert_statement = \
"""
insert into document_bank
(
  id,
  subcorpus_id,
  tag
) values(%s, %s, %s)
"""

class tree_alignable_sgml_span(object):

    def __init__(self, name):
        self._subtree_id = None
        self._start_token_index = None
        self._end_token_index = None     #---- inclusive
        self._start_word_index = None
        self._end_word_index = None      #---- inclusive
        self._sentence_index = None
        self._start_leaf = None
        self._end_leaf = None

        # with these, 0 means no subtoken annotation at all and is the
        # default.  Other values mean that we should be interpreted as
        # containing only parts of our initial and final tokens.  So
        # if we have:
        #
        #   <ENAMEX TYPE="NORP" S_OFF="4">non-Hong Kong</ENAMEX>
        #
        # that is equivalent to the (illegal) expression:
        #
        #   non-<ENAMEX TYPE="NORP">Hong Kong</ENAMEX>
        #
        # Similarly, if we have:
        #
        #   <ENAMEX TYPE="NORP" S_OFF="6" E_OFF="6">Macao-Hong Kong-Zhuai</ENAMEX>
        #
        # that would be:
        #
        #   Macao-<ENAMEX TYPE="NORP">Hong Kong</ENAMEX>-Zhuai
        #
        # If all three entities were tagged, we would get the mess:
        #
        #
        #   <ENAMEX TYPE="NORP" S_OFF="5" E_OFF="6"><ENAMEX TYPE="NORP" E_OFF="5">Macao-Hong
        #   </ENAMEX> <ENAMEX TYPE="NORP" S_OFF="5">Kong-Zhuai</ENAMEX></ENAMEX>
        #
        # which would be the only somewhat less messy:
        #
        #   <ENAMEX TYPE="NORP">Macao</ENAMEX>-<ENAMEX TYPE="NORP">Hong Kong</ENAMEX>-<ENAMEX TYPE="NORP">Zhuai</ENAMEX>
        #
        self.start_char_offset = 0 # how many characters from the start of the first token we start
        self.end_char_offset = 0   # how many characters from the end of the last token we end

        self.__sgml_span_name = name

        self.string = None


    def get_string_ignoring_subtoken_annotation(self):
        if self.start_leaf and self.end_leaf:
            if self.start_leaf.get_root() is self.end_leaf.get_root():
                assert self.start_leaf.get_token_index() <= self.end_leaf.get_token_index(), (self.start_leaf.get_token_index(), self.end_leaf.get_token_index())

                tokens = None
                for a_leaf in self.start_leaf.get_root().leaves():
                    if a_leaf == self.start_leaf:
                        tokens = []
                    if tokens is not None:
                        tokens.append(a_leaf.get_word())
                    if a_leaf == self.end_leaf:
                        if not tokens:
                            return ""

                        return " ".join(tokens)
        return ""

    def _get_string(self):
        if self.start_leaf and self.end_leaf:
            tr = None
            if self.start_leaf.get_root() is self.end_leaf.get_root():
                try:
                    tr = self.get_string_ignoring_subtoken_annotation()
                except Exception:
                    tr = None

                if tr:
                    return tr[self.start_char_offset : len(tr) - self.end_char_offset]

            # if there's something screwy about the tree or start and end leaves are in the wrong tree

            self.valid = False

            start_sentence = self.start_leaf.get_root().get_word_string()
            end_sentence = self.end_leaf.get_root().get_word_string()

            lang = self.start_leaf.get_root().language

            #if lang == "ar":
            #    start_sentence = on.common.util.buckwalter2unicode(start_sentence)
            #    end_sentence = on.common.util.buckwalter2unicode(end_sentence)

        return self._string


    def _set_string(self, val):
        if self.start_leaf and self.end_leaf:
            raise Exception("Cannot set string after enrichment.  Set start_leaf and end_leaf instead.")
        self._string = val

    string = property(_get_string, _set_string)

    def _get_start_leaf(self): return self._start_leaf

    def _set_start_leaf(self, new_leaf):
        if self.start_leaf:
            start_spans = self.__get_spans("start", self.start_leaf)
            if self in start_spans:
                start_spans.remove(self)

        self.__remove_us_from_subtree()

        if new_leaf is None:
            if self.__sgml_span_name == "named_entity":
                self._start_token_index = None
                self._start_word_index = self.start_word_index
            else:
                self._start_word_index = None
                self._start_token_index = self.start_token_index

            #self._sentence_index = self.sentence_index
            self._subtree_id = self.subtree_id

            try:
                self._string = self._get_string()
            except Exception:
                self._string = None

            self._start_leaf = None
        else:
            while new_leaf.is_trace() and self.__sgml_span_name == "named_entity":
                try:
                    new_leaf = new_leaf.get_root()[new_leaf.start+1]
                except Exception:
                    raise Exception("Name can't start with an end-of-tree trace")

            self.__get_spans("start", new_leaf).append(self)
            self._start_leaf = new_leaf
            self.__update_subtree_with_us()

    start_leaf = property(_get_start_leaf, _set_start_leaf)

    def __get_spans(self, start_or_end, a_leaf):
        return getattr(a_leaf, start_or_end + "_" + self.__sgml_span_name + "_list")

    def _get_end_leaf(self): return self._end_leaf

    def _set_end_leaf(self, new_leaf):
        if self.end_leaf:
            end_spans = self.__get_spans("end", self.end_leaf)
            if self in end_spans:
                end_spans.remove(self)

        self.__remove_us_from_subtree()

        if new_leaf is None:
            if self.__sgml_span_name == "named_entity":
                self._end_token_index = None
                self._end_word_index = self.end_word_index
            else:
                self._end_word_index = None
                self._end_token_index = self.end_token_index
            self._end_leaf = None
        else:
            while new_leaf.is_trace() and self.__sgml_span_name == "named_entity":
                try:
                    new_leaf = new_leaf.get_root()[new_leaf.start-1]
                except Exception:
                    raise Exception("Name can't end with a start-of-tree trace")

            end_spans = self.__get_spans("end", new_leaf).append(self)
            self._end_leaf = new_leaf
            self.__update_subtree_with_us()



    end_leaf = property(_get_end_leaf, _set_end_leaf)

    def __remove_us_from_subtree(self):
        a_subtree = self.subtree
        if a_subtree:
            setattr(a_subtree, self.__sgml_span_name, None)

    def __update_subtree_with_us(self):
        a_subtree = self.subtree
        if a_subtree:
            prior_span = getattr(a_subtree, self.__sgml_span_name)
            if prior_span:
                on.common.log.report(self.__sgml_span_name,
                                     "duplicate %s annotation on node" % self.__sgml_span_name,
                                     id=a_subtree.id,
                                     link_a=prior_span._get_string(),
                                     link_b=self._get_string())
            else:
                setattr(a_subtree, self.__sgml_span_name, self)


    def __get_indexer(start_or_end, word_or_token):
        assert start_or_end in ["start", "end"]
        assert word_or_token in ["word", "token"]
        other = {"word" : "token", "token" : "word"}

        def g(self):
            a_leaf = getattr(self, start_or_end + "_leaf")
            if not a_leaf:
                return getattr(self, "_" + start_or_end + "_" + word_or_token + "_index")
            elif word_or_token == "word":
                return a_leaf.get_word_index(sloppy = ("next" if start_or_end == "start" else "prev"))
            else: # token
                return a_leaf.get_token_index()
        return g

    @property
    def primary_start_index(self):
        if self.start_token_index is not None:
            return self.start_token_index
        return self.start_word_index

    @property
    def primary_end_index(self):
        if self.end_token_index is not None:
            return self.end_token_index
        return self.end_word_index

    def __set_indexer(start_or_end, word_or_token):
        assert start_or_end in ["start", "end"]
        assert word_or_token in ["word", "token"]
        other = {"word" : "token", "token" : "word"}

        def s(self, val):
            if val is not None:
                val = int(val)
            a_leaf = getattr(self, start_or_end + "_leaf")
            if not a_leaf:
                if getattr(self, "_" + start_or_end + "_" + other[word_or_token] + "_index") is not None:
                    raise Exception("Tried to set %s %s index when %s %s index was already set" % (
                        start_or_end, word_or_token, start_or_end, other[word_or_token]))

                return setattr(self, "_" + start_or_end + "_" + word_or_token + "_index", val)
            raise Exception("Cannot set %s %s index after enrichment.  Set %s_leaf instead." % (
                start_or_end, word_or_token, start_or_end))
        return s

    start_token_index = property(__get_indexer("start", "token"), __set_indexer("start", "token"))
    end_token_index = property(__get_indexer("end", "token"), __set_indexer("end", "token"))
    start_word_index = property(__get_indexer("start", "word"), __set_indexer("start", "word"))
    end_word_index = property(__get_indexer("end", "word"), __set_indexer("end", "word"))

    def _get_sentence_index(self):
        start_idx = self.start_leaf.get_sentence_index() if self.start_leaf else self._sentence_index
        end_idx = self.end_leaf.get_sentence_index() if self.end_leaf else self._sentence_index

        assert start_idx == end_idx
        return start_idx

    def __set_only_if_no_leaves_set(attr):
        def s(self, val):
            if self.start_leaf or self.end_leaf:
                raise Exception("Cannot set %s after enrichment.  Set start_leaf and end_leaf." % attr)
            setattr(self, attr, val)
        return s

    sentence_index = property(_get_sentence_index, __set_only_if_no_leaves_set("_sentence_index"))

    def _get_subtree_id(self):
        if not self.start_leaf or not self.end_leaf:
            return self._subtree_id
        a_subtree = self.subtree
        if not a_subtree:
            return None
        return a_subtree.id

    subtree_id = property(_get_subtree_id, __set_only_if_no_leaves_set("_subtree_id"))

    @property
    def subtree(self):
        if not self.start_leaf or not self.end_leaf or self.start_leaf.get_root() != self.end_leaf.get_root():
            return None

        try:
            return self.start_leaf.get_root().get_subtree_by_span(self.start_leaf, self.end_leaf)
        except Exception:
            return None # no alignment with tree

    def enrich_tree(self, a_tree):

        initial_string_tokens = on.common.util.make_sgml_unsafe(self.string).split()

        try:
            if self.start_token_index is not None and self.end_token_index is not None:

                self.start_leaf = a_tree.get_leaf_by_token_index(self.start_token_index)
                self.end_leaf = a_tree.get_leaf_by_token_index(self.end_token_index)

            elif self.start_word_index is not None and self.end_word_index is not None:

                self.start_leaf = a_tree.get_leaf_by_word_index(self.start_word_index)
                self.end_leaf = a_tree.get_leaf_by_word_index(self.end_word_index)

            else:
                raise Exception("Cannot enrich tree because indicies are not defined.  Have only sti=%s, eti=%s, swi=%s, ewi=%s." % (
                    self.start_token_index, self.end_token_index, self.start_word_index, self.end_word_index))

        except KeyError:
            self.start_leaf = None
            self.end_leaf = None

        if not self.start_leaf or not self.end_leaf:
            self.valid = False
            on.common.log.report(self.__sgml_span_name, "dropping annotation for not having a start/end leaf", tree=a_tree.pretty_print(), tid=a_tree.id, ist=initial_string_tokens)
            #raise Exception("breakpoint")

        elif self.start_leaf and self.end_leaf and not self.subtree:
            if all(a_leaf.tag == "XX" for a_leaf in a_tree):
                """ this is a dummy tree and we should be ignoring it for alignment purposes """
                pass
            else:
                a_start_leaf, a_end_leaf = self.start_leaf.get_root().get_closest_aligning_span(self.start_leaf, self.end_leaf)

                if not a_start_leaf or not a_end_leaf:
                    # lets be even laxer
                    a_start_leaf, a_end_leaf = self.start_leaf.get_root().get_closest_aligning_span(self.start_leaf, self.end_leaf, strict_alignment=False)

                if a_start_leaf and a_end_leaf:
                    self.start_leaf = a_start_leaf
                    self.end_leaf = a_end_leaf

        found_tokens = 0
        for a_token in initial_string_tokens:
            for a_leaf in a_tree:
                if (a_leaf.get_word(buckwalter=True, vocalized=False) == a_token or
                    a_leaf.get_word(buckwalter=True, vocalized=True) == a_token or
                    a_leaf.get_word(buckwalter=False, vocalized=False) == a_token or
                    a_leaf.get_word(buckwalter=False, vocalized=True) == a_token):

                    found_tokens += 1
                    break

        if (found_tokens != len(initial_string_tokens) and

            # don't generate this report if we're on dummy trees
            not all(a_leaf.tag == "XX" for a_leaf in a_tree)):

            def report_alignment_failed(subreason):

                on.common.log.report(self.__sgml_span_name, "alignment failed %s" % subreason, document_id=a_tree.document_id, tree_string=a_tree.get_word_string(),
                                     tokens=initial_string_tokens, tokens_b=" ".join(str(t) for t in initial_string_tokens),
                                     tree_string_bkw_unv = a_tree.get_word_string(buckwalter=True, vocalized=False))

            if not found_tokens:
                if "".join(initial_string_tokens) not in a_tree.get_word_string().replace(" ", ""):
                    """ ignoring tokenization, if we don't match at all, that's quite bad """
                    report_alignment_failed("badly")
                else:
                    """ this means we match, sort of, but tokenization off """
                    report_alignment_failed("somewhat badly")
            else:
                """ this means some of our tokens match fully, but for others we do not match """
                report_alignment_failed("somewhat")
