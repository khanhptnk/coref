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
# KIND.  THE SOFTWARE IS PROVIDED FOR RESEARCH PURPOSES ONLY. AS SUCH,
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
:mod:`sense` -- Word Sense Annotation
------------------------------------------------

See:

 - :class:`on.corpora.sense.on_sense`
 - :class:`on.corpora.sense.sense_bank`
 - :class:`on.corpora.sense.on_sense_type`

Word sense annotation consists of specifying which sense a word is
being used in.  In the ``.sense`` file format, a word sense would
be annotated as:

.. code-block: bash

  bn/cnn/00/cnn_0001@all@cnn@bn@en@on 6 9 fire-n ?,? 4

This tells us that word 9 of sentence 6 in broadcast news document
cnn_0001 has the lemma "fire", is a noun, and has sense 4.  The
sense numbers, such as 4, are defined in the sense inventory files.
Looking up sense 4 of fire-n in
``data/english/metadata/sense-inventories/fire-n.xml``, we see:

.. code-block:: xml

  <sense n="4" type="Event" name="the discharge of a gun" group="1">
    <commentary>
      FIRE[+event][+physical][+discharge][+gun]
      The event of a gun going off.
    </commentary>
    <examples>
      Hold your fire until you see the whites of their eyes.
      He ran straight into enemy fire.
      The marines came under heavy fire when they stormed the hill.
    </examples>
    <mappings><wn version="2.1">2</wn><omega></omega><pb></pb></mappings>
    <SENSE_META clarity=""/>
  </sense>

Just knowing that word 9 of sentence 6 in some document has some
sense is not very useful on its own.  We need to match this data
with the document it was annotated against.  The python code can do
this for you.  First, load the data you're interested in, to memory
with :mod:`on.corpora.tools.load_to_memory`.  Then we can
iterate over all the leaves to look for cases where a_leaf was
tagged with a noun sense "fire":

.. code-block:: python

  fire_n_leaves = []
  for a_subcorpus in a_ontonotes:
     for a_tree_document in a_subcorpus["tree"]:
        for a_tree in a_tree_document:
           for a_leaf in a_tree.leaves():
              if a_leaf.on_sense: # whether the leaf is sense tagged
                 if a_leaf.on_sense.lemma == "fire" and a_leaf.on_sense.pos == "n":
                    fire_n_leaves.append(a_leaf)

Now say we want to print the sentences for each tagged example of
"fire-n":

.. code-block:: python

  # first we collect all the sentences for each sense of fire
  sense_to_sentences = defaultdict(list)
  for a_leaf in fire_n_leaves:
     a_sense = a_leaf.on_sense.sense
     a_sentence = a_leaf.get_root().get_word_string()
     sense_to_sentences[a_sense].append(a_sentence)

  # then we print them
  for a_sense, sentences in sense_to_sentences.iteritems():
     a_sense_name = on_sense_type.get_name("fire", "n", a_sense)

     print "Sense %s: %s" % (a_sense, a_sense_name)
     for a_sentence in sentences:
        print "  ", a_sentence

     print ""

Correspondences:

 ===============================  ==============================  ====================================================================================
 **Database Tables**              **Python Objects**              **File Elements**
 ===============================  ==============================  ====================================================================================
 ``sense_bank``                   :class:`sense_bank`             All ``.sense`` files in a :class:`on.corpora.subcorpus`
 None                             :class:`sense_tagged_document`  A single ``.sense`` file
 ``on_sense``                     :class:`on_sense`               A line in a ``.sense`` file
 None                             :class:`sense_inventory`        A sense inventory xml file (SI)
 ``on_sense_type``                :class:`on_sense_type`          Fields four and six of a sense line and the ``inventory/sense`` element of a SI
 ``on_sense_lemma_type``          :class:`on_sense_lemma_type`    The ``inventory/ita`` element of a SI
 ``wn_sense_type``                :class:`wn_sense_type`          The ``inventory/sense/mappings/wn`` element of a SI
 ``pb_sense_type``                :class:`pb_sense_type`          The ``inventory/sense/mappings/pb`` element of a SI
 ``tree``                         :class:`on.corpora.tree.tree`   The first three fields of a sense line
 ===============================  ==============================  ====================================================================================

Classes:

.. autoclass:: sense_bank
.. autoclass:: sense_tagged_document
.. autoclass:: on_sense
.. autoclass:: on_sense_type
.. autoclass:: on_sense_lemma_type
.. autoclass:: sense_inventory
.. autoclass:: pb_sense_type
.. autoclass:: wn_sense_type

"""

#---- standard python imports ----#
from __future__ import with_statement
import operator
import os.path

try:
    import MySQLdb
except ImportError:
    pass

import string
import sys
import re
import exceptions
import codecs

#---- xml specific imports ----#
from xml.etree import ElementTree
import xml.etree.cElementTree as ElementTree




#---- custom package imports ----#
import on

import on.common.log
import on.common.util

import on.corpora
import on.corpora.tree
import on.corpora.proposition
import on.corpora.coreference
import on.corpora.name

from collections import defaultdict
from on.common.util import is_db_ref, is_not_loaded, insert_ignoring_dups, esc

from on.corpora import abstract_bank


#--------------------------------------------------------------------------------#
# this is a class of objects that contain just enough information that
# can be used to locate the actual object, and change the properties
# of the actual object if required -- such as the wordnet and frame
# sense mappings, which are not really class variables, but then we
# also don't want to replicate them all over the place, and also be
# able to manipulate that information using back-pointers to the actual
# objects pool is required
#--------------------------------------------------------------------------------#
class on_sense(object):
    """ A sense annotation; a line in a ``.sense`` file.

    Contained by: :class:`sense_tagged_document`

    Attributes:

        .. attribute:: lemma

          Together with the :attr:`pos` , a reference to a
          :class:`sense_inventory` .

        .. attribute:: pos

          Either ``n`` or ``v``.  Indicates whether this leaf was annotated
          by people who primarily tagged nouns or verbs.  This should
          agree with :meth:`on.corpora.tree.tree.is_noun` and
          :meth:`~on.corpora.tree.tree.is_verb` methods for English and Arabic,
          but not Chinese.

        .. attribute:: sense

          Which sense in the :class:`sense_inventory` the annotators gave
          this leaf.

    """

    def __init__(self, document_id, tree_index, word_index, lemma, pos, ann_1_sense,
                 ann_2_sense, adj_sense, sense, adjudicated_flag, a_cursor=None, indexing="word"):
        self.lemma = lemma
        self.pos = pos
        self.ann_1_sense = ann_1_sense
        self.ann_2_sense = ann_2_sense
        self.adj_sense = adj_sense
        self.adjudicated_flag = adjudicated_flag
        self.enc_sense = None

        self.valid = True

        #--------------------------------------------------------------#
        # this is the final sense associated with this lemma.  it is
        # either the adjudicated sense, or the sense agreed upon by
        # both annotators.  if it is None, that means this is an
        # unadjudicated disagreement, and should probably not ever be
        # a part of the corpus.
        #--------------------------------------------------------------#
        self.sense = sense

        self._word_index = None
        self._token_index = None
        self._tree_index = tree_index
        self._document_id = document_id
        self._leaf = None # none until successful enrichment

        if(self.sense == None):
            on.common.log.error("a None sense should not be added to the corpus")

        self.sense_list = self.sense.split("&&")   #---- we will always consider a sense to be a part of multiple senses

        if indexing == "word" or (indexing == "ntoken_vword" and pos == 'v') or (indexing == "nword_vtoken" and pos == 'n'):
            self.word_index = word_index
        elif indexing == "token" or (indexing == "ntoken_vword" and pos == 'n') or (indexing == "nword_vtoken" and pos == 'v'):
            self.token_index = word_index
        else:
            raise Exception("sense indexing must be 'word', 'token', 'ntoken_vword', or 'ntoken_vword'.  Given %s" % lemma_index)


    def _get_word_index(self):
        if self.leaf:
            return self.leaf.get_word_index()
        return self._word_index

    def _set_word_index(self, idx):
        if idx is not None:
            idx = int(idx)
        if self.leaf:
            raise Exception("Cannot set on_sense's word index after enrichment.  Set on_sense.leaf instead.")
        elif self.token_index is not None:
            raise Exception("Tried to set on_sense.word_index when on_sense.token_index was already set.")
        self._word_index = idx

    word_index = property(_get_word_index, _set_word_index)

    def _get_token_index(self):
        if self.leaf:
            return self.leaf.get_token_index()
        return self._token_index

    def _set_token_index(self, idx):
        if idx is not None:
            idx = int(idx)

        if self.leaf:
            raise Exception("Cannot set on_sense's token index after enrichment.  Set on_sense.leaf instead.")
        elif self.word_index is not None:
            raise Exception("Tried to set on_sense.token_index when on_sense.word_index was already set.")
        self._token_index = idx

    token_index = property(_get_token_index, _set_token_index)

    def _get_tree_index(self):
        if self.leaf:
            return self.leaf.get_sentence_index()
        return self._tree_index

    def _set_tree_index(self, idx):
        if self.leaf:
            raise Exception("Cannot set on_sense's tree index after enrichment.  Set on_sense.leaf instead.")
        self._tree_index = idx

    tree_index = property(_get_tree_index, _set_tree_index)

    def _get_leaf(self):
        return self._leaf

    def _set_leaf(self, new_leaf):
        if new_leaf is None and self.leaf is not None:
            self._token_index = None
            self._word_index = self.word_index
            self._tree_index = self.tree_index
            self._document_id = self.document_id

        if new_leaf is not None:
            new_leaf.on_sense = self
        if self.leaf is not None:
            self.leaf.on_sense = None

        self._leaf = new_leaf

    leaf = property(_get_leaf, _set_leaf)

    def _get_document_id(self):
        if self.leaf:
            return self.leaf.get_document_id()
        return self._document_id

    def _set_document_id(self, new_document_id):
        if self.leaf:
            raise Exception("Cannot set on_sense's document_id after enrichment.  Set on_sense.leaf instead.")
        self._document_id = new_document_id

    document_id = property(_get_document_id, _set_document_id)

    @property
    def id(self):
        return "%s.%s@%s@%s@%s@%s" % (self.lemma, self.sense, self.pos, self.word_index, self.tree_index, self.document_id)


    def sense_annotation(self, preserve_ita=False):
        """ return all sense components after the pointer """

        if not preserve_ita or not self.enc_sense:
            lp = "%s-%s" % (self.lemma, self.pos)

            if self.adjudicated_flag:
                return "%s ?,? %s" % (lp, self.sense)
            elif self.ann_2_sense != "?":
                return "%s %s,%s" % (lp, self.sense, self.sense)
            else:
                return "%s %s" % (lp, self.sense)

        return " ".join(self.enc_sense.split()[3:])


    @property
    def primary_index(self):
        if self.word_index is not None:
            return self.word_index
        return self.token_index

    def enrich_tree(self, a_tree):
        try:
            if self.word_index is not None:
                self.leaf = a_tree.get_leaf_by_word_index(self.word_index)
            elif self.token_index is not None:
                self.leaf = a_tree.get_leaf_by_token_index(self.token_index)
            else:
                raise KeyError("No index available")
        except KeyError, e:
            self.valid = False

    def __repr__(self):
        return "<on_sense object: id: %s; tree_index: %s; index: %s; lemma: %s; pos: %s; ann_1_sense: %s; ann_2_sense: %s; adj_sense: %s sense: %s>" % (
            self.id, self.tree_index, self.primary_index, self.lemma, self.pos, self.ann_1_sense, self.ann_2_sense, self.adj_sense, self.sense)


    sql_table_name = "on_sense"
    sql_create_statement = \
"""
create table on_sense
(
  id varchar(255) not null,
  lemma varchar(255),
  pos varchar(255),
  ann_1_sense varchar(255),
  ann_2_sense varchar(255),
  adj_sense varchar(255),
  sense varchar(255),
  adjudicated_flag int,
  word_index int,
  tree_index int,
  document_id varchar(255),
  foreign key (document_id) references document.id
)
default character set utf8;
"""


    sql_insert_statement = \
"""insert into on_sense
(
  id,
  lemma,
  pos,
  ann_1_sense,
  ann_2_sense,
  adj_sense,
  sense,
  adjudicated_flag,
  word_index,
  tree_index,
  document_id
) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

    def write_to_db(self, cursor):

        data = []

        for a_sense in self.sense_list:

            if self.word_index is None:
                if self.token_index is not None:
                    raise Exception("Cannot write token indexed sense files to db until after enrichment")
                raise Exception("Cannot write senses to db if they are not indexed")

            a_tuple = (self.id, \
                       self.lemma, \
                       self.pos, \
                       "?", \
                       "?", \
                       "?", \
                       a_sense, \
                       self.adjudicated_flag, \
                       self.word_index, \
                       self.tree_index, \
                       self.document_id)

            data.append(a_tuple)

        #---- insert the value in the table ----#
        cursor.executemany("%s" % (self.__class__.sql_insert_statement), data)


    def pb_mappings(self, a_sense_bank):
        """ -> [(pb_lemma, pb_sense_num), ...] or None if no mapping """
        return a_sense_bank.pb_mappings(self.lemma, self.pos, self.sense)



class wn_sense_type(on.corpora.abstract_open_type_table):
    """ a wordnet sense, for mapping ontonotes senses to wordnet senses

    Contained by: :class:`on_sense_type`
    """


    type_hash = defaultdict(int)
    wn_sense_num_hash = {}
    wn_version_hash = {}
    pos_hash = {}
    lemma_hash = {}

    def __init__(self, lemma, wn_sense_num, pos, wn_version):
        self.lemma = lemma             #---- this is the lemma for which the sense is defined
        self.wn_sense_num = wn_sense_num #---- the wn sense number
        self.wn_version = wn_version
        self.pos = pos

        on.corpora.abstract_open_type_table.__init__(self, "%s.%s@%s@%s" % (self.lemma, self.wn_sense_num, self.pos, self.wn_version))

        self.wn_sense_num_hash[self.id] = wn_sense_num
        self.wn_version_hash[self.id] = wn_version
        self.pos_hash[self.id] = pos
        self.lemma_hash[self.id] = lemma


    def __repr__(self):
        return "<wn_sense_type object: lemma: %s; wn_sense_num: %s; pos: %s; wn_version: %s>" % (self.lemma, self.wn_sense_num, self.pos, self.wn_version)


    sql_table_name = "wn_sense_type"
    sql_create_statement = \
"""
create table wn_sense_type
(
  id varchar(255) not null collate utf8_bin primary key,
  lemma varchar(255) not null,
  wn_sense varchar(255) not null,
  pos varchar(255) not null,
  wn_version varchar(255) not null
)
default character set utf8;
"""


    sql_insert_statement = \
"""insert into wn_sense_type
(
  id,
  lemma,
  wn_sense,
  pos,
  wn_version
) values (%s, %s, %s, %s, %s)
"""

    @classmethod
    def write_to_db(cls, cursor):

        for a_type in cls.type_hash.keys():
            insert_ignoring_dups(cls, cursor, a_type, cls.lemma_hash[a_type],
                                 cls.wn_sense_num_hash[a_type],
                                 cls.pos_hash[a_type], cls.wn_version_hash[a_type])

class pb_sense_type(on.corpora.abstract_open_type_table):
    """ A frame sense

    Contained by: :class:`on.corpora.proposition.frame_set`, :class:`on_sense_type`

    """

    type_hash = defaultdict(int)

    def __init__(self, lemma, num):
        self.lemma = lemma             #---- this is the lemma for which the sense is defined
        self.num = num #---- this is the frame sense number

        on.corpora.abstract_open_type_table.__init__(self, "%s.%s" % (self.lemma, self.num))

    def __repr__(self):
        return "<pb_sense_type object: lemma: %s; num: %s>" % (self.lemma, self.num)


    sql_table_name = "pb_sense_type"
    sql_create_statement = \
"""
create table pb_sense_type
(
  id varchar(255) not null collate utf8_bin primary key
)
default character set utf8;
"""


    sql_insert_statement = \
"""insert into pb_sense_type
(
  id
) values (%s)
"""


class on_sense_type(on.corpora.abstract_open_type_table):
    """ Information to interpret :class:`on_sense` annotations

    Contained by: :class:`sense_inventory`

    Attributes:

        .. attribute:: lemma
        .. attribute:: sense_num
        .. attribute:: pos

           Either 'n' or 'v', depending on whether this is a noun sense or a verb sense.

        .. attribute:: wn_sense_types

           list of :class:`wn_sense_type` instances

        .. attribute:: pb_sense_types

           list of :class:`pb_sense_type` instances (frame senses)

        .. attribute:: sense_type

           the type of the sense, such as 'Event'

    Methods:

        .. automethod:: get_name

    """

    type_hash = defaultdict(int)
    eventive_hash = {}
    nominalization_hash = {}
    name_hash = {}
    sense_number_hash = {}
    sense_pos_hash = {}
    sense_lemma_hash = {}

    def __init__(self, lemma, pos, group, sense_num, name, sense_type):
        self.lemma = lemma             #---- this is the lemma for which this sense is defined
        self.pos = pos                 #---- pos of the lemma
        self.group = group
        self.sense_num = sense_num   #---- this is the sense number or id
        self.sense_type = sense_type
        self.name = name

        if(self.lemma == None or
           self.pos == None or
           self.group == None or
           self.sense_num == None or
           self.sense_type == None or
           self.name == None):
            on.common.log.warning("problem with sense definition for %s-%s" % (self.lemma, self.pos))

        self.wn_sense_types = []       #---- a list of wn sense objects that form this sense
        self.pb_sense_types = []       #---- a list of frame sense objects that form this sense
        self.commentary = ""           #---- a commentary
        self.examples = []             #---- a list of examples

        on.corpora.abstract_open_type_table.__init__(self, "%s@%s@%s" % (self.lemma, self.sense_num, self.pos))


        self.eventive_flag = False
        if(self.sense_type == "Event"):
            self.eventive_flag = True
            self.eventive_hash[self.id] = 1
        else:
            self.eventive_hash[self.id] = None

        self.nominalization_hash[self.id] = None

        self.sense_number_hash[self.id] = sense_num

        self.sense_pos_hash[self.id] = pos

        self.sense_lemma_hash[self.id] = lemma

        self.name_hash[self.id] = name


    @classmethod
    def get_name(cls, a_lemma, a_pos, a_sense):
        """ given a lemma, pos, and sense number, return the name from the sense inventory """

        candidate_ids = [a_id for (a_id, lemma) in cls.sense_lemma_hash.iteritems()
                         if (lemma == a_lemma and cls.sense_pos_hash[a_id] == a_pos and \
                                                  cls.sense_number_hash[a_id] == a_sense)]

        if not candidate_ids:
            return None

        if len(candidate_ids) > 1:
            on.common.log.report("sense", "duplicate on_sense_types",
                                 lemma=a_lemma, pos=a_pos, sense=a_sense)

        return cls.name_hash[candidate_ids[0]]


    def __repr__(self):

        wn_sense_type_string = ""
        for wn_sense_type in self.wn_sense_types:
            wn_sense_type_string = wn_sense_type_string + "\n\t\t" + "%s" % (wn_sense_type)

        if(wn_sense_type_string == ""):
            wn_sense_type_string = "<None>"

        pb_sense_type_string = ""
        for pb_sense_type in self.pb_sense_types:
            pb_sense_type_string = pb_sense_type_string + "\n\t\t" + "%s" % (pb_sense_type)

        if(pb_sense_type_string == ""):
            pb_sense_type_string = "<None>"

        return "<on_sense_type object:\n\tlemma: %s;\n\tpos: %s;\n\tgroup: %s;\n\tsense_num: %s;\n\tname: %s;\n\tsense_type: %s;\n\twn_sense_types: %s;\n\tpb_sense_types: %s;\n\tcommentary: %s....;\n\texamples: %s....>" % (self.lemma, self.pos, self.group, self.sense_num, self.name, self.sense_type, wn_sense_type_string, pb_sense_type_string, self.commentary.strip()[0:10], str(self.examples).strip()[0:10])


    sql_table_name = "on_sense_type"
    sql_create_statement = \
"""
create table on_sense_type
(
  id varchar(255) not null collate utf8_bin primary key,
  lemma varchar(255) not null,
  sense_num varchar(255) not null,
  pos varchar(255) not null,
  eventive_flag varchar(255) default null,
  nominalization_flag varchar(255) default null,
  name text
)
default character set utf8;
"""
    wn_sql_table_name = "on_sense_type_wn_sense_type"
    wn_sql_create_statement = \
"""
create table on_sense_type_wn_sense_type
(
  on_sense_type varchar(255) not null,
  wn_sense_type varchar(255),
  unique key(on_sense_type),
  foreign key (on_sense_type) references on_sense_type.id,
  foreign key (wn_sense_type) references wn_sense_type.id
)
default character set utf8;
"""

    pb_sql_table_name = "on_sense_type_pb_sense_type"
    pb_sql_create_statement = \
"""
create table on_sense_type_pb_sense_type
(
  on_sense_type varchar(127) not null,
  pb_sense_type varchar(127),
  unique key(on_sense_type, pb_sense_type),
  foreign key (on_sense_type) references on_sense_type.id,
  foreign key (pb_sense_type) references pb_sense_type.id
)
default character set utf8;
"""


    sql_insert_statement = \
"""insert into on_sense_type
(
  id,
  lemma,
  sense_num,
  pos,
  eventive_flag,
  nominalization_flag,
  name
) values (%s, %s, %s, %s, %s, %s, %s)
"""


    wn_sql_insert_statement = \
"""insert into on_sense_type_wn_sense_type
(
  on_sense_type,
  wn_sense_type
) values (%s, %s)
"""

    pb_sql_insert_statement = \
"""insert into on_sense_type_pb_sense_type
(
  on_sense_type,
  pb_sense_type
) values (%s, %s)
"""

    @classmethod
    def write_to_db(cls, cursor):
        for a_type in cls.type_hash.keys():
            insert_ignoring_dups(cls, cursor,
                 a_type, cls.sense_lemma_hash[a_type],
                 cls.sense_number_hash[a_type],
                 cls.sense_pos_hash[a_type],
                 cls.eventive_hash[a_type],
                 cls.nominalization_hash[a_type],
                 cls.name_hash[a_type])

    def write_instance_to_db(self, b_key, cursor):

        for a_pb_sense_type in self.pb_sense_types:
            insert_ignoring_dups(self.__class__.pb_sql_insert_statement, cursor, b_key, a_pb_sense_type.id)

        for a_wn_sense_type in self.wn_sense_types:
            insert_ignoring_dups(self.__class__.wn_sql_insert_statement, cursor,
                                 b_key, a_wn_sense_type.id)
            #a_wn_sense_type.write_to_db(cursor)


class on_sense_lemma_type(on.corpora.abstract_open_type_table):
    """ computes and holds ita statistics for a lemma/pos combination

    """

    type_hash = defaultdict(int)
    count = defaultdict(int)
    ann_1_2_agreement = defaultdict(int)

    lemma_pos_hash = {}

    def __init__(self, a_on_sense):
        lemma = a_on_sense.lemma
        pos = a_on_sense.pos

        on.corpora.abstract_open_type_table.__init__(self, "%s@%s" % (lemma, pos))

        if a_on_sense.ann_1_sense != '?' and a_on_sense.ann_2_sense != '?':
            """ if we have the individual annotator information, update the other counts -- otherwise, make no changes """

            ann_1_2_agree = False

            ann_1_2_agree = (a_on_sense.ann_1_sense == a_on_sense.ann_2_sense == a_on_sense.sense)

            if ann_1_2_agree:
                self.__class__.ann_1_2_agreement[self.id] += 1
            self.__class__.count[self.id] += 1

        self.__class__.lemma_pos_hash[self.id] = lemma, pos

    @classmethod
    def write_to_db(cls, cursor):
        for id, (lemma, pos) in cls.lemma_pos_hash.iteritems():
            insert_ignoring_dups(cls, cursor, id, lemma, pos, 0, 0)

        for counter, c_name in [[cls.count,             "count"],
                                [cls.ann_1_2_agreement, "ann_1_2_agreement"]]:
            for id, val in counter.iteritems():
                cursor.execute("update on_sense_lemma_type set %s=%s+%s where id='%s'" % esc(
                    c_name, c_name, val, id))

    @classmethod
    def write_to_db_quick(cls, lemma, pos, ita_dict, cursor):
        id = lemma + "@" + pos

        cursor.execute("select * from on_sense_lemma_type where id='%s'" % id)
        if cursor.fetchall():
            on.common.log.report("sense", "ita already in db", lemma=lemma, pos=pos)
            return

        cursor.executemany("%s" % (cls.sql_insert_statement), [
            esc(id, lemma, pos,
                ita_dict["count"],
                ita_dict["ann_1_2_agreement"])])

    @staticmethod
    def update_sense_inventory_detail(a_sense_inventory_fname, count, agreement):
        count = int(count)
        agreement = float(agreement)

        if agreement >= 1:
            agreement = agreement/count

        ita_attributes = 'count="%s" ann_1_2_agreement="%s"' % (count, agreement)

        with codecs.open(a_sense_inventory_fname, "r", "utf8") as inf:
            in_lines = inf.readlines()


        out_lines = []
        set_ita = False
        for line in in_lines:
            if "<ita " in line:

                if set_ita:
                    on.common.log.report("sense", "duplicate sense inventory ita field", fname=a_sense_inventory_fname, line=line)
                    return

                before, ita_and_after = line.split("<ita")
                ita, after = ita_and_after.split(" />", 1)

                line = "%s<ita %s />%s" % (before, ita_attributes, after)
                set_ita = True

            if "</inventory>" in line and not set_ita:
                before, after = line.split("</inventory>")
                out_lines.append(before + "\n")
                out_lines.append("<ita %s />\n" % ita_attributes)
                line = "</inventory>" + after
                set_ita = True

            out_lines.append(line)

        if not set_ita:
            on.common.log.report("sense", "no close inventory tag in file; no ita info stored", fname=a_sense_inventory_fname)

        with codecs.open(a_sense_inventory_fname, "w", "utf8") as outf:
            outf.writelines(out_lines)


    @staticmethod
    def update_sense_inventory(a_sense_inventory_fname, a_cursor):
        """ add sections to the sense_inventory file that correspond
        to the fields in this table, or update them if already
        present.

        The general form is:

        .. code-block:: xml

           <inventory lemma="foo">
           ...
           <ita (fieldname="value")* />
           </inventory>

        That is, we're adding an ita element at the very end of the
        inventory which has an attribute for each numeric field
        in this table.  With the current set of fields, this would
        be:

        .. code-block:: xml

          <ita count="N_1" ann_1_2_agreement="N_2" />

        """

        lemma, pos = sense_inventory.extract_lemma_pos(a_sense_inventory_fname)
        if not lemma or not pos:
            return

        try:
            a_cursor.execute("""SELECT count, ann_1_2_agreement FROM on_sense_lemma_type WHERE lemma = '%s' AND pos = '%s';""" % esc(lemma, pos))
        except Exception:
            raise

        rows = a_cursor.fetchall()
        if not rows:
            on.common.log.report("sense", "unused sense inventory file", lemma=lemma, pos=pos, fname=a_sense_inventory_fname)
            return
        if len(rows) != 1:
            raise Exception("Multiple rows found for lemma %s with pos %s in on_sense_lemma_type" % (lemma, pos))

        a_row = rows[0]

        on_sense_lemma_type.update_sense_inventory_detail(a_sense_inventory_fname, a_row["count"], a_row["ann_1_2_agreement"])

    sql_table_name = "on_sense_lemma_type"
    sql_create_statement = \
"""
create table on_sense_lemma_type
(
  id varchar(255) not null collate utf8_bin primary key,
  lemma varchar(255) not null,
  pos varchar(255) not null,
  count int not null,
  ann_1_2_agreement float not null
)
default character set utf8;
"""


    sql_insert_statement = \
"""insert into on_sense_lemma_type
(
  id,
  lemma,
  pos,
  count,
  ann_1_2_agreement
) values (%s, %s, %s, %s, %s)
"""


class sense_inventory:
    """ Contains: :class:`on_sense_type` """

    IS_VN_REF_RE = re.compile("[a-z_]+(-[0-9]+(\.[0-9]+)*)+") # things that should be in the <vn> element

    def __init__(self, a_fname, a_xml_string, a_lang_id, a_frame_set_hash={}):




        self.sense_hash = {}
        self.lemma = None
        self.pos = None
        self.lang_id = a_lang_id

        def complain(reason, *args):
            pos = "n" if a_fname.endswith("-n.xml") else "v"


        def drop(reason, *args):
            complain(reason, *args)
            raise Exception(reason)

        #---- create a DOM object for the xml string ----#
        try:
            a_inventory_tree = ElementTree.fromstring(a_xml_string)
        except Exception, e:
            drop("problem reading sense inventory xml file", ["error", e])

        a_lemma_attribute = on.common.util.make_sgml_unsafe(on.common.util.get_attribute(a_inventory_tree, "lemma"))
        a_lemma_attribute = a_lemma_attribute.split()[-1]

        if "-" not in a_lemma_attribute:
            #drop("lemma attribute of sense inventory doesn't have 2 bits",
            #     ["lemma", a_lemma_attribute])
            self.lemma = a_lemma_attribute
            self.pos = "v"
        else:
            self.lemma, self.pos = a_lemma_attribute.rsplit("-", 1)

        if(self.lemma == ""):
            drop("sense inv lemma is undefined")

        match_fname = os.path.splitext(a_fname)[0]
        lemma_pos = '-'.join([self.lemma, self.pos])
        if lemma_pos != match_fname:
            complain("sense inv lemma doesn't match fname", ["lemma_pos", lemma_pos], ["match_fname", match_fname])

        self.ita_dict = {}

        ita_trees = a_inventory_tree.findall(".//ita")
        if ita_trees:
            if len(ita_trees) != 1:
                complain("sense inv has mulitple ita segments")
            else:
                self.ita_dict = ita_trees[0].attrib


        on.common.log.debug("processing inventory element: %s" % (a_lemma_attribute),
                            on.common.log.DEBUG, on.common.log.MAX_VERBOSITY)

        #---- get the sense elements in this inventory ----#
        i=0
        for a_sense_tree in a_inventory_tree.findall(".//sense"):
            #---- lets get the attributes of this sense element ----#
            a_group = on.common.util.get_attribute(a_sense_tree, "group")
            n     = on.common.util.get_attribute(a_sense_tree, "n")
            a_name  = on.common.util.get_attribute(a_sense_tree, "name")
            a_sense_type  = on.common.util.get_attribute(a_sense_tree, "type")

            a_on_sense_type = on_sense_type(self.lemma, self.pos, a_group, n, a_name, a_sense_type)


            #---- get the on sense commentary ----#
            for a_commentary_tree in a_sense_tree.findall(".//commentary"):

                a_commentary = a_commentary_tree.text
                a_on_sense_type.commentary = a_commentary


            #---- get the on sense examples ----#
            for a_examples_tree in a_sense_tree.findall(".//examples"):
                if a_examples_tree.text:
                    a_examples = a_examples_tree.text.split("\n")
                else:
                    a_examples = []
                a_on_sense_type.examples = a_examples


            #--------------------------------------------------------------------------------#
            # as all the three -- wn, omega and pb mapping are in
            # this one element, we will process them one by one.
            # however, there is no mapping with omega elements at
            # this point
            #--------------------------------------------------------------------------------#

            k=0
            for a_mapping_tree in a_sense_tree.findall(".//mappings"):

                if self.lang_id.startswith("en"):

                    #---- now get the wn elements ----#
                    for a_wn_tree in a_mapping_tree.findall(".//wn"):

                        if(a_wn_tree.attrib.has_key("lemma")):
                            a_wn_lemma = on.common.util.get_attribute(a_wn_tree, "lemma")
                        else:
                            #---- using the default lemma ----#
                            #---- there is an assumption that the wordnet lemma is the same as the one for the inventory, if it is not defined ----#
                            a_wn_lemma = self.lemma

                        if(a_wn_tree.attrib.has_key("version")):
                            a_wn_version = on.common.util.get_attribute(a_wn_tree, "version")

                            #---- check if it is indeed wordnet and not something else ----#
                            if(a_wn_version not in ["1.7", "2.0", "2.1"]):
                                continue

                        else:
                            a_wn_version = ""
                            complain("undefined wn version", ["wn_version", a_wn_version])

                        a_wn_description = a_wn_tree.text
                        if a_wn_description:
                            a_wn_description = a_wn_description.strip()

                        if a_wn_description:
                            if(a_wn_description.find(",") != -1):
                                a_wn_sense_nums = a_wn_description.split(",")
                            else:
                                a_wn_sense_nums = a_wn_description.split(" ")

                            for a_wn_sense_num in a_wn_sense_nums:
                                a_on_sense_type.wn_sense_types.append(wn_sense_type(
                                    a_wn_lemma, a_wn_sense_num, self.pos, a_wn_version))


                #---- now get the pb elements ----#
                for a_pb_tree in a_mapping_tree.findall(".//pb"):
                    a_pb_description = a_pb_tree.text

                    if a_pb_description and a_pb_description.strip():
                        for a_pb_sense_description in a_pb_description.split(","):
                            if self.IS_VN_REF_RE.match(a_pb_sense_description):
                                continue # ignore verbnet mappings

                            period_separated_bits = len(a_pb_sense_description.split("."))

                            if period_separated_bits == 2:
                                a_pb_lemma, a_pb_sense_num = a_pb_sense_description.split(".")
                                a_pb_lemma = a_pb_lemma.strip()

                                if on.corpora.proposition.proposition_bank.is_valid_frameset_helper(
                                    a_frame_set_hash, a_pb_lemma, "v", a_pb_sense_num):

                                    a_pb_sense_type = pb_sense_type(a_pb_lemma, a_pb_sense_num)
                                    a_on_sense_type.pb_sense_types.append(a_pb_sense_type)
                                else:
                                    complain("Bad frame reference", ["lemma", self.lemma], ["pb_ref", a_pb_sense_description])

                            elif a_pb_sense_description not in ["NM", "NP"]:
                                complain("Bad pb field",
                                         ["lemma", self.lemma],
                                         ["pb_desc", a_pb_description],
                                         ["sense number", n])


                if(self.sense_hash.has_key(a_on_sense_type.id)):
                    drop("sense inventories define this-sense multiple times", ["a_on_sense_type_id", a_on_sense_type.id])
                else:
                    self.sense_hash[a_on_sense_type.id] = a_on_sense_type


    def num_senses(self):
        return len(self.sense_hash.keys())


    def write_to_db(self, cursor):

        for b_key, a_on_sense_type in self.sense_hash.iteritems():
            a_on_sense_type.write_instance_to_db(b_key, cursor)

        if self.ita_dict:
            on_sense_lemma_type.write_to_db_quick(self.lemma, self.pos, self.ita_dict, cursor)

    @staticmethod
    def extract_lemma_pos(fname):
        """ given a filename representing a sense inventory file, return
        the defined lemma and pos for that file..
        """

        language = "unknown"
        for l in ["english", "chinese", "arabic"]:
            if l in fname:
                language = l

        pos = "n" if fname.endswith("-n.xml") else "v"

        lemma_pos = ""
        with codecs.open(fname, "r", "utf8") as f:
            for line in f:
                if '<inventory lemma=' in line:
                    lemma_pos = line.split('<inventory lemma="')[1].split('"')[0]

        if not lemma_pos:
            on.common.log.warning("no lemma field found for sense inventory file")
            return None, None

        lemma_pos = lemma_pos.strip().split()[-1]

        if lemma_pos.endswith("-n"):
            lemma, pos = lemma_pos[:-len("-n")], "n"
        elif lemma_pos.endswith("-v"):
            lemma, pos = lemma_pos[:-len("-v")], "v"
        else:
            lemma = lemma_pos

        return lemma, pos

class sense_tagged_document:
    """
    Contained by: :class:`sense_bank`

    Contains: :class:`on_sense`

    """

    def __init__(self, sense_tagged_document_string, document_id,
                 a_sense_bank, a_cursor=None, preserve_ita=False,
                 indexing="word"):
        on.common.log.debug("building document: %s" % (document_id), on.common.log.DEBUG, on.common.log.MAX_VERBOSITY)
        self.on_sense_list = []
        self.sense_tagged_document_string = sense_tagged_document_string
        self.document_id = document_id
        self.lemma_pos_hash = {}
        self.a_sense_bank = a_sense_bank
        self.preserve_ita = preserve_ita

        lang = document_id.split("@")[-2]

        if(a_cursor == None):
            #---- initialize the SENSE bank that contains skeletal sense information ----#
            for enc_sense in sense_tagged_document_string.split("\n"):

                def reject(errcode, *comments):
                    on.common.log.reject(["docid", self.document_id, "sense"], "sense", [[errcode, comments]], enc_sense)

                #--- if a blank line is found, just skip it ----#
                if(enc_sense.strip() == ""):
                    continue

                a_list = enc_sense.split()

                ann_1_sense, ann_2_sense = "?", "?"
                a_sense = None

                if len(a_list) == 6 and a_list[5] == "-1":
                    del a_list[5]

                try:
                    if len(a_list) == 4:
                        continue # just a pointer
                    elif len(a_list) == 5:
                        adjudicated_flag = 0
                        adj_sense = None

                        (document_id, tree_index, word_index, lemma_pos, senses) = a_list

                        if "," not in senses:
                            a_sense = senses
                        else:
                            (ann_1_sense, ann_2_sense) = senses.split(",")

                            if ann_1_sense != ann_2_sense:
                                if "&&" in ann_1_sense and ann_2_sense in ann_1_sense.split("&&"):
                                    a_sense = ann_2_sense
                                elif "&&" in ann_2_sense and ann_1_sense in ann_2_sense.split("&&"):
                                    a_sense = ann_1_sense

                            if not a_sense:
                                a_sense = ann_1_sense

                    elif len(a_list) == 6:
                        (document_id, tree_index, word_index, lemma_pos, senses, adj_sense) = a_list

                        ann_1_sense, ann_2_sense = senses.split(",")

                        a_sense = adj_sense
                        adjudicated_flag = 1
                    else:
                        reject("invfields")
                        continue

                except ValueError:
                    reject("invfields")
                    continue


                # treat ",1 1" as "1,1"
                # treat "1, 1" as "1,1"
                if ann_1_sense and adj_sense and not ann_2_sense:
                    ann_2_sense = adj_sense
                    adj_sense = None
                    adjudicated_flag = 0
                elif ann_2_sense and adj_sense and not ann_1_sense:
                    ann_1_sense = adj_sense
                    adj_sense = None
                    adjudicated_flag = 0
                elif not ann_1_sense or not ann_2_sense:
                    reject("blankfield")
                    continue

                if ann_1_sense == "-1":
                    ann_1_sense = "?"
                if ann_2_sense == "-1":
                    ann_2_sense = "?"

                lemma, pos = lemma_pos.rsplit("-", 1)

                if (not adjudicated_flag and
                    ann_2_sense not in [ann_1_sense, "?"] and
                    ann_1_sense not in ann_2_sense.split("&&") and
                    ann_2_sense not in ann_1_sense.split("&&")):

                    ann_2_sense = "?"

                if not a_sense:
                    reject("invsense")
                    continue

                a_sense = a_sense.split("&&")[0]



                #---- add lemma_pos to the hash ----#
                if(not self.lemma_pos_hash.has_key(lemma_pos)):
                    self.lemma_pos_hash[lemma_pos] = 0

                document_id = "%s" % (re.sub(".mrg", "", document_id))

                #---- at this point the document id does not have a .wsd ----#
                self.document_id = re.sub(".wsd", "", self.document_id)

                #---- do not perform this check for chinese files ----#
                if(lang != "ch"):
                    if(document_id != self.document_id.split("@")[0]):
                        on.common.log.warning("looks like the document id from filename (%s) and the id inside (%s) do not match" % (self.document_id.split("@")[0], document_id), on.common.log.MAX_VERBOSITY)

                valid_senses = a_sense_bank.list_valid_senses(lemma, pos)

                if not a_sense_bank.sense_inventories_loaded() or a_sense in valid_senses or (lang == "ch" and pos == "n"):
                #if True:
                    a_on_sense = on_sense(self.document_id, tree_index, word_index, lemma, pos, ann_1_sense, ann_2_sense,
                                          adj_sense, a_sense, adjudicated_flag, indexing=indexing)

                    a_on_sense.enc_sense = enc_sense

                    on.common.log.debug(enc_sense, on.common.log.DEBUG, on.common.log.MAX_VERBOSITY)
                    on.common.log.debug(a_on_sense, on.common.log.DEBUG, on.common.log.MAX_VERBOSITY)

                    self.on_sense_list.append(a_on_sense)

                    a_on_sense_lemma_type = on_sense_lemma_type(a_on_sense) # we just instantiate it; nothing else need happen
                else:
                    reject("invsense" if a_sense != "XXX" else "invsenseXXX",
                           "valid_senses=%s" % ", ".join(valid_senses) or "None",
                           "extracted_sense=%s" % a_sense)

    def __getitem__(self, index):
        return self.on_sense_list[index]

    def __len__(self):
        return len(self.on_sense_list)

    def __repr__(self):
        return "sense_tagged_document instance, id=%s, on_senses:\n%s" % (
            self.document_id, on.common.util.repr_helper(enumerate(a_on_sense.id for a_on_sense in self)))

    def write_to_db(self, cursor):
        for a_on_sense in self.on_sense_list:
            if a_on_sense.leaf:
                a_on_sense.write_to_db(cursor)

    def dump_view(self, a_cursor=None, out_dir=""):
        #---- write view file -----#
        with codecs.open(on.common.util.output_file_name(self.document_id, self.a_sense_bank.extension, out_dir), "w", "utf-8") as f:

            sense_tuples = [( a_on_sense.document_id, int(a_on_sense.tree_index),
                              int(a_on_sense.word_index), a_on_sense.sense_annotation(self.preserve_ita) )
                              for a_on_sense in self.on_sense_list if a_on_sense.leaf ]

            sense_tuples.sort()

            f.writelines([ "%s %s %s %s\n" % t for t in sense_tuples ])




class sense_bank(abstract_bank):
    """
    Extends: :class:`on.corpora.abstract_bank`

    Contains: :class:`sense_tagged_document`

    """

    def __init__(self, a_subcorpus, tag, a_cursor=None, extension="sense",
                 a_sense_inv_hash=None, a_frame_set_hash=None, indexing="word"):
        abstract_bank.__init__(self, a_subcorpus, tag, extension)

        self.lemma_pos_hash = {}
        self.sense_inventory_hash = a_sense_inv_hash if a_sense_inv_hash else {}

        if(a_cursor == None):
            if not self.sense_inventory_hash:
                self.sense_inventory_hash = self.build_sense_inventory_hash(
                    a_subcorpus.language_id, a_subcorpus.top_dir, self.lemma_pos_hash,
                    a_frame_set_hash)

            sys.stderr.write("reading the sense bank [%s] ..." % self.extension)
            for a_file in self.subcorpus.get_files(self.extension):
                sys.stderr.write(".")

                sense_tagged_document_id = "%s@%s" % (a_file.document_id, a_subcorpus.id)

                with codecs.open(a_file.physical_filename, "r", "utf-8") as sf:
                    a_sense_tagged_document = sense_tagged_document(sf.read(), sense_tagged_document_id, self, indexing=indexing)

                #---- update the lemma_hash ----#
                for a_lemma_pos in a_sense_tagged_document.lemma_pos_hash:
                    self.lemma_pos_hash[a_lemma_pos] = 0

                self.append(a_sense_tagged_document)

            sys.stderr.write("\n")

        else:
            if not self.sense_inventory_hash:
                self.sense_inventory_hash = on.common.util.make_db_ref(a_cursor)

    def sense_inventories_loaded(self):
        return not is_not_loaded(self.sense_inventory_hash)

    @staticmethod
    def build_sense_inventory_hash(lang_id, top_dir, lemma_pos_hash=None,
                                   a_frame_set_hash=None):
        """ read in all the sense inventories from disk and make a hash of :class:`sense_inventory` instances """


        sense_inv_hash = {}

        sys.stderr.write("reading the sense inventory files ....")
        sense_inv_dir = "%s/metadata/sense-inventories" % top_dir

        unproc_sense_invs = None
        for sense_inv_fname in [sfn for sfn in os.listdir(sense_inv_dir)
                                if sfn[-4:] == ".xml"]:

            try:

                full_sense_inv_fname = os.path.join(sense_inv_dir, sense_inv_fname)

                lemma, pos = sense_inventory.extract_lemma_pos(full_sense_inv_fname)
                a_lemma_pos = "%s-%s" % (lemma, pos)
                if lemma_pos_hash and not a_lemma_pos in lemma_pos_hash:
                    on.common.log.debug("skipping %s ...." % a_lemma_pos, on.common.log.DEBUG,
                                        on.common.log.MAX_VERBOSITY)
                    continue
                else:
                    on.common.log.debug("adding %s ...." % (a_lemma_pos), on.common.log.DEBUG,
                                    on.common.log.MAX_VERBOSITY)

                on.common.log.debug("processing %s ...." % (sense_inv_fname),
                                        on.common.log.DEBUG, on.common.log.MAX_VERBOSITY)
                sys.stderr.write(".")

                with codecs.open(full_sense_inv_fname, "r", "utf-8") as s_inv_f:
                    sense_inv_file_str = s_inv_f.read()

                    sense_inv_hash[a_lemma_pos] = on.corpora.sense.sense_inventory(
                        sense_inv_fname, sense_inv_file_str, lang_id, a_frame_set_hash)

            except Exception, e:
                on.common.log.report("senseinv", "sense inventory failed to load", fname=sense_inv_fname)

        sys.stderr.write("\n")
        return sense_inv_hash

    def pb_mappings(self, a_lemma, a_pos, a_sense):
        """ return [(pb_lemma, pb_sense), ...] or [] """

        if is_not_loaded(self.sense_inventory_hash):
            return []

        elif is_db_ref(self.sense_inventory_hash):
            a_cursor = self.sense_inventory_hash['DB']

            try:
                a_cursor.execute("""SELECT pb_sense_type
                                    FROM   on_sense_type_pb_sense_type
                                    WHERE  on_sense_type = '%s';""" % esc("@".join([a_lemma, a_sense, a_pos])))
            except Exception:
                raise

            return [row["pb_sense_type"].split(".") for row in a_cursor.fetchall()]

        else:
            lpos = "%s-%s" % (a_lemma, a_pos)
            if lpos not in self.sense_inventory_hash:
                return []

            senses = [sense for sense in self.sense_inventory_hash[lpos].sense_hash.itervalues() if sense.sense_num == a_sense]

            if len(senses) != 1:
                on.common.log.report("sense", "pb_mappings -- did not expect invalid sense here",
                                     lemma=a_lemma, pos=a_pos, sense=a_sense)
                return []

            pbs = senses[0].pb_sense_types
            return [(pb.lemma, pb.num) for pb in pbs]


    def list_valid_senses(self, a_lemma, a_pos):
        """ One can't just assume that senses 1 through
        lookup_senses(a_lemma, a_pos)-1 are the valid senses for a
        lemma/pos combination because there are allowed to be gaps.
        So we provide a helpful list of valid senses
        """

        if is_not_loaded(self.sense_inventory_hash):
            return []

        elif is_db_ref(self.sense_inventory_hash):
            a_cursor = self.sense_inventory_hash['DB']

            try:
                a_cursor.execute("""SELECT sense_num
                FROM   on_sense_type
                WHERE  lemma = '%s'
                AND pos = '%s';""" % esc(a_lemma, a_pos))
            except Exception:
                raise


            return [document_row["sense_num"]
                    for document_row in a_cursor.fetchall()]

        else:
            lpos = "%s-%s" % (a_lemma, a_pos)
            if lpos not in self.sense_inventory_hash:
                return []

            senses = self.sense_inventory_hash[lpos].sense_hash

            return [senses[sense_id].sense_num for sense_id in senses]



    def enrich_treebank_helper(self, a_on_sense, a_tree_document, lang_id, ignore_lemma_mismatches):
        def reject(errcode, *comments):
            on.common.log.reject(["docid", a_tree_document.document_id, "sense"], "sense",
                                 [[errcode, comments]], a_on_sense.enc_sense)
            dropme = (on.common.log.ERRS["sense"][1][errcode][0][0] == "5")
            if dropme:
                a_on_sense.leaf = None
            return None


        #----- now lets get the required tree ----#
        a_tree_id = "%s@%s" % (a_on_sense.tree_index, a_on_sense.document_id)

        try:
            a_tree = a_tree_document.get_tree(a_tree_id)
        except Exception:
            return reject("badtree")

        a_leaf = None

        #---- attach the ontonotes sense to the required token ----#

        a_on_sense.enrich_tree(a_tree)
        a_leaf = a_on_sense.leaf

        if not a_leaf:
            return reject("oob")

        if lang_id in ["ch", "ar"]:

            leaf_lemma = a_leaf.get_lemma()

            if leaf_lemma != a_on_sense.lemma and not ignore_lemma_mismatches:
                return reject("badlemma",
                              "leaf_lemma='%s'" % leaf_lemma,
                              "on_sense_lemma='%s'" % a_on_sense.lemma)

        leaf_pos = 'other'
        if a_leaf.part_of_speech:
            if lang_id == "en" and a_leaf.is_aux():
                leaf_pos = 'auxilliary_verb'
            elif a_leaf.is_noun():
                leaf_pos = 'n'
            elif a_leaf.is_verb():
                leaf_pos = 'v'

        if leaf_pos != a_on_sense.pos:
            return reject("posmm" if lang_id != 'ch' else "notinc",
                          "leaf pos=%s" % leaf_pos,
                          "sense pos=%s" % a_on_sense.pos,
                          "leaf tag=%s" % a_leaf.tag,
                          "leaf word=%s" % a_leaf.word)

    def enrich_treebank(self, a_treebank, a_cursor=None, ignore_lemma_mismatches=False):

        abstract_bank.enrich_treebank(self, a_treebank)

        for a_sense_tagged_document in self:
            sys.stderr.write(".")

            for a_on_sense in a_sense_tagged_document.on_sense_list:
                self.enrich_treebank_helper(a_on_sense, a_sense_tagged_document.tree_document,
                                            lang_id = a_treebank.subcorpus.language_id,
                                            ignore_lemma_mismatches=ignore_lemma_mismatches)

        sys.stderr.write("\n")
        return a_treebank

    sql_table_name = "sense_bank"
    sql_exists_table = "on_sense"

    ## @var SQL create statement for the syntactic_link table
    #
    sql_create_statement = \
"""
create table sense_bank
(
  id varchar(255) not null collate utf8_bin primary key,
  subcorpus_id varchar(255) not null,
  tag varchar (255) not null,
  foreign key (subcorpus_id) references subcorpus.id
)
default character set utf8;
"""



    ## @var SQL insert statement for the syntactic_link table
    #
    sql_insert_statement = \
"""
insert into sense_bank
(
  id,
  subcorpus_id,
  tag
) values(%s, %s, %s)
"""



    def write_to_db(self, a_cursor):
        abstract_bank.write_to_db(self, a_cursor)
        self.write_sense_inventory_hash_to_db(self.sense_inventory_hash, a_cursor)


    @staticmethod
    def write_sense_inventory_hash_to_db(a_sense_inventory_hash, a_cursor):
        if not is_db_ref(a_sense_inventory_hash) and not is_not_loaded(a_sense_inventory_hash):
            for a_sense_inventory in a_sense_inventory_hash.itervalues():
                try:
                    a_sense_inventory.write_to_db(a_cursor)
                    sys.stderr.write("... writing sense inventory " + a_sense_inventory.lemma + "\n")
                except AttributeError, e:
                    on.common.log.report("sense", "Failed to write sense inventory to db", "si: %s" % a_sense_inventory)
            sys.stderr.write("\n")

            wn_sense_type.write_to_db(a_cursor)

    @classmethod
    def from_db(cls, a_subcorpus, tag, a_cursor, affixes=None):
        sys.stderr.write("reading the sense bank ....")
        a_sense_bank = sense_bank(a_subcorpus, tag, a_cursor)

        #---- now get document ids for this treebank ----#
        a_cursor.execute("""select document.id from document where subcorpus_id = '%s';""" % (a_subcorpus.id))
        document_rows = a_cursor.fetchall()

        #---- and process each document ----#
        for document_row in document_rows:
            a_document_id = document_row["id"]

            if not on.common.util.matches_an_affix(a_document_id, affixes):
                continue

            sys.stderr.write(".")

            a_sense_tagged_document = sense_tagged_document("", a_document_id, a_sense_bank, a_cursor)

            a_cursor.execute("""select * from on_sense where document_id = '%s';""" % (a_document_id))
            on_sense_rows = a_cursor.fetchall()

            for on_sense_row in on_sense_rows:
                # a_on_sense_id = on_sense_row["id"]
                a_on_sense_lemma = on_sense_row["lemma"]
                a_on_sense_pos = on_sense_row["pos"]
                a_on_sense_ann_1_sense = on_sense_row["ann_1_sense"]
                a_on_sense_ann_2_sense = on_sense_row["ann_2_sense"]
                a_on_sense_adj_sense = on_sense_row["adj_sense"]
                a_on_sense_adjudicated_flag = on_sense_row["adjudicated_flag"]
                a_on_sense_sense = on_sense_row["sense"]
                a_on_sense_word_index = on_sense_row["word_index"]
                a_on_sense_tree_index = on_sense_row["tree_index"]
                a_on_sense_document_id = on_sense_row["document_id"]

                a_on_sense = on_sense(a_on_sense_document_id, a_on_sense_tree_index, a_on_sense_word_index, a_on_sense_lemma, a_on_sense_pos,
                                      a_on_sense_ann_1_sense, a_on_sense_ann_2_sense, a_on_sense_adj_sense, a_on_sense_sense, a_on_sense_adjudicated_flag, a_cursor)

                a_sense_tagged_document.on_sense_list.append(a_on_sense)
                a_sense_tagged_document.lemma_pos_hash["%s-%s" % (a_on_sense_lemma, a_on_sense_pos)] = 0

            a_sense_bank.append(a_sense_tagged_document)

        sys.stderr.write("\n")
        return a_sense_bank



