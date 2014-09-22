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
-------------------------------------------------------
:mod:`proposition` -- Proposition Annotation
-------------------------------------------------------

See:

 - :class:`on.corpora.proposition.proposition`
 - :class:`on.corpora.proposition.proposition_bank`
 - :class:`on.corpora.proposition.frame_set`
 - :class:`on.corpora.sense.pb_sense_type`

Correspondences:

 =============================== =======================================  ==================================================================================================
 **Database Tables**             **Python Objects**                       **File Elements**
 =============================== =======================================  ==================================================================================================
 ``proposition_bank``            :class:`proposition_bank`                All ``.prop`` files in an :class:`on.corpora.subcorpus`
 None                            :class:`proposition_document`            A single ``.prop`` file
 ``proposition``                 :class:`proposition`                     A line in a ``.prop`` file, with everything after the ``-----`` an "argument field"
 None                            :class:`predicate_analogue`              REL argument fields (should only be one)
 None                            :class:`argument_analogue`               ARG argument fields
 None                            :class:`link_analogue`                   LINK argument fields
 ``predicate``                   :class:`predicate`                       Asterisk-separated components of a predicate_analogue.  Each part is coreferential.
 ``argument``                    :class:`argument`                        Asterisk-separated components of an argument_analogue.  Each part is coreferential.
 ``proposition_link``            :class:`link`                            Asterisk-separated components of a link_analogue. Each part is coreferential.
 ``predicate_node``              :class:`predicate_node`                  Comma-separated components of predicates.  The parts together make up the predicate.
 ``argument_node``               :class:`argument_node`                   Comma-separated components of arguments.  The parts together make up the argument.
 ``link_node``                   :class:`link_node`                       Comma-separated components of links.  The parts together make up the link.
 None                            :class:`frame_set`                       An xml frame file (FF)
 ``pb_sense_type``               :class:`on.corpora.sense.pb_sense_type`  Field six of a prop line and a FF's ``frameset/predicate/roleset`` element's ``id`` attribute
 ``pb_sense_type_argument_type`` :class:`argument_composition`            For a FF's ``frameset/predicate`` element, a mapping between ``roleset.id`` and ``roleset/role.n``
 ``tree``                        :class:`on.corpora.tree.tree`            The first three fields of a prop line
 =============================== =======================================  ==================================================================================================

This may be better seen with an example.  The prop line:

 ``bc/cnn/00/cnn_0000@all@cnn@bc@en@on 191 3 gold say-v say.01 ----- 1:1-ARGM-DIS 2:1-ARG0 3:0-rel 4:1*6:1,8:1-ARG1``

breaks up as:

 ============================= =====================================================================================================================
 **Python Object**             **File Text**
 ============================= =====================================================================================================================
 :class:`proposition`          ``bc/cnn/00/cnn_0000@all@cnn@bc@en@on 191 3 gold say-v say.01 ----- 1:1-ARGM-DIS 2:1-ARG0 3:0-rel 4:1*6:1,8:1-ARG1``
 :class:`on.corpora.tree.tree` ``bc/cnn/00/cnn_0000@all@cnn@bc@en@on 191 3``
 :class:`predicate_analogue`   ``3:0-rel``
 :class:`predicate`            ``3:0``
 :class:`predicate_node`       ``3:0``
 :class:`argument_analogue`    each of ``1:1-ARGM-DIS``, ``2:1-ARG0``, and ``4:1*6:1,8:1-ARG1``
 :class:`argument`             each of ``1:1``, ``2:1``, ``4:1``, and ``6:1,8:1``
 :class:`argument_node`        each of ``1:1``, ``2:1``, ``4:1``, ``6:1``, and ``8:1``
 ============================= =====================================================================================================================

Similarly, the prop line:

 ``bc/cnn/00/cnn_0000@all@cnn@bc@en@on 309 5 gold go-v go.15 ----- 2:0*1:1-ARG1 4:1-ARGM-ADV 5:0,6:1-rel``

breaks up as:

 =============================  =====================================================================================================================
 **Python Object**              **File Text**
 =============================  =====================================================================================================================
 :class:`proposition`           ``bc/cnn/00/cnn_0000@all@cnn@bc@en@on 309 5 gold go-v go.15 ----- 2:0*1:1-ARG1 4:1-ARGM-ADV 5:0,6:1-rel``
 :class:`on.corpora.tree.tree`  ``bc/cnn/00/cnn_0000@all@cnn@bc@en@on 309 5``
 :class:`predicate_analogue`    ``5:0,6:1-rel``
 :class:`predicate`             ``5:0,6:1``
 :class:`predicate_node`        each of ``5:0`` and ``6:1``
 :class:`argument_analogue`     each of ``2:0*1:1-ARG1`` and ``4:1-ARGM-ADV``
 :class:`argument`              each of ``2:0``, ``1:1`` and ``4:1``
 :class:`argument_node`         each of ``2:0``, ``1:1``, and ``4:1``
 =============================  =====================================================================================================================


Classes:

.. autoclass:: proposition_bank
.. autoclass:: proposition_document
.. autoclass:: proposition

.. autoclass:: abstract_proposition_bit
.. autoclass:: abstract_holder
.. autoclass:: abstract_analogue
.. autoclass:: abstract_node_holder
.. autoclass:: abstract_node

.. autoclass:: predicate_analogue
.. autoclass:: predicate
.. autoclass:: predicate_node
.. autoclass:: argument_analogue
.. autoclass:: argument
.. autoclass:: argument_node
.. autoclass:: link_analogue
.. autoclass:: link
.. autoclass:: link_node
.. autoclass:: frame_set


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
import copy
import traceback
import pprint

#---- xml specific imports ----#
from xml.etree import ElementTree
import xml.etree.cElementTree as ElementTree



#---- custom package imports ----#
import on

import on.common.log
import on.common.util

import on.corpora
import on.corpora.tree


from collections import defaultdict
from on.common.util import is_db_ref, is_not_loaded, insert_ignoring_dups, esc, same_except_for_tokenization_and_hyphenization
from on.corpora import abstract_bank

class predicate_type:

    allowed = ["n",  # nominalizations
               "v",  # normal propositions
               "j"]  # predicate adjectives

    references = [["predicate", "type"]]

    sql_table_name = "predicate_type"
    sql_create_statement = \
"""
create table predicate_type
(
  id varchar(8) not null collate utf8_bin primary key
)
default character set utf8;
"""


    sql_insert_statement = \
"""insert into predicate_type
(
  id
) values (%s)
"""


class link_type:

    allowed = ["LINK-PCR", "LINK-SLC"]
    references = [["link", "type"]]

    sql_table_name = "link_type"
    sql_create_statement = \
"""
create table link_type
(
  id varchar(8) not null collate utf8_bin primary key
)
default character set utf8;
"""

    sql_insert_statement = \
"""insert into link_type
(
  id
) values (%s)
"""


class argument_type(on.corpora.abstract_type_table):

    references = [["argument", "type"],
                  ["pb_sense_type_argument_type", "argument_type"]]

    allowed = [ "ARG0",
                "ARG1",
                "ARG2",
                "ARG3",
                "ARG4",
                "ARG5",
                "ARG6",
                "ARG7",
                "ARG8",
                "ARG9",
                "ARGM-DIR",
                "ARGM-LOC",
                "ARGM-MNR",
                "ARGM-TMP",
                "ARGM-EXT",
                "ARGM-REC",
                "ARGM-PRD",
                "ARGM-PNC",
                "ARGM-CAU",
                "ARGM-DIS",
                "ARGM-ADV",
                "ARGM-MOD",
                "ARGM-NEG",
                "ARGM-DSP",
                "ARGM-RLC",
                "ARGA",

                # chinese propbank
                "ARGM-BNF",
                "ARGM-DGR",
                "ARGM-FRQ",
                "ARGM-TPC",
                "ARGM-PRP",
                "rel-Sup" ]

    sql_table_name = "argument_type"
    sql_create_statement = \
"""
create table argument_type
(
  id varchar(255) not null collate utf8_bin primary key
)
default character set utf8;
"""

    sql_insert_statement = \
"""insert into argument_type
(
  id
) values (%s)
"""


class abstract_proposition_bit(object):
    """ any subcomponent of a proposition after the '``-----``'.

    Attributes:

      .. attribute:: id
      .. attribute:: index_in_parent
      .. attribute:: lemma
      .. attribute:: pb_sense_num
      .. attribute:: proposition
      .. attribute:: document_id
      .. attribute:: enc_self

         Encode whatever we represent as a string, generally by
         combining the encoded representations of sub-components

    Class Hierarchy:

     - :class:`abstract_proposition_bit` -- things with parents

        - :class:`abstract_node`
        - :class:`abstract_holder`

     - :class:`abstract_node` -- things that align with subtrees

        - :class:`argument_node`
        - :class:`predicate_node`
        - :class:`link_node`

     - :class:`abstract_holder` -- things with children

        - :class:`abstract_analogue`
        - :class:`abstract_node_holder`

     - :class:`abstract_analogue` -- children are coreferential (split on '*')

        - :class:`argument_analogue`
        - :class:`predicate_analogue`
        - :class:`link_analogue`

     - :class:`abstract_node_holder` -- children together make up an entity (split on ',')

        - :class:`argument`
        - :class:`predicate`
        - :class:`link`

    """


    def __init__(self, a_parent):
        self.parent = a_parent
        self.parent.add(self)

    @property
    def index_in_parent(self):
        return self.parent.get_index_of(self)

    @property
    def proposition(self):
        if self.parent.__class__.__name__ == "proposition":
            return self.parent
        return self.parent.proposition

    @property
    def document_id(self):
        return self.proposition.document_id

    @property
    def lemma(self):
        return self.proposition.lemma

    @property
    def pb_sense_num(self):
        return self.proposition.pb_sense_num


class abstract_holder(abstract_proposition_bit):
    """ represents any proposition bit that holds other proposition bits

    Extends :class:`abstract_proposition_bit`

    See:

     - :class:`abstract_analogue`
     - :class:`abstract_node_holder`

    """

    def __init__(self, sep, a_parent):
        abstract_proposition_bit.__init__(self, a_parent)

        self.children = []
        self.sep = sep

    def add(self, a_child):
        self.children.append(a_child)

    def get_index_of(self, a_child):
        return self.children.index(a_child)

    def add(self, a_child):
        self.children.append(a_child)

    def __len__(self):
        return len(self.children)

    def __getitem__(self, idx):
        return self.children[idx]

    def _get_enc_self(self):
        sub_enc = [a.enc_self for a in self]
        sub_enc = [x for x in sub_enc if x]
        if sub_enc:
            return self.sep.join(sub_enc)
        else:
            return None

    enc_self = property(_get_enc_self)

    def enrich_tree(self, a_tree):
        for a in self:
            a.enrich_tree(a_tree)

    def __repr__(self):
        return """<%s object: id: %s; enc_self: '%s'%s>""" \
               % (self.__class__.__name__, self.id, self.enc_self, "\n   " + ",\n   ".join([str(c) for c in self.children]).replace("\n", "\n   "))

class abstract_analogue(abstract_holder):
    """ represents argument_analogue, predicate_analogue, link_analogue

    Example: ``0:1,3:2*2:0-ARGM``

    Extends: :class:`abstract_holder`

    Represents:

     - :class:`argument_analogue`
     - :class:`predicate_analogue`
     - :class:`link_analogue`

    This class is used for the space separated portions of a
    proposition after the '``-----``'

    All children are coreferential, and usually all but one are traces.

    """

    def __init__(self, a_parent, a_analogue_type):
        """ analogue type is 'link', 'argument', or 'predicate' """

        self.analogue_type = a_analogue_type

        abstract_holder.__init__(self, "*", a_parent)
        # implementers need to set their own type

    @property
    def id(self):
        return "%s@%s@%s" % (self.index_in_parent, self.type, self.parent.id)

    @property
    def enc_self(self):
        sub_enc = [a.enc_self for a in self]
        sub_enc = [x for x in sub_enc if x]

        if sub_enc:
            return "%s-%s" % (self.sep.join(sub_enc), self.enc_self_type)
        else:
            return None

    @property
    def enc_self_type(self):
        return self.type

    def write_to_db(self, a_cursor):
        for a_node_holder in self:
            a_node_holder.write_to_db(a_cursor)



class abstract_node_holder(abstract_holder):
    """ represents argument, predicate, link

    Example: ``0:1,3:2``

    Extends: :class:`abstract_holder`

    Represents:

     - :class:`argument`
     - :class:`predicate`
     - :class:`link`

    This class is used for any bit of a proposition which has
    representation A,B where A and B are nodes

    """

    def __init__(self, a_parent):
        abstract_holder.__init__(self, ",", a_parent)

    @property
    def id(self):
        return "%s@%s" % (self.index_in_parent, self.parent.id)

    @property
    def type(self):
        return self.parent.type

    @property
    def enc_self(self):
        sub_enc = [a.enc_self for a in self]
        sub_enc = [x for x in sub_enc if x]
        if sub_enc:
            return self.sep.join(sub_enc)
        else:
            return None


class abstract_node(abstract_proposition_bit):
    """ represents argument_node, predicate_node, and link_node

    Example: ``0:1``

    Extends: :class:`abstract_proposition_bit`

    Represents:

     - :class:`argument_node`
     - :class:`predicate_node`
     - :class:`link_node`

    This class is used for any bit of a proposition which has representation A:B

    Attributes:

       .. attribute:: sentence_index

          which tree we're in

       .. attribute:: token_index

          which leaf in the tree we are

       .. attribute:: height

          how far up from the leaves we are (a leaf is height 0)

       .. attribute:: parent

          an abstract_node_holder to add yourself to

       .. attribute:: subtree

          which :class:`on.corpora.tree.tree` we're aligned to.  None until enrichment.

       .. attribute:: is_ich_node

          ``True`` only for argument nodes.  ``True`` when proposition
          taggers would separate this node from others with a '``;``'
          in the encoded form.  That is, ``True`` if the subtree we
          are attached to is indexed to an ``*ICH*`` leaf or we have
          an ``*ICH*`` leaf among our leaves, ``False`` otherwise.

       .. attribute:: errcomms

          This is a list, by default the empty list.  If errors are
          found in loading this proposition, strings that can be
          passed to :func:`on.common.log.reject` or
          :func:`on.common.log.adjust` are appended to it along with
          comments, like::

            errcomms.append(['reason', ['explanation', 'details', ...]])


    Initially, a node is created with sentence and token indecies.
    During enrichment we gain a reference to a
    :class:`on.corpora.tree.tree` instance.  After enrichment,
    requests for sentence and token indecies are forwarded to the
    subtree.

    """

    def __init__(self, sentence_index, token_index, height, parent):
        abstract_proposition_bit.__init__(self, parent)

        self.subtree = None # until enrichment
        self.sentence_index = sentence_index
        self.token_index = token_index
        self.height = height

        self.primary = False
        self.is_ich_node = False

        self.errcomms= []

    @staticmethod
    def _find_trace_issues(from_subtree, to_subtree, alignment_from_to):
        """ given a pair of subtrees, determine if there have been
        trace changes between the two.  If there have, return a list
        of errcomms, otherwise return the empty list """


        expected_subtree_leaves = [to_leaf for from_leaf in from_subtree
                                   for to_leaf in alignment_from_to.get(from_leaf, [])]
        new_subtree_leaves = to_subtree.leaves() # new leaves in the new subtree we found

        errcomms = []

        def adderr(errcode, *comments):
            comments = list(comments)

            if not errcomms: # only append these on the first error
                comments.append("from:\n%s" % from_subtree.pretty_print())
                comments.append("to:\n%s" % to_subtree.pretty_print())

                if not from_subtree.is_root():
                    comments.append("from_tree:\n%s" % from_subtree.get_root().pretty_print())
                if not to_subtree.is_root():
                    comments.append("to_tree:\n%s" % to_subtree.get_root().pretty_print())

            errcomms.append([errcode, comments])

        # trace insertions
        for n_leaf in new_subtree_leaves:
            if n_leaf not in expected_subtree_leaves:
                if n_leaf.is_trace():
                    adderr("modinstrace", "trace in new tree: %s" % n_leaf.word)

        # trace deletions and modifications
        for from_leaf in from_subtree.leaves():
            if from_leaf.is_trace():
                trace_toleaves = [to_leaf for to_leaf in alignment_from_to.get(from_leaf, []) if to_leaf.is_trace()]

                if not trace_toleaves:
                    adderr("deltrace", "trace in old tree: %s" % from_leaf.word)
                elif len(trace_toleaves) != 1:
                    adderr("modinstrace",
                           "trace in old tree: %s" % from_leaf.word,
                           "traces in new tree: %s" % [to_leaf.word for to_leaf in trace_toleaves])
                elif trace_toleaves[0].trace_type != from_leaf.trace_type:
                    adderr("modinstrace",
                           "trace in old tree: %s" % from_leaf.word,
                           "trace in new tree: %s" % trace_toleaves[0].word)

        return errcomms


    @property
    def id(self):
        return "%s@%s" % (self.index_in_parent, self.parent.id)

    def enrich_tree(self, a_tree):
        if self.subtree:
            raise Exception("Cannot enrich tree once subtree is set")

        if self.sentence_index is not None:
            if a_tree.get_sentence_index() != self.sentence_index:
                raise Exception("Cannot enrich tree %s which has sentence index %s because our index is %s." % (
                    a_tree.id, a_tree.get_sentence_index(), self.sentence_index))

        a_leaf = a_tree.get_leaf_by_token_index(self.token_index)

        a_subtree = a_leaf
        try:
            for i in range(self.height):
                a_subtree = a_subtree.parent
        except AttributeError:
            return None # will be caught later

        was_ich_node = self.is_ich_node
        was_enc_self = self.enc_self
        self.subtree = a_subtree

        if was_ich_node != self.is_ich_node:
            if self.proposition.original_enc_prop: # if not loaded from db
                on.common.log.report("proposition", "incorrect %suse of semicolon" % ("dis-" if not was_ich_node else ""),
                                     subtree=a_subtree, was_enc_self=was_enc_self,
                                     is_enc_self=self.enc_self, tree=a_subtree.get_root().pretty_print(),
                                     trace_types=", ".join(str(a_leaf.trace_type) for a_leaf in a_subtree),
                                     prop=self.proposition.original_enc_prop)


    @property
    def enrich_name(self):
        return self.__class__.__name__ # argument_node, predicate_node, or link_node

    @property
    def node_id(self):
        return "%s:%s" % (self.token_index, self.height)

    @property
    def type(self):
        return self.parent.type

    @staticmethod
    def __clean(val):
        return None if val is None else int(val)

    def _get_token_index(self):
        if self.subtree:
            return self.subtree.get_token_index()
        return self._token_index

    def _set_token_index(self, val):
        if self.subtree:
            raise Exception("Cannot set token index after enrichment -- set subtree instead")
        self._token_index = self.__clean(val)

    token_index = property(_get_token_index, _set_token_index)

    def _get_sentence_index(self):
        if self.subtree:
            return self.subtree.get_sentence_index()
        return self._sentence_index

    def _set_sentence_index(self, val):
        if self.subtree:
            raise Exception("Cannot set sentence index after enrichment -- set subtree instead")
        self._sentence_index = self.__clean(val)

    sentence_index = property(_get_sentence_index, _set_sentence_index)

    def _get_height(self):
        if self.subtree:
            return self.subtree.get_height()
        return self._height

    def _set_height(self, val):
        if self.subtree:
            raise Exception("Cannot set height after enrichment -- set subtree instead")
        self._height = self.__clean(val)

    height = property(_get_height, _set_height)

    def _get_subtree(self):
        return self._subtree

    def _set_subtree(self, new_subtree):
        if not hasattr(self, "_subtree"):
            self._subtree = None

        if self.subtree:
            if new_subtree is None:
                self._token_index = self.token_index
                self._height = self.height
                self._is_ich_node = self.is_ich_node

            self.__get_annotations_list(self.subtree).remove(self)
            if self.primary:
                self.subtree.proposition = None

        if new_subtree:
            self.__get_annotations_list(new_subtree).append(self)
            if self.primary:
                if new_subtree.proposition:
                    orig_a = new_subtree.proposition.original_enc_prop
                    orig_b = self.proposition.original_enc_prop

                    identical = (proposition.canonical_enc_prop_args(orig_a) == proposition.canonical_enc_prop_args(orig_b) and
                                 new_subtree.proposition.lemma == self.proposition.lemma and
                                 new_subtree.proposition.pb_sense_num == self.proposition.pb_sense_num)

                    if identical:
                        errcomm = ["identdup", ["keeping: %s" % orig_a]]
                    else:
                        errcomm = ["nonidentdup",
                                   ["keeping: %s" % orig_a,
                                    "onf with prop we're keeping:\n%s" % new_subtree.onf()]]

                    on.common.log.reject(["docid", new_subtree.get_root().document_id, "prop"], "prop", [errcomm], orig_b)
                    self.proposition.valid = False
                    return

                new_subtree.proposition = self.proposition

        self._subtree = new_subtree

    subtree = property(_get_subtree, _set_subtree)

    def __get_annotations_list(self, a_subtree):
        return getattr(a_subtree, self.enrich_name + "_list")

    @property
    def enc_self(self):
        if not self.subtree and self.proposition.get_primary_predicate().subtree:
            """ if our primary predicate has been attached to a tree but we've not been, that's bad """
            return None
        else:
            return self.node_id

    def __repr__(self):
        return """<%s object: id: %s; enc_self: '%s'>""" \
               % (self.__class__.__name__, self.id, self.enc_self)

class predicate_node(abstract_node):
    """ represents the different nodes of a multi-word predicate

    Extends: :class:`abstract_node`

    Contained by: :class:`predicate`

    Attributes:

        .. attribute:: a_predicate

           :class:`on.corpora.proposition.predicate`

        .. attribute:: sentence_index

           which tree in the document do we belong to

        .. attribute:: token_index

           token index of this node within the predicate's tree

        .. attribute:: height

           how far up in the tree from the leaf at token_index we need to
           go to get the subtree this node represents

        .. attribute:: primary

           are we the primary predicate?

    """

    def __init__(self, sentence_index, token_index, height, a_predicate, primary=False):
        abstract_node.__init__(self, sentence_index, token_index, height, a_predicate)
        self.primary = primary

    @property
    def predicate(self):
        return self.parent

    sql_table_name = "predicate_node"
    sql_create_statement = \
"""
create table predicate_node
(
  id varchar(255) not null collate utf8_bin primary key,
  predicate_id varchar(255),
  node_id varchar(16),
  primary_flag int,
  index_in_parent int,
  foreign key (predicate_id) references predicate.id
)
default character set utf8;
"""


    sql_insert_statement = \
"""insert into predicate_node
(
  id,
  predicate_id,
  node_id,
  primary_flag,
  index_in_parent
) values (%s, %s, %s, %s, %s)
"""

    def write_to_db(self, cursor):

        #---- insert the value in the table ----#
        cursor.executemany("%s" % (self.__class__.sql_insert_statement), [
            (self.id, self.predicate.id, self.node_id, self.primary, self.index_in_parent)])



class predicate(abstract_node_holder):
    """
    Extends: :class:`abstract_node_holder`

    Contained by: :class:`predicate_analogue`

    Contains: :class:`predicate_node`

    """

    def __init__(self, enc_predicate, sentence_index, token_index, a_predicate_analogue):

        abstract_node_holder.__init__(self, a_predicate_analogue)

        if enc_predicate:
            for node_id in enc_predicate.split(","):
                pp_token_index, height = node_id.split(':')

                primary = int(pp_token_index) == int(token_index)

                assert "," in enc_predicate or primary

                predicate_node(sentence_index, pp_token_index, height, self, primary=primary)

    @property
    def predicate_analogue(self):
        return self.parent

    @property
    def token_index(self):
        return self.predicate_analogue.token_index

    @property
    def sentence_index(self):
        return self.predicate_analogue.sentence_index

    def get_primary_predicate(self):
        for a_predicate_node in self:
            if a_predicate_node.primary:
                return a_predicate_node
        return None


    sql_table_name = "predicate"
    sql_create_statement = \
"""
create table predicate
(
  id varchar(255) not null collate utf8_bin primary key,
  index_in_parent int not null,
  proposition_id varchar(255),
  type varchar(255),
  sentence_index int not null,
  token_index int,
  lemma varchar(255),
  pb_sense_num varchar(255),
  foreign key (proposition_id) references proposition.id,
  foreign key (type) references predicate_type.id
)
default character set utf8;
"""

    sql_insert_statement = \
"""insert into predicate
(
  id,
  index_in_parent,
  proposition_id,
  type,
  sentence_index,
  token_index,
  lemma,
  pb_sense_num
) values (%s, %s, %s, %s, %s, %s, %s, %s)
"""

    def write_to_db(self, cursor):
        cursor.executemany("%s" % (self.__class__.sql_insert_statement), [(
            self.id, self.index_in_parent, self.proposition.id, self.type,
            self.sentence_index, self.token_index, self.lemma, self.pb_sense_num)])

        for a_predicate_node in self:
            a_predicate_node.write_to_db(cursor)

class predicate_analogue(abstract_analogue):
    """ The ``REL``-tagged field of a proposition.

    Extends: :class:`abstract_analogue`

    Contained by: :class:`proposition`

    Contains: :class:`predicate`

    """

    def __init__(self, enc_predicates, a_type, sentence_index, token_index, a_proposition):
        abstract_analogue.__init__(self, a_proposition, "predicate")
        self.type = a_type

        if enc_predicates:
            assert "*" not in enc_predicates
            enc_predicate = enc_predicates
            predicate(enc_predicate, sentence_index, token_index, self)


    @property
    def id(self):
        return "%s.%s@%s@%s@%s" % (self.lemma, self.pb_sense_num, self.type, self.token_index, self.tree_id)

    @property
    def tree_id(self):
        return "%s@%s" % (self.sentence_index, self.document_id)

    @property
    def token_index(self):
        return self.primary_predicate.token_index

    @property
    def sentence_index(self):
        return self.primary_predicate.sentence_index

    def get_primary_predicate(self):
        for a_predicate in self:
            p = a_predicate.get_primary_predicate()
            if p:
                return p
        return None
    primary_predicate = property(get_primary_predicate)

    @property
    def enc_self_type(self):
        return "rel"



class link_node(abstract_node):
    """
    Extends: :class:`abstract_node`

    Contained by: :class:`link`

    """

    def __init__(self, sentence_index, token_index, height, parent):
        abstract_node.__init__(self, sentence_index, token_index, height, parent)

    @property
    def link(self):
        return self.parent

    sql_table_name = "link_node"
    sql_create_statement = \
"""
create table link_node
(
  id varchar(255) not null collate utf8_bin primary key,
  link_id varchar(255) not null,
  node_id varchar(255) not null,
  index_in_parent int,
  foreign key (link_id) references proposition_link.id
)
default character set utf8;
"""

    sql_insert_statement = \
"""insert into link_node
(
  id,
  link_id,
  node_id,
  index_in_parent
) values (%s, %s, %s, %s)
"""

    def write_to_db(self, cursor):
        #---- insert the value in the table ----#
        cursor.executemany("%s" % (self.__class__.sql_insert_statement),
                           [(self.id, self.link.id, self.node_id, self.index_in_parent)])


class link(abstract_node_holder):
    """
    Extends: :class:`abstract_node_holder`

    Contained by: :class:`link_analogue`

    Contains: :class:`link_node`

    Attributes:

        .. attribute:: associated_argument

           the :class:`argument_analogue` this link is providing additional detail for

    """

    def __init__(self, enc_link, a_link_analogue):
        abstract_node_holder.__init__(self, a_link_analogue)

        if enc_link:
            for enc_link_node in enc_link.split(","):
                token_index, height = enc_link_node.split(":")
                link_node(None, token_index, height, self)

    @property
    def associated_argument(self):
        return self.link_analogue.associated_argument

    @property
    def link_analogue(self):
        return self.parent

    sql_table_name = "proposition_link"
    sql_create_statement = \
"""
create table proposition_link
(
  id varchar(255) not null collate utf8_bin primary key,
  index_in_parent int not null,
  link_analogue_index int not null,
  type varchar(255),
  proposition_id varchar(255),
  associated_argument_id varchar(255),
  foreign key (proposition_id) references proposition.id,
  foreign key (associated_argument_id) references argument_analogue.id
)
default character set utf8;
"""


    sql_insert_statement = \
"""insert into proposition_link
(
  id,
  index_in_parent,
  link_analogue_index,
  type,
  proposition_id,
  associated_argument_id
) values (%s, %s, %s, %s, %s, %s)
"""

    def write_to_db(self, cursor):

        cursor.executemany("%s" % (self.__class__.sql_insert_statement), [(
            self.id, self.index_in_parent, self.link_analogue.index_in_parent, self.type,
            self.proposition.id, self.associated_argument.id)])

        for a_link_node in self:
            a_link_node.write_to_db(cursor)

class link_analogue(abstract_analogue):
    """
    Extends: :class:`abstract_analogue`

    Contained by: :class:`proposition`

    Contains: :class:`link`

    """

    def __init__(self, enc_links, a_type, a_proposition, a_associated_argument):
        abstract_analogue.__init__(self, a_proposition, "link")
        self.type = a_type
        self.associated_argument = a_associated_argument

        if enc_links:
            for enc_link in enc_links.split("*"):
                link(enc_link, self)

class argument_node(abstract_node):
    """
    Extends: :class:`abstract_node`

    Contained by: :class:`argument`

    """

    def __init__(self, sentence_index, token_index, height, a_argument):
        abstract_node.__init__(self, sentence_index, token_index, height, a_argument)

    def _get_is_ich_node(self):
        if self.subtree:

            def has_ich_leaf(a_subtree):
                return "*ICH*" in (a_leaf.trace_type for a_leaf in a_subtree.leaves())

            def pointed_to_by_ich_leaf(a_subtree):
                return "*ICH*" in (a_leaf.trace_type for a_leaf in a_subtree.reference_leaves)

            return has_ich_leaf(self.subtree) or pointed_to_by_ich_leaf(self.subtree)



        return self._is_ich_node

    def _set_is_ich_node(self, val):
        if self.subtree:
            raise Exception("Cannot set is_ich_node enrichment -- set subtree instead")
        self._is_ich_node = bool(val)

    is_ich_node = property(_get_is_ich_node, _set_is_ich_node)

    @property
    def argument(self):
        return self.parent

    @property
    def enc_self(self):
        """ if we're an ICH node, return in form ``;index:height;`` otherwise just ``index:height`` """

        if not self.subtree and self.proposition.get_primary_predicate().subtree:
            """ if our primary predicate has been attached to a tree but we've not been, that's bad """
            return None

        add_sep = ";" if self.is_ich_node else ""

        return add_sep + self.node_id + add_sep

    sql_table_name = "argument_node"
    sql_create_statement = \
"""
create table argument_node
(
  id varchar(255) not null collate utf8_bin primary key,
  argument_id varchar(255) not null,
  node_id varchar(255) not null,
  index_in_parent int,
  foreign key (argument_id) references argument.id
)
default character set utf8;
"""

    sql_insert_statement = \
"""insert into argument_node
(
  id,
  argument_id,
  node_id,
  index_in_parent
) values (%s, %s, %s, %s)
"""

    def write_to_db(self, cursor):
        #---- insert the value in the table ----#
        cursor.executemany("%s" % (self.__class__.sql_insert_statement),
                           [(self.id, self.argument.id, self.node_id, self.index_in_parent)])


class argument(abstract_node_holder):
    """
    Extends: :class:`abstract_node_holder`

    Contained by: :class:`argument_analogue`

    Contains: :class:`argument_node`

    """

    def __init__(self, enc_argument, a_argument_analogue):
        abstract_node_holder.__init__(self, a_argument_analogue)

        self.argument_subtype = None #---- this will be set later on when we are enriching the treebank.  it can be one of trace or relative-pronoun (usually)

        if enc_argument:
            old_enc_argument = enc_argument
            enc_argument = old_enc_argument.replace(";", ";,;")

            for enc_argument_node in enc_argument.split(","):
                is_ich_node = ";" in enc_argument_node

                enc_argument_node = enc_argument_node.replace(";", "")

                arg_part_token_index, arg_part_height = enc_argument_node.split(":")
                a_argument_node = argument_node(None, arg_part_token_index, arg_part_height, self)

                a_argument_node.is_ich_node = is_ich_node




    @property
    def argument_analogue(self):
        return self.parent

    @property
    def split_argument_flag(self):
        return len(self) != 1

    @property
    def enc_self(self):
        """ our children will return either normally, or enclosed with
        ``;``s.  If the latter, we should use ``;`` as the separator
        instead of ``,``.  We should also make sure we never return
        something starting or ending with ``;``, as it's only a
        separator.
        """

        sub_enc = [a.enc_self for a in self]
        sub_enc = [x for x in sub_enc if x]
        if not sub_enc:
            return None
        else:
            es = ",".join(sub_enc)

        es = es.replace(";,;", ";").replace(";,", ";").replace(",;", ";")

        if es.startswith(";"):
            es = es[1:]
        if es.endswith(";"):
            es = es[:-1]

        return es


    sql_table_name = "argument"
    sql_create_statement = \
"""
create table argument
(
  id varchar(255) not null collate utf8_bin primary key,
  argument_analogue_index int not null,
  type varchar(255),
  index_in_parent int,
  split_argument_flag int,
  argument_subtype varchar(255),
  proposition_id varchar(255),
  foreign key (type) references argument_type.id,
  foreign key (proposition_id) references proposition.id
)
default character set utf8;
"""


    sql_insert_statement = \
"""insert into argument
(
  id,
  argument_analogue_index,
  type,
  index_in_parent,
  split_argument_flag,
  argument_subtype,
  proposition_id
) values (%s, %s, %s, %s, %s, %s, %s)
"""

    def write_to_db(self, cursor):

        #---- insert the value in the table ----#
        cursor.executemany("%s" % (self.__class__.sql_insert_statement),
                           [(self.id, self.argument_analogue.index_in_parent, self.type, self.index_in_parent,
                             self.split_argument_flag, self.argument_subtype, self.proposition.id)])

        for a_argument_node in self:
            a_argument_node.write_to_db(cursor)

    @staticmethod
    def compress_name(a_argument_name):
        """
        a small function to convert ARG -> A and ARGM -> AM in the argument names
        """
        return re.sub("ARG", "A", a_argument_name)

class BadArgumentTypeException(Exception):
    pass

class argument_analogue(abstract_analogue):
    """

    Extends: :class:`abstract_analogue`

    Contained by: :class:`proposition`

    Contains: :class:`argument`

    """

    def __init__(self, enc_argument_analogue, a_proposition):
        abstract_analogue.__init__(self, a_proposition, "argument")
        self.type = None

        if enc_argument_analogue:

            #--------------------------------------------------------------------------------#
            # the one split is important as some arguments contain a
            # "-" in their name - "ARGM-MNR", for eg. also predicates
            # are considered part of the argument, so we will have to
            # construct them here itself
            #--------------------------------------------------------------------------------#

            argument_nodes, self.type = enc_argument_analogue.split("-", 1)
            type_bits = self.type.split("-")[:2]

            if type_bits[0] == "ARGM" and type_bits[1].lower() != type_bits[1]:
                """ this is a legitimate ARGM """
                self.type = "-".join(type_bits)
            elif self.type in ["REL-Sup", "rel-Sup"]:
                """ this is a support verb in chinese"""
                self.type = "rel-Sup"
            else:
                """ bare ARGM or anything else """
                self.type = type_bits[0]

            if self.type not in argument_type.allowed:
                raise BadArgumentTypeException("bad argument type: " + self.type)

            if(argument_nodes.strip() == ""):
                on.common.log.report("propbank", "MISSING ARGUMENT NODE FOR PROPOSITION",
                                     "id=" + a_proposition.original_enc_prop + "\n" +
                                     "considering some random node as argument")
                # HACK consider some random node as argument
                argument_nodes = "1:0"

            for enc_argument in argument_nodes.split("*"):
                argument(enc_argument, self)

    def get_argument_number(self):
        """ if we're a numbered arg, return that number.  Otherwise return None """

        if not self.type or not self.type.startswith("ARG"):
            return None

        num = self.type.split("-")[0].replace("ARG", "")
        try:
            return int(num)
        except ValueError:
            return None


    def get_subtree_tuple(self, follow_links=True, follow_traces=True):
        """ return (core_subtree, r_subtrees, c_subtrees)

        core_subtree is our best subtree, defined by default as
        belonging to the non-trace node farthest from the predicate.

        the union of c_subtrees and r_subtrees is a list of the
        subtrees for all the other nodes.  A node contributes to
        r_subtrees if it is a coreferring node. A node contributes to
        c_subtrees if it is a continuation node.

        the follow_links variable controls whether the case of::

            A*B-ARG A*C-LINK

        is treated like::

            A*B*C-ARG

        or not.  If follow_links is false, links are just ignored.


        if follow_traces is true, then if the tree says

          *-1 -> *PRO*-2 -> John

        but the proposition doesn't contain this chain, tagging only
        the '*-1' then we add additional r-args to consideration for
        '*PRO*-2' and 'John'.  In this case, this would make the core
        argument be 'John' instead and '*-1' and '*PRO*-2' would end
        up only ase r-args.

        """

        def canonical(a_subtree):
            """ in the case of only children, two subtrees might really
            be the same.  Pull all to the lowest level. """

            if not a_subtree:
                return a_subtree

            while a_subtree.children and len(a_subtree.children) == 1:
                a_subtree = a_subtree.children[0]
            return a_subtree

        def subtree_equal(x, y):
            """ return true if x and y represent the same leaf span """
            return canonical(x) is canonical(y)

        def dist_from_pri_pred(a_subtree, pp_subtree=self.proposition.get_primary_predicate().subtree):
            # note that by putting pp_subtree as a default argument it
            # will have one value for the whole get_subtree_tuple call
            return abs((a_subtree.end - a_subtree.start) - (pp_subtree.end - pp_subtree.start))

        def expand_with_links(a_subtree):
            """ follow links to return other subtrees linked to this one """
            linked_subtrees = []
            for a_link_analogue in self.proposition.link_analogues:

                # first figure out if this link applies to this subtree
                if any(subtree_equal(a_link_node.subtree, a_subtree)
                       for a_link in a_link_analogue
                       for a_link_node in a_link
                       if a_link_node.subtree):
                    # then add any subtrees in the link (that aren't
                    # the one we were given) to what we're returning
                    linked_subtrees.extend([a_link_node.subtree
                                            for a_link in a_link_analogue
                                            for a_link_node in a_link
                                            if a_link_node.subtree and not subtree_equal(a_link_node.subtree, a_subtree)])
            return linked_subtrees

        def expand_with_traces(a_subtree, ignore_subtrees):
            """ if a_subtree is an indexed trace and is not on the ignore list, then follow it home """

            r_args = []
            t = a_subtree.identity_subtree
            while True:
                t = canonical(t)
                if not t or not t.is_trace():
                    break
                r_args.append(t)
                t = t.identity_subtree

            return [x for x in r_args
                    if not any(subtree_equal(x, ignore) for ignore in ignore_subtrees)]


        def make_argument_lists():
            """ turn the argument structure into lists of subtrees

            return [coreferential, coreferential, coreferential, ...]
            where coreferential is [continuation, continuation, continuation]

            """

            argument_list = []
            for a_argument in self:
                argument_sub_list = []
                for a_argument_node in a_argument:
                    st = a_argument_node.subtree
                    if st:
                        argument_sub_list.append(st)
                        if follow_links:
                            # links count as additional r-args (star separated coreferential args)
                            for a_subtree in expand_with_links(st):
                                argument_list.append([a_subtree])
                argument_list.append(argument_sub_list)

            if follow_traces:
                extendwith = []
                for argument_sub_list in argument_list:
                    for a_subtree in argument_sub_list:
                        for a_trace_subtree in expand_with_traces(a_subtree, ignore_subtrees=[x for y in argument_list for x in argument_sub_list] +
                                                                  [y for x in extendwith for y in x]):

                            extendwith.append([a_trace_subtree])

                argument_list.extend(extendwith)

            return argument_list

        argument_list = make_argument_lists()

        def leftmost(some_subtrees):

            distances = [(a_subtree.start, a_subtree) for a_subtree in some_subtrees]
            distances.sort()

            if not distances:
                return None

            leftmost_distance, lefty = distances[0]
            return lefty

        def calculate_core_subtree():
            distances = []
            for argument_sub_list in argument_list:
                # from argument_sub_list, consider only the leftmost non trace argument
                lefty = leftmost(a_subtree for a_subtree in argument_sub_list
                                 if not canonical(a_subtree).is_trace())
                if lefty:
                    distances.append((dist_from_pri_pred(lefty), lefty))
            distances.sort()

            if not distances:
                assert all(canonical(a_subtree).is_trace()
                           for argument_sub_list in argument_list
                           for a_subtree in argument_sub_list)
                return None

            core_subtree_distance, core_subtree = distances[-1]
            return core_subtree

        core_subtree = calculate_core_subtree()

        r_subtrees = [] # coreferential (*-separated) arguments
        c_subtrees = [] # continuation (,- and ;-separated) arguments
        for argument_sub_list in argument_list:
            is_r = not any(subtree_equal(a_subtree, core_subtree)
                           for a_subtree in argument_sub_list)
            if type(argument_sub_list) != type([]):
                print self.enc_self
                print argument_sub_list
                raise Exeption("err")

            for a_subtree in argument_sub_list:
                is_c = not subtree_equal(a_subtree, core_subtree)

                if is_r:
                    r_subtrees.append(a_subtree)
                elif is_c:
                    c_subtrees.append(a_subtree)
                else:
                    assert subtree_equal(a_subtree, core_subtree)

        return core_subtree, r_subtrees, c_subtrees




class argument_composition:
    def __init__(self, lemma):
        self.argument_types = []
        self.note = None
        self.lemma = lemma
        self.examples = []

    def __repr__(self):
        a_string = "\n        <argument_composition object: \n        complex_lemma = %s;" % (self.lemma)
        i=0
        for a_argument_type in self.argument_types:
            a_string = a_string + "\n            " + a_argument_type
        a_string = a_string + ">"

        return a_string

    sql_table_name = "pb_sense_type_argument_type"
    sql_create_statement = \
"""
create table pb_sense_type_argument_type
(
  pb_sense_type varchar(25) not null,
  argument_type varchar(25) not null,
  unique key(pb_sense_type, argument_type),
  foreign key (pb_sense_type) references pb_sense_type.id,
  foreign key (argument_type) references argument_type.id
)
default character set utf8;
"""


    sql_insert_statement = \
"""insert into pb_sense_type_argument_type
(
  pb_sense_type,
  argument_type
) values (%s, %s)
"""

    def write_to_db(self, key, cursor):
        for a_argument_type in self.argument_types:
            insert_ignoring_dups(self, cursor, key, a_argument_type)


class frame_set:
    """ information for interpreting a proposition annotation """

    def __init__(self, a_xml_string, a_subcorpus=None, lang_id=None):
        #--------------------------------------------------------------------------------#
        # the definition of a lemma in this case is quite different,
        # in the sense that it can also represent a multi-word
        # predicate, but then still have the sense marked with the
        # single sense with another sense number.  the way we will populate this is
        # by using the value of the id, which is always the core lemma and the
        # sense id, and get the lemma name out of it.  that way things will be quite
        # consistent
        #--------------------------------------------------------------------------------#
        self.lemma = None
        self.subcorpus = a_subcorpus
        self.argument_composition_hash = {}

        if a_subcorpus and not lang_id:
            lang_id = a_subcorpus.language_id


        try:
            a_frameset_tree = ElementTree.fromstring(a_xml_string)
        except SyntaxError:
            raise

        on.common.log.debug(a_frameset_tree, on.common.log.DEBUG, on.common.log.MAX_VERBOSITY)

        if lang_id == "en":
            for a_predicate_tree in a_frameset_tree.findall(".//predicate"):
                on.common.log.debug(a_predicate_tree, on.common.log.DEBUG, on.common.log.MAX_VERBOSITY)

                a_predicate_complex_lemma = on.common.util.get_attribute(a_predicate_tree, "lemma")

                on.common.log.debug(a_predicate_complex_lemma, on.common.log.DEBUG, on.common.log.MAX_VERBOSITY)

                for a_roleset_tree in a_predicate_tree.findall(".//roleset"):
                    a_id = on.common.util.get_attribute(a_roleset_tree, "id")

                    #---- this is just to create a type object  ----#
                    aa_list = a_id.split(".")
                    some_pb_sense_type = on.corpora.sense.pb_sense_type(aa_list[0], aa_list[1])


                    #---- do this just once because this is common for the entire frame file ----#
                    if(self.lemma == None):
                        self.lemma = re.sub("\..*$", "", a_id)


                    #---- lets create an empty argument_composition object -----#

                    if not a_predicate_complex_lemma.strip():
                        a_predicate_complex_lemma = self.lemma

                    a_argument_composition = argument_composition(a_predicate_complex_lemma)

                    if not a_id.strip():
                        raise Exception("id not set")

                    self.argument_composition_hash[a_id] = a_argument_composition

                    for a_role_tree in a_roleset_tree.findall(".//role"):
                        n = on.common.util.get_attribute(a_role_tree, "n")
                        if n.isdigit():
                            a_argument_composition.argument_types.append("ARG%s" % (n))



        elif lang_id in ["ch", "ar"]:

            for a_id_tree in a_frameset_tree.findall(".//id"):
                self.lemma = a_id_tree.text

            for x in ["-v", "-n", "-m", "-j"]:
                if self.lemma.endswith(x):
                    self.lemma = self.lemma[:-2]

            if self.lemma is None or not self.lemma.strip():
                self.lemma = ""
                raise Exception("No lemma set for frame")

            self.lemma = self.lemma.strip()

            for a_ch_frame_set_tree in a_frameset_tree.findall(".//frameset"):
                pb_sense_num = on.common.util.get_attribute(a_ch_frame_set_tree, "id")

                # f1->01; f2->02, etc. in this case
                pb_sense_num = re.sub("^f", "0", pb_sense_num)

                a_id = "%s.%s" % (self.lemma.strip(), pb_sense_num)

                #---- this is just to create a type object  ----#
                some_pb_sense_type = on.corpora.sense.pb_sense_type(self.lemma.strip(), pb_sense_num)


                #---- lets create an empty argument_composition object -----#

                if not a_id.strip():
                    raise Exception("id not set")

                a_argument_composition = argument_composition(self.lemma)
                self.argument_composition_hash[a_id] = a_argument_composition

                for a_role_tree in a_ch_frame_set_tree.findall(".//role"):
                    a_num = on.common.util.get_attribute(a_role_tree, "argnum")
                    if a_num.isdigit():
                        a_argument_composition.argument_types.append("ARG%s" % (a_num))


        else:
            on.common.log.error("please edit this file to accomodate the new language %s" % lang_id)



    def __repr__(self):

        a_string = "<frame_set object: \n"
        for key in self.argument_composition_hash:
            a_string = a_string + "\n" + key
            a_string = a_string + str(self.argument_composition_hash[key]) + "\n"
        a_string = a_string + ">"

        return a_string


    def write_to_db(self, cursor):

        for key in self.argument_composition_hash:
            self.argument_composition_hash[key].write_to_db(key, cursor)


class proposition(object):
    """ a proposition annotation; a line in a .prop file

    Contained by: :class:`proposition_document`

    Contains: :class:`predicate_analogue` , :class:`argument_analogue` , and :class:`link_analogue` (in that order)

    Attributes:

      .. attribute:: lemma

         Which :class:`frame_set` this leaf was annotated against

      .. attribute:: pb_sense_num

         Which sense in the :class:`frame_set` the arguments are relative to

      .. attribute:: predicate

         A :class:`predicate_analogue` instance

      .. attribute:: quality

         gold
           double annotated, adjudicated, release format

      .. attribute:: type

         v
           standard proposition

         n
           nominalization proposition

      .. attribute:: argument_analogues

         A list of :class:`argument_analogue`

      .. attribute:: link_analogue

         A list of :class:`link_analogue`

      .. attribute:: document_id

      .. attribute:: enc_prop

         This proposition and all it contains, encoded as a string.
         Lines in the ``.prop`` files are in this format.

    Methods:

      .. automethod:: write_to_db
      .. automethod:: __getitem__

    """

    DELETE_TBERR_RE = re.compile(" [^ ]+TBERR")

    def __init__(self, encoded_prop, subcorpus_id, document_id, a_proposition_bank=None, tag=None):

        self.document_id = document_id   #---- id of the document
        self.predicate = None            #---- this will contain the predicate object
                                         #     (multi-word predicate is the reason
                                         #      this is required)
        self.argument_analogues = []     #---- this will contain the list of argument
                                         #     objects for this predicate
        self.link_analogues = []         # like argument_analogues but for -LINK arguments

        # iterating over self gives you first the predicate, then the argument analogues, then the link analogues

        self.quality = "gold"
        self.raw_argument_node_pointers = []
        self.lemma_hash = {}
        self.corpus_id  = subcorpus_id
        self.valid = True
        self.original_enc_prop = encoded_prop

        self.errcomms = []

        lang = document_id.split("@")[-2]

        if tag:
            self.tag = tag
        elif a_proposition_bank:
            self.tag = a_proposition_bank.tag
        else:
            self.tag = "gold"

        if encoded_prop:

            def reject(errcode, *comments):
                on.common.log.reject(["docid", document_id, "prop"], "prop",
                                     [[errcode, comments]], self.original_enc_prop)
                self.valid = False
                return None

            #---- variables depending on enc_prop ----#
            #---- six columns making the entire definitoin ----#


            original_enc_prop = encoded_prop
            encoded_prop = re.sub(self.__class__.DELETE_TBERR_RE, "", encoded_prop)
            if encoded_prop != original_enc_prop:
                on.common.log.report("propbank", "removed TBERR argument",
                                     "changed: %s\nto: %s" % (original_enc_prop, encoded_prop))

            if self.invalid_rel_index(encoded_prop):
                return reject("invrel")

            s_link_node_id_re = re.compile("(\d+:0)-LINK-SLC")
            s_link_node_id_list = s_link_node_id_re.findall(encoded_prop)

            original_enc_prop = encoded_prop
            for s_link_node_id in s_link_node_id_list:
                if not re.findall("%s-A" % (s_link_node_id), encoded_prop):
                    new_s_link_node_id = "%s:1" % (s_link_node_id.split(":")[0])
                    encoded_prop = re.sub("%s-LINK-SLC" % (s_link_node_id), "%s-LINK-SLC" % (new_s_link_node_id), encoded_prop)

            if original_enc_prop != encoded_prop:
                on.common.log.report("propbank", "messed with slc link stuff",
                                     "changed: %s\nto: %s" % (original_enc_prop, encoded_prop))

            original_enc_prop = encoded_prop
            encoded_prop = encoded_prop.replace("-Sup", "-rel-Sup")
            encoded_prop = encoded_prop.replace("-rel-Support", "-rel-Sup")
            encoded_prop = encoded_prop.replace("-rel-rel-Sup", "-rel-Sup")
            encoded_prop = encoded_prop.replace("-REL-rel-Sup", "-rel-Sup")
            if original_enc_prop != encoded_prop:
                 on.common.log.report("propbank", "messed with -Sup",
                                      "changed: %s\nto: %s" % (original_enc_prop, encoded_prop))

            prop_def_list = encoded_prop.split(" ", 7)

            sentence_index = prop_def_list[1]
            predicate_index = prop_def_list[2]

            self.quality    = prop_def_list[3].lower()
            a_predicate_type = prop_def_list[4].split("-")[-1]

            if a_predicate_type not in predicate_type.allowed:
                return reject("invpred")

            lemma_and_sense = prop_def_list[5]


            #---- if the sense is defined, extract it ----#
            if "." in lemma_and_sense:
                self.lemma = lemma_and_sense.split(".")[0]
                self.pb_sense_num = lemma_and_sense.split(".")[1]
                ltype = "m" if a_predicate_type == "m" else "v"

                if self.pb_sense_num == "XX":
                    pass
                else:
                    try:
                        test_int = int(self.pb_sense_num)
                    except Exception:
                        return reject("invsense")

                    if (a_proposition_bank is not None and
                        a_proposition_bank.frames_loaded() and
                        not a_proposition_bank.is_valid_frameset(self.lemma, ltype, self.pb_sense_num)):

                        if not a_proposition_bank.is_valid_lemma(self.lemma, ltype):
                            return reject("invframe", "lemma: %s" % self.lemma)
                        return reject("invfsid",
                                      "lemma: %s" % self.lemma,
                                      "fsid: %s" % self.pb_sense_num)

            else:
                return reject("nosense")


            #---- this marks the span of this proposition ----#
            self.min_left  = 1000
            self.max_right = 0

            #---- we won't make these attributes of the proposition class because they will be store properly in the predicate nad argument classes respectively ---#
            predicate_properties   = prop_def_list[6]
            enc_predicate_argument = prop_def_list[7]

            on.common.log.debug("encoded predicate argument: %s" % (enc_predicate_argument), on.common.log.DEBUG, on.common.log.MAX_VERBOSITY)


            enc_argument_list = []
            enc_predicate_list = []
            enc_syntactic_link_list = []

            enc_predicate_argument_parts = enc_predicate_argument.split()
            for enc_predicate_argument_part in enc_predicate_argument_parts:

                try:
                    (node_pointers, arg_type) = enc_predicate_argument_part.split("-", 1)
                except Exception:
                    return reject("notype")

                arg_type = arg_type.upper()

                if "ARG" in arg_type or "REL-SUP" == arg_type:
                    enc_argument_list.append(enc_predicate_argument_part)
                elif "REL" in arg_type:
                    enc_predicate_list.append(enc_predicate_argument_part)
                elif "LINK" in arg_type:
                    enc_syntactic_link_list.append(enc_predicate_argument_part)
                else:
                    return reject("badtype",
                                  "type: %s" % arg_type,
                                  "encoded part: %s" % enc_predicate_argument_part)

            if len(enc_predicate_list) != 1:
                return reject("wrongnpred")
            else:
                enc_predicates = enc_predicate_list[0].split("-")[0]
                if "*" in enc_predicates:
                    return reject("tracepred")
                self.predicate = predicate_analogue(enc_predicates, a_predicate_type, sentence_index, predicate_index, self)

            if self.get_primary_predicate() is None:
                return reject("nopripred")

            self.raw_argument_node_pointers_hash = {}
            #---- each argument list is in the form of space separated node(s)-argument pairs which are joined using a "-" -----#

            for enc_argument_analogue in enc_argument_list:
                #---- found actual argument information ----#

                try:
                    a_argument_analogue = argument_analogue(enc_argument_analogue, self)
                except BadArgumentTypeException, bate:
                    return reject("badargtype",
                                  "bad argument type: %s" % bate,
                                  "argument: %s" % enc_argument_analogue)
                except Exception:
                    return reject("badargtype",
                                  "argument: %s" % enc_argument_analogue)

                #--------------------------------------------------------------#
                # although most of the following code is part of the
                # argument analogue construction, we need to replicate it
                # here so as to be able to account for the correctness of the
                # LINKs
                #--------------------------------------------------------------#
                (a_node_collection, the_argument) = enc_argument_analogue.split("-", 1)
                a_possible_node_set = a_node_collection.split("*")  #---- "possible" because there might be a node which is a aggregate separated by a ","
                for a_possible_node in a_possible_node_set:
                    for a_node in a_possible_node.split(","):  #---- this is to ensure that we consider all the sub-nodes

                        #--------------------------------------------------------------#
                        # we don't care of multiple nodes get assigned the
                        # same argument, and we take advantage of the fact
                        # that a node does not belong to more than one
                        # arguments in a given proposition. also we want
                        # the argument_analogue object as we need the
                        # actual argument id
                        #--------------------------------------------------------------#
                        self.raw_argument_node_pointers_hash[a_node] = a_argument_analogue


            #---- handle the links here.  most often this is going to be one link only ----#
            for enc_syntactic_link in enc_syntactic_link_list:

                #---- first let's find out which of the arguments this connection belongs to ----#
                (enc_link_node_pointers, a_link_type) = enc_syntactic_link.split("-", 1)

                if ";" in enc_link_node_pointers:
                    return reject("linksemi")

                number_of_asterixs = enc_link_node_pointers.count("*")
                if( number_of_asterixs == 0 ):
                    return reject("singlink")

                #if(a_link_type == "LINK-SLC" and number_of_asterixs != 1):
                #    drop("excess slc links (too many asterixes)")
                #    return

                found_connection_flag = False
                for a_node_pointer in enc_link_node_pointers.split("*"):
                    if a_node_pointer in self.raw_argument_node_pointers_hash:
                        found_connection_flag = True
                        the_respective_argument_analogue = self.raw_argument_node_pointers_hash[a_node_pointer]
                        break

                if not found_connection_flag:
                    return reject("mlinka")

                if a_link_type not in link_type.allowed:
                    return reject("invltype", "link type: %s" % a_link_type)

                link_analogue(enc_link_node_pointers, a_link_type, self, the_respective_argument_analogue)

            fs_args = self.canonical_enc_prop_args(encoded_prop)
            ag_args = self.canonical_enc_prop_args(self.enc_prop)

            if fs_args != ag_args:
                on.common.log.report("proposition", "enc_prop_issue", proposition_on_disk=fs_args, auto_generated_prop=ag_args)


            if a_proposition_bank is not None and a_proposition_bank.frames_loaded() and self.pb_sense_num != "XX":
                for a_argument_analogue in self.argument_analogues:
                    argument_number = a_argument_analogue.get_argument_number()
                    if not a_proposition_bank.is_valid_argument_number(self.lemma, ltype, self.pb_sense_num, argument_number):
                        return reject("invargno", "lemma: %s" % self.lemma,
                                                  "fsid: %s" % self.pb_sense_num,
                                                  "argno: %s" % argument_number)


        else:
            pass

    @property
    def id(self):
        return "%s@%s" % (self.predicate.token_index, self.predicate.tree_id)

    def get_enc_prop_list(self, short_args_form=False, no_links_form=False):

        def move_down(x_tree):
            while len(x_tree.children) == 1:
                x_tree = x_tree.children[0]
            return x_tree

        def same_subtree(x_tree, y_tree):
            return move_down(x_tree) is move_down(y_tree)

        def find_argument_to_use(a_prop, a_argument_analogue):
            """ return the closest trace node and the other nodes """

            ratings = []
            arg_nodes = []
            for a_argument in a_argument_analogue:
                assert len(a_argument) == 1 # we decided to punt on cases with both , and *
                a_argument_node = a_argument[0]
                arg_nodes.append(a_argument_node)

                if not a_argument_node.subtree or not move_down(a_argument_node.subtree).is_trace():
                    continue

                ratings.append((abs(a_argument_node.subtree.start - a_prop.get_primary_predicate().subtree.start), a_argument_node))
            ratings.sort()

            # if ratings is empty there is a data error that should
            # have been caught elsewhere; A*B requires a trace
            # connecting A and B

            if not ratings:
                on.common.log.report("proposition", "A*B not linked when should be",
                                     tree=a_argument[0].subtree.get_root().pretty_print()
                                       if a_argument[0].subtree else "no subtree",
                                     enc_prop=self.enc_prop,
                                     subtrees="\n".join(a_argument[0].subtree.to_string() for a_argument in a_argument_analogue))

                argument_node_to_use = a_argument_analogue[0][0]
            else:
                argument_node_to_use_dist, argument_node_to_use = ratings[0]

            return argument_node_to_use, [x for x in arg_nodes if x is not argument_node_to_use]

        def nid(x):
            return "%s:%s" % (x.get_token_index(), x.get_height())

        def lookup_by_nid(nid):
            token_index, height = nid.split(":")
            a_tree = self.get_tree()
            leaf = a_tree[int(token_index)]
            node = leaf
            height = int(height)
            while height > 0:
                node = node.parent
                height -= 1
            return node

        def all_reference_leaves(a_node):
            if a_node.reference_leaves:
                for n in a_node.reference_leaves:
                    yield n
            if a_node.parent and len(a_node.parent.children) == 1:
                for n in all_reference_leaves(a_node.parent):
                    yield n

        def find_all_coreferent_nodes(a_node, add_to):
            def find_forward(n):
                if n.identity_subtree:
                    m = move_down(n.identity_subtree)
                    add_to.add(m)
                    find_forward(m)
            def find_backward(n):
                for m in all_reference_leaves(n):
                    add_to.add(m)
                    find_backward(m)

            add_to.add(a_node)
            find_forward(a_node)
            find_backward(a_node)

        def get_node_list(s):
            return s.split("-")[0].replace(",","*").replace(";","*").split("*")


        def node_compare(node_one, node_two):
            return cmp(int(node_one.split(":")[0]), int(node_two.split(":")[0]))


        def unique_reorder_enc_arg(enc_arg):
            left, right = enc_arg.split("-", 1)
            nodes = list(set(left.split("*")))
            nodes.sort(node_compare)

            enc_arg = "%s-%s" % ("*".join(nodes), right)
            return enc_arg


        enc_args = []
        seen_link = False
        modify_links_by = {} # old subtree -> new subtree
        for a_thing in self:
            enc_arg = a_thing.enc_self
            if not enc_arg:
                continue

            if a_thing.analogue_type != "link":
                assert not seen_link # links have to come after arguments or we'll mess them up
            else:
                seen_link = True

            if no_links_form and a_thing.analogue_type == "link":
                continue

            if no_links_form:
                already_linked_nodes = get_node_list(enc_arg)

                # the idea of the mess below is that we want to
                # look at every node in a_thing, and if it is
                # linked to some other subtree via a link we want
                # to connect them in the output with a star.
                # Sameer used to call this "normalizing
                # propbank". The code is a little messy, though.

                for a_sub_thing in a_thing:
                    for a_node in a_sub_thing:
                        if a_node.subtree:
                            for a_link_node in a_node.subtree.link_node_list:
                                for a_link in a_link_node.link.link_analogue:
                                    for b_link_node in a_link:
                                        if b_link_node is not a_link_node:
                                            if b_link_node.subtree:
                                                if nid(b_link_node.subtree) not in already_linked_nodes:
                                                    new_enc_arg = "%s*%s" % (nid(b_link_node.subtree), enc_arg)
                                                    new_enc_arg = unique_reorder_enc_arg(new_enc_arg)
                                                    print "    Changing enc_arg from %s to %s" % (enc_arg, new_enc_arg)
                                                    enc_arg = new_enc_arg
                assert "LINK" not in enc_arg

            elif short_args_form:
                """ for arguments and predicates, return A-ARG instead of A*B*C-ARG.  Adjust links to match """

                if a_thing.analogue_type != "link":

                    if "*" not in enc_arg:
                        pass # no trace chains to change
                    elif "," in enc_arg or ";" in enc_arg:
                        pass # these are rare (500 out of 300,000) and tricky, leave alone
                    else:
                        # first find the argument to use
                        # returns None, None if there are non-coreferential A*B pairs
                        #
                        argument_node_to_use, other_argument_nodes = find_argument_to_use(self, a_thing)
                        if argument_node_to_use is not None:
                            enc_arg = "%s-%s" % (argument_node_to_use.node_id, a_thing.enc_self_type)
                            # then note that we need to modify links appropriately
                            for argument_node_not_to_use in other_argument_nodes:
                                modify_links_by[argument_node_not_to_use.subtree] = argument_node_to_use.subtree

                else: # link
                    seen_link = True

                    for find, replace in modify_links_by.iteritems():
                        for a_link in a_thing:
                            for a_link_node in a_link:
                                if same_subtree(a_link_node.subtree, find):
                                    a_link_node.subtree = replace
                    enc_arg = a_thing.enc_self

            else:
                pass

            if not short_args_form and self.get_tree() and a_thing.analogue_type != "link":
                all_referenced_nodes = set()
                all_existing_nodes = set(move_down(lookup_by_nid(nid)) for nid in get_node_list(enc_arg))

                for existing_node in all_existing_nodes:
                    find_all_coreferent_nodes(existing_node, all_referenced_nodes)

                assert not (all_existing_nodes - all_referenced_nodes)

                for node_to_add in all_referenced_nodes - all_existing_nodes:
                    enc_arg = "%s*%s" % (nid(node_to_add), enc_arg)
                    enc_arg = unique_reorder_enc_arg(enc_arg)

            enc_args.append(enc_arg)

        assert enc_args # if there's not at least one argument, something is really screwed up
        return [self.document_id, self.predicate.sentence_index, self.predicate.token_index,  self.quality,
                "%s-%s" % (self.lemma, self.predicate.type), "%s.%s" % (self.lemma, self.pb_sense_num),
                "-----"] + enc_args

    @property
    def enc_prop(self):
        return " ".join(str(x) for x in self.get_enc_prop_list())

    @property
    def path_enc_prop(self):
        if self.tag != "gold":
            parse_ext = self.tag + "_parse"
        else:
            parse_ext = "parse"



        data, language, annotations, genre, source, section, docid = on.common.util.output_file_name(self.document_id, parse_ext).split("/")
        assert data == "data", annotations == "annotations"

        ofn = "/".join(["on", language, annotations, "parse", genre, source, section, docid])

        return " ".join(str(x) for x in [ofn] + self.get_enc_prop_list()[1:])

    @property
    def tree_id(self):
        return self.predicate.tree_id

    def enrich_tree(self, a_tree):
        for a_thing in self:
            a_thing.enrich_tree(a_tree)

    def get_tree(self):
        """ return our tree, or None if this is pre enrichment or anything is wrong """

        try:
            return self.get_primary_predicate().subtree.get_root()
        except Exception:
            return None

    def __getitem__(self, idx):
        """ return first the predicate_analogue, then the argument analogues, then the link analogues """

        return ([self.predicate] + self.argument_analogues + self.link_analogues)[idx]

    def get_index_of(self, a_analogue):
        """ returns indecies into the separate bins, *not* into the proposition.

        This means argument analogues have sequential indecies
        starting at zero, for example.  It also means that the
        following code evaluates to False:

        .. code-block:: python

           a_analogue == a_analogue.proposition[a_analogue.proposition.get_index_of(a_analogue)]

        """

        if a_analogue == self.predicate:
            return 0
        if a_analogue in self.argument_analogues:
            return self.argument_analogues.index(a_analogue)
        return self.link_analogues.index(a_analogue)

    def __len__(self):
        return 1 + len(self.argument_analogues) + len(self.link_analogues)

    def _get_pb_sense_num(self):
        return self._pb_sense_num

    def _set_pb_sense_num(self, val):
        self._pb_sense_num = val


    pb_sense_num = property(_get_pb_sense_num, _set_pb_sense_num)

    def invalid_rel_index(self, encoded_prop):
        """ if the token index given by the rel argument disagrees
        with the top level token index """

        token_index_master = encoded_prop.split()[2]

        rel_bits = [b for b in encoded_prop.split()
                    if b.endswith("-rel") or b.endswith("-REL")]

        if len(rel_bits) != 1 or len(rel_bits[0].split('-')) != 2:
            return True

        for ref_bit in rel_bits[0].split('-')[0].split(','):
            if ref_bit.split(':')[0] == token_index_master:
                return False

        return True


    def all_nodes(self):
        return [a_node
                for a_analogue in self
                for a_node_holder in a_analogue
                for a_node in a_node_holder]



    def args_overlap(self, ignore_traces=False):
        """ are any tree leaves claimed by multiple arguments?

        Exceptions:

          - ``*ICH*`` and ``*PRO*`` leaves are considered but their targets are not
          - the tree's root, if tagged, is not considered overlapping

        """

        def get_arg_start_ends(a_tree):
            """ return a sorted list of (start end) token index pairs for
            each subtree that is refered by any argument.  Good for
            checking for overlaps.  End is exclusive. """

            start_ends = []
            for a_analogue in self:
                if a_analogue.analogue_type == "link":
                    continue

                for a_node_holder in a_analogue:
                    for a_node in a_node_holder:
                        if a_node.subtree:
                            leaves = list(a_node.subtree.leaves())

                            if not ignore_traces or len(leaves) != 1 or not leaves[0].is_trace():

                                consider = not a_node.subtree.is_root() # the root doesn't overlap
                                for a_reference_leaf in a_node.subtree.reference_leaves:
                                    if a_reference_leaf.trace_type in ["*ICH*", "*PRO*", "*pro*"]:
                                        consider = False
                                if consider:
                                    start_ends.append((a_node.subtree.start, a_node.subtree.end, a_node))

            start_ends.sort()

            return start_ends


        if not self.get_primary_predicate() or not self.get_primary_predicate().subtree:
            return False
        a_tree = self.get_primary_predicate().subtree.get_root()

        last_end = 0
        last_node = None
        for start, end, a_node in get_arg_start_ends(a_tree):
            if start < last_end:
                return [len(list(last_node.subtree.leaves())), list(last_node.subtree.leaves()), list(last_node.subtree.leaves())[0],
                        list(last_node.subtree.leaves())[0].is_trace(),
                        ignore_traces, not ignore_traces or len(list(last_node.subtree.leaves())) != 1 or not list(last_node.subtree.leaves())[0].is_trace()]

            last_end = end
            last_node = a_node

        return []

    def add(self, a_analogue):
        """ put the analogue into self.argument_analogues, self.link_analogues, or self.predicate as appropriate

        Note that this is called by the various analogues in their constructors

        """

        add_fn = { "argument" : self.add_argument_analogue,
                   "link" : self.add_link,
                   "predicate" : self.set_predicate }[a_analogue.analogue_type]
        add_fn(a_analogue)

    def add_link(self, a_link_analogue):
        self.link_analogues.append(a_link_analogue)

    def add_argument_analogue(self, a_argument_analogue):
        self.argument_analogues.append(a_argument_analogue)

    def get_predicate(self):
        return self.predicate

    def set_predicate(self, predicate):
        self.predicate = predicate


    def get_primary_predicate(self):
        return self.get_predicate().get_primary_predicate()

    def get_argument_analogues(self):
        return self.argument_analogues



    def __repr__(self):
        a_string = """
%s
proposition object:
------------------
id        : %s
doc_id    : %s
tree_id  : %s
frame    : %s.%s
enc_prop : %s
""" % ("-"*40, self.id, self.document_id, self.tree_id, self.lemma, self.pb_sense_num, self.enc_prop)

        a_string += "predicate:\n"
        a_string += str(self.predicate)
        a_string += "\n"
        a_string += "arguments:\n"

        a_string += "\n".join([str(a_argument_analogue) for a_argument_analogue in self.argument_analogues])

        a_string += "\n"
        a_string += "links:\n"
        a_string += "\n".join([str(a_link_analogue) for a_link_analogue in self.link_analogues])

        a_string = a_string + "-"*40
        a_string = a_string + "\n"

        return a_string


    sql_table_name = "proposition"
    sql_create_statement = \
"""
create table proposition
(
  id varchar(255) not null collate utf8_bin primary key,
  document_id varchar(255) not null,
  encoded_proposition text not null,
  quality varchar(16) not null,
  foreign key (document_id) references document.id
)
default character set utf8;
"""


    sql_insert_statement = \
"""insert into proposition
(
  id,
  document_id,
  encoded_proposition,
  quality
) values (%s, %s, %s, %s)
"""

    def write_to_db(self, cursor):
        """ write this proposition and all its components to the database """

        cursor.executemany("%s" % (self.__class__.sql_insert_statement), [(self.id, self.document_id, self.enc_prop.strip(), self.quality)])

        for a_analogue in self:
            a_analogue.write_to_db(cursor)

    @classmethod
    def canonical_enc_prop(cls, a_enc_prop):
        """ turn this encoded proposition into canonical form -- rel first

        return None on error

        """

        a_enc_prop = a_enc_prop.strip()

        enc_prop_stable = a_enc_prop.split("-----")[0]
        enc_prop_variable = cls.canonical_enc_prop_args(a_enc_prop)

        if not enc_prop_stable or not enc_prop_variable:
            return None

        return "-----".join([enc_prop_stable, enc_prop_variable])

    @classmethod
    def canonical_enc_prop_args(cls, a_enc_prop):
        a_enc_prop = a_enc_prop.strip()

        enc_prop_variable_bits = a_enc_prop.split("-----")[1].split(" ")

        pred_bit = None
        new_a_bits = []
        new_l_bits = []
        for v_bit in enc_prop_variable_bits:
            if v_bit.endswith("-rel") or v_bit.endswith("-REL"):
                if pred_bit:
                    return None
                pred_bit = v_bit.replace("-REL", "-rel")
            elif "-LINK" in v_bit:
                new_l_bits.append(v_bit)
            elif v_bit:
                bits = v_bit.split("-")
                if len(bits) == 3 and bits[1].startswith("ARG") and not bits[1] == "ARGM":
                    v_bit = "%s-%s" % (bits[0], bits[1])
                new_a_bits.append(v_bit)

        if not pred_bit:
            return None

        return " ".join([pred_bit] + new_a_bits + new_l_bits)

class proposition_document:
    """
    Contained by: :class:`proposition_bank`

    Contains: :class:`proposition`

    """

    def __init__(self, document_id, extension="prop"):
        self.document_id = document_id
        self.extension=extension
        self.propositions = []

    def append(self, a_proposition):
        self.propositions.append(a_proposition)

    def __getitem__(self, index):
        return self.propositions[index]

    def __len__(self):
        return len(self.propositions)

    def __repr__(self):
        return "proposition_document instance, id=%s, propositions:\n%s" % (
            self.document_id, on.common.util.repr_helper(enumerate(a_proposition.id for a_proposition in self)))

    def write_to_db(self, a_cursor):
        for a_proposition in self:
            if a_proposition.valid:
                a_proposition.write_to_db(a_cursor)

    def dump_view(self, a_cursor=None, out_dir="",
                  short_args_form=True, no_links_form=None):
        """ write out to a file

        if short_args_form, use:
          A-ARG A*D-LINK instead of A*B*C-ARG C*D-LINK

          we would do A,B-ARG instead of A,B*C-ARG but these can be
          tricky, so if we have both stars and commas we act as if
          short_args_form is false.

        if no_links_form, use:

          A*B*C*D-ARG instead of A*B*C-ARG C*D-LINK

        no_links_form takes precedence over short_args_form

        """

        if no_links_form is None:
            if self.document_id.split("@")[-2] == "ar": # arabic
                no_links_form = True
            else:
                no_links_form = False

        ext = self.extension

        with codecs.open(on.common.util.output_file_name(self.document_id, ext, out_dir), "w", "utf-8") as f:
                enc_prop_lists = [a_proposition.get_enc_prop_list(short_args_form=short_args_form, no_links_form=no_links_form)
                                  for a_proposition in self if a_proposition.valid]

                for a_enc_prop_list in sorted(enc_prop_lists):
                    f.write(" ".join([str(x) for x in a_enc_prop_list]) + "\n")



class proposition_bank(abstract_bank):
    """
    Extends: :class:`on.corpora.abstract_bank`

    Contains: :class:`proposition_document`

    """

    def __init__(self, a_subcorpus, tag, a_cursor=None, extension="prop", a_frame_set_hash = None):
        abstract_bank.__init__(self, a_subcorpus, tag, extension)
        self.lemma_hash = {}

        self.frame_set_hash = a_frame_set_hash if a_frame_set_hash else {}

        if(a_cursor == None):
            if not self.frame_set_hash:
                self.frame_set_hash = self.build_frame_set_hash(a_subcorpus.top_dir, a_subcorpus.language_id, self.lemma_hash)

            sys.stderr.write("reading the proposition bank [%s] ..." % self.extension)
            for a_file in self.subcorpus.get_files(self.extension):
                a_proposition_document = proposition_document("%s@%s" % (a_file.document_id, a_subcorpus.id), extension)
                sys.stderr.write(".")
                proposition_file = codecs.open(a_file.physical_filename, "r", "utf-8")

                try:
                    for encoded_proposition in proposition_file:
                        #---- check here, before sending the encoded proposition for object creation whether there is atleast a REL/rel defined ----#
                        if "rel" not in encoded_proposition and "REL" not in encoded_proposition:
                            on.common.log.reject(["docid", a_proposition_document.document_id, "prop"], "prop",
                                                 [["nopripred", []]], encoded_proposition)
                        else:
                            try:
                                a_proposition = proposition(encoded_proposition, a_subcorpus.id, a_proposition_document.document_id, self)
                            except Exception:
                                trb = traceback.format_exc()
                                on.common.log.reject(["docid", a_proposition_document.document_id, "prop"], "prop",
                                                     [["syntaxerr", [["traceback", trb]]]], encoded_proposition)
                                continue

                            if a_proposition and a_proposition.predicate and a_proposition.valid:
                                #---- update the lemma hash ----#
                                self.lemma_hash["%s-%s" % (a_proposition.lemma, a_proposition.predicate.type)] = 0

                                a_proposition_document.append(a_proposition)
                            else:
                                on.common.log.report("propbank", "Dropped prop (probably dup err)", given=encoded_proposition)
                finally:
                    proposition_file.close()

                self.append(a_proposition_document)

            sys.stderr.write("\n")

        else:
            if not self.frame_set_hash:
                self.frame_set_hash = on.common.util.make_db_ref(a_cursor)

    def frames_loaded(self):
        return not is_not_loaded(self.frame_set_hash)

    @staticmethod
    def build_frame_set_hash(top_dir, language_id, lemma_hash={}):
        """ Read all the frame files from disk and return a hash of :class:`frame_set` instances """

        frame_set_hash = {}

        sys.stderr.write("reading the frames files ....")

        def list_frames(basedir):
            frame_sets = []
            for curpath, curdirs, curfiles in os.walk(basedir):
                for curfile in curfiles:
                    if curfile.endswith(".xml"):
                        frame_sets.append((curfile, os.path.join(curpath, curfile)))
            return frame_sets

        #---- lets process the framesets ----#
        for frame_set_file_name, frame_set_file_name_full in list_frames("%s/metadata/frames" % top_dir):

            prop_type = "v"
            fname_lemma = frame_set_file_name.replace(".xml", "")

            for x in ["n",   # noun
                      "j",   # adj
                      "v",  # verb
                      ]:
                if frame_set_file_name.endswith("-%s.xml" % x):
                    prop_type = x
                    fname_lemma = frame_set_file_name.replace("-%s.xml" % x, "")

            if language_id in ["en", "ar"]:
                #---- check if we want to add this lemma (whether there are any instances annotated in the frame bank ----#
                a_lemma = fname_lemma
            elif language_id == "ch":

                frame_set_file = codecs.open(frame_set_file_name_full, "r", "utf-8")

                try:
                    frame_set_file_string = frame_set_file.read()
                except UnicodeDecodeError, e:
                    continue

                try:
                    a_lemma = re.findall("<id>\s+(.*?)\s+</id>", frame_set_file_string)[0]
                except Exception, e:
                    continue
            else:
                on.common.log.error("please change this code to address the new langauge (given %s)" % language_id, False)
                break

            lemma_pos = "%s-%s" % (a_lemma, prop_type)
            if lemma_hash and not lemma_hash.has_key(a_lemma):
                on.common.log.debug("skipping %s ...." % (a_lemma), on.common.log.DEBUG, on.common.log.MAX_VERBOSITY)
                continue
            else:
                on.common.log.debug("adding %s ...." % (a_lemma), on.common.log.DEBUG, on.common.log.MAX_VERBOSITY)


            on.common.log.debug("processing %s ...." % (frame_set_file_name), on.common.log.DEBUG, on.common.log.MAX_VERBOSITY)
            sys.stderr.write(".")

            with codecs.open(frame_set_file_name_full, "r", "utf-8") as frame_set_file:
                try:
                    frame_set_file_string = frame_set_file.read()
                    a_frame_set = frame_set(frame_set_file_string, lang_id=language_id)
                    on.common.log.debug(a_frame_set, on.common.log.MAX_VERBOSITY)

                    if a_frame_set.lemma != a_lemma:
                        a_frame_set.lemma = a_lemma

                    frame_set_hash[lemma_pos] = a_frame_set
                except Exception, e:
                    on.common.log.report("prop", "found some problem processing frame file", fname=frame_set_file_name)

        sys.stderr.write("\n")

        return frame_set_hash

    def is_valid_argument_number(self, lemma, pos, frameset, argument_number):
        if argument_number is None:
            return True # unnumbered arguments are always fine

        if not is_db_ref(self.frame_set_hash):
            return True # this was already checked when ran the data through the db

        a_cursor = self.frame_set_hash["DB"]
        try:
            a_cursor.execute("""SELECT argument_type
                                FROM   pb_sense_type_argument_type
                                WHERE  pb_sense_type = '%s.%s'""" % esc(lemma, frameset))

            allowed_argument_types = [row['argument_type'] for row in a_cursor.fetchall()]
            given_argument_type = "ARG%s" % (argument_number)

            return given_argument_type in allowed_argument_types

        except MySQLdb.Error:
            on.common.log.report("proposition", "issue with lemma db argument number lookup",
                                 lemma=lemma, fsid=frameset)
            return True



    def is_valid_frameset(self, lemma, pos, frameset):
        if frameset is None:
            return False

        return self.is_valid_frameset_helper(self.frame_set_hash, lemma, pos, frameset)

    def is_valid_lemma(self, lemma, pos):
        return self.is_valid_frameset_helper(self.frame_set_hash, lemma, pos)

    @classmethod
    def is_valid_frameset_helper(cls, a_frame_set_hash, lemma, pos, frameset=None):
        if not lemma or (not frameset and frameset is not None):
            return False

        if is_db_ref(a_frame_set_hash):
           a_cursor = a_frame_set_hash["DB"]
           try:
               if frameset:
                   a_cursor.execute("""SELECT id
                                       FROM   pb_sense_type
                                       WHERE  id = '%s.%s'""" % esc(lemma, frameset))
               else:
                   a_cursor.execute("""SELECT id
                                       FROM   pb_sense_type
                                       WHERE  id regexp '^%s.0'""" % esc(lemma))
               return a_cursor.fetchall()
           except MySQLdb.Error:
               on.common.log.report("proposition", "issue with lemma db lookup",
                                    lemma=lemma, fsid=frameset)
               return False

        return frameset in cls.list_valid_frameset_helper(a_frame_set_hash, lemma, pos)

    @classmethod
    def list_valid_frameset_helper(cls, a_frame_set_hash, lemma, pos):
        if is_db_ref(a_frame_set_hash):
            raise Exception("Not supported -- use is_valid_frameset")

        lemma_pos = "%s-%s" % (lemma, pos)
        if lemma_pos not in a_frame_set_hash:
            return []

        return [x.split(".")[1] for x in
                a_frame_set_hash[lemma_pos].argument_composition_hash]

    def list_valid_framesets(self, lemma, pos):
        """ given a lemma, return a list of the valid frame
        references.  For example:

        get_valid_frame_references('keep') == [
            '01', '02', '03', '04', '05', '06', '08'].
        """
        return self.list_valid_frameset_helper(self.frame_set_hash, lemma, pos)


    def check_proposition(self, a_proposition, mode="normal", document_id=None, ignore_errors=False):
        """ if ignore_errors is set, ignore minor errors """

        pripred=None
        if not a_proposition.get_primary_predicate():
            a_subtree = None
        else:
            pripred=a_proposition.get_primary_predicate()
            a_subtree = a_proposition.get_primary_predicate().subtree

        if not document_id:
            document_id = a_proposition.document_id

        def reject(errcomms, full_reject=False):

            where = ["docid", document_id, "prop"]
            dropped_from = "prop"


            if mode == "normal" or full_reject:
                on.common.log.reject(where, dropped_from, errcomms, a_proposition.original_enc_prop)
            else:
                on.common.log.adjust(where, dropped_from, errcomms,
                                     a_proposition.original_enc_prop,
                                     a_proposition.path_enc_prop)
            def is_warning(errcode):
                # errors are in the form [prefix]+[suffix] where
                # prefix is two digits and suffix is three.  If the
                # suffix starts with 5, then this is a real error,
                # otherwise it's just a warning.
                #
                # We use dropped_from and errcode to look up the
                # appropriate suffix in the ERRS table in
                # on/common/log.py
                suffix = on.common.log.ERRS[dropped_from][1][errcode][0]
                return suffix[0] != "5"

            dropme = any(not is_warning(errcode) for errcode, comments in errcomms)
            if dropme:
                a_proposition.valid = False

            return None

        if not a_subtree:
            reject([["nopripred", ["pripred: %s" % pripred]]], full_reject=True)
            return

        errcomms = []

        def adderr(errcode, *comments):
            errcomms.append([errcode, comments])

        if a_subtree.language == "en" and a_subtree.is_aux(prop=True):
            adderr("notinc", "auxilliary verb")

        def is_vp_directed_trace(a_leaf):
            if not a_leaf.identity_subtree:
                return False

            return a_leaf.identity_subtree.tag.startswith("VP")


        nounable = any(a_leaf.is_noun() for a_leaf in a_subtree)
        verbable = any(a_leaf.is_verb() for a_leaf in a_subtree) or any(is_vp_directed_trace(a_leaf) for a_leaf in a_subtree)

        if "-n " in a_proposition.enc_prop and not nounable:
            adderr("nnotn")
        elif "-v " in a_proposition.enc_prop and not verbable:
            adderr("vnotv")

        if not verbable:
            adderr("notinc", "coverage calculated only on verbs")

        if a_subtree.get_height() != 0:
            adderr("hnotzero", "height: %s" % a_subtree.get_height())

        if a_proposition.args_overlap(ignore_traces=True):
            adderr("ovargnt", "debug output: %s" % a_proposition.args_overlap(ignore_traces=True))
        elif a_proposition.args_overlap():
            adderr("ovarg", "debug output: %s" % a_proposition.args_overlap())

        if a_subtree.language in ["ch", "ar"]:
            leaf_lemma = a_subtree[0].get_lemma()

            if leaf_lemma is not None and leaf_lemma != a_proposition.lemma:
                adderr("badlemma",
                       "leaf_lemma='%s'" % leaf_lemma,
                       "prop_lemma='%s'" % a_proposition.lemma)

        for a_node in a_proposition.all_nodes():
            if a_node.errcomms:
                errcomms += a_node.errcomms
            if not a_node.subtree:
                adderr("invrgp", "node id: %s" % a_node.node_id)

        if a_proposition.pb_sense_num == "XX":
            if (a_subtree.language == "ch" and
                a_subtree.is_leaf() and
                a_subtree.part_of_speech == "VV" and
                not a_proposition.argument_analogues):

                adderr("modalXX")
            else:
                adderr("invsenseXX")


        errcomms += a_proposition.errcomms

        assert a_proposition.valid # nothing should have invalidated the prop before we got it

        def is_serious(errcom):
            """ copy to new trees errors are not serious """
            errcode, comments = errcom
            serious = errcode not in ["notarget", "notracetarget", "leafdiff", "nosubtree", "spanstrees",
                                      "modtok", "modinstrace", "deltrace", "prop_modinstrace", "prop_deltrace",
                                      "invrgp", "invsenseXX", "badargtype", "notinc",
                                      "vnotv"]
            return serious

        if errcomms:
            if ignore_errors and not any(is_serious(errcomm) for errcomm in errcomms):
                pass
            else:
                reject(errcomms)

        if a_proposition.valid and not ignore_errors:
            for a_node in a_proposition.all_nodes():
                assert a_node.subtree


    def enrich_treebank(self, a_treebank, a_cursor=None, ignore_errors=False):

        abstract_bank.enrich_treebank(self, a_treebank)

        for a_proposition_document in self:
            sys.stderr.write(".")

            for a_proposition in a_proposition_document:

                a_tree = a_proposition_document.tree_document.tree_hash[a_proposition.tree_id]

                if not a_proposition.get_primary_predicate():
                    self.check_proposition(a_proposition, ignore_errors=ignore_errors) # will drop it
                    continue

                old_enc_prop = a_proposition.enc_prop
                try:
                    a_proposition.enrich_tree(a_tree)
                except Exception:
                    print old_enc_prop
                    raise

                self.check_proposition(a_proposition, ignore_errors=ignore_errors)


        sys.stderr.write("\n")
        return a_treebank


    sql_table_name = "proposition_bank"
    sql_exists_table = "proposition"

    sql_create_statement = \
"""
create table proposition_bank
(
  id varchar(255) not null collate utf8_bin primary key,
  subcorpus_id varchar(255) not null,
  tag varchar (255) not null,
  foreign key (subcorpus_id) references subcorpus.id
)
default character set utf8;
"""



    sql_insert_statement = \
"""
insert into proposition_bank
(
  id,
  subcorpus_id,
  tag
) values(%s, %s, %s)
"""

    def write_to_db(self, a_cursor):
        abstract_bank.write_to_db(self, a_cursor)
        self.write_frame_set_hash_to_db(self.frame_set_hash, a_cursor)


    @classmethod
    def write_frame_set_hash_to_db(cls, a_frame_set_hash, a_cursor):
        if a_frame_set_hash and not is_not_loaded(a_frame_set_hash) and not is_db_ref(a_frame_set_hash):
            for a_frame_set in a_frame_set_hash.itervalues():
                a_frame_set.write_to_db(a_cursor)


    @classmethod
    def from_db(cls, a_subcorpus, tag, a_cursor, affixes=None):
        #---- create an empty proposition bank ----#
        sys.stderr.write("reading the proposition bank ....")
        a_proposition_bank = proposition_bank(a_subcorpus, tag, a_cursor)

        #---- now get document ids for this treebank ----#
        a_cursor.execute("""select document.id from document where subcorpus_id = '%s';""" % (a_subcorpus.id))
        document_rows = a_cursor.fetchall()

        #---- and process each document ----#
        for document_row in document_rows:
            a_proposition_document = proposition_document(document_row["id"], a_proposition_bank.extension)

            if not on.common.util.matches_an_affix(a_proposition_document.document_id, affixes):
                continue

            sys.stderr.write(".")
            #---- process each proposition in this document  ----#
            a_cursor.execute("""select * from proposition where document_id = '%s';""" % (a_proposition_document.document_id))

            for a_proposition_row in a_cursor.fetchall():

                #---- create an empty proposition object ----#
                a_proposition = proposition("", a_subcorpus.id, a_proposition_document.document_id, a_proposition_bank)
                a_proposition.quality = a_proposition_row["quality"]

                #--- process each predicate in this proposition ----#
                a_cursor.execute("""select * from predicate where proposition_id = '%s' order by index_in_parent asc;""" % (a_proposition_row["id"]))
                for a_predicate_row in a_cursor.fetchall():
                    if not a_proposition.predicate:
                        predicate_analogue("", a_predicate_row["type"], sentence_index=None, token_index=None, a_proposition=a_proposition)
                        a_proposition.lemma = a_predicate_row["lemma"]
                        a_proposition.pb_sense_num = a_predicate_row["pb_sense_num"]

                    else:
                        assert a_proposition.lemma == a_predicate_row["lemma"]
                        assert a_proposition.pb_sense_num == a_predicate_row["pb_sense_num"]
                        assert a_proposition.predicate.type == a_predicate_row["type"]

                    a_predicate = predicate("", sentence_index=None, token_index=None, a_predicate_analogue=a_proposition.predicate)

                    #---- get the predicate part information ----#
                    a_cursor.execute("""select * from predicate_node where predicate_id = '%s' order by index_in_parent asc;"""  % (a_predicate_row["id"]))
                    for predicate_node_row in a_cursor.fetchall():
                        token_index, height = predicate_node_row["node_id"].split(":")

                        a_predicate_node = predicate_node(a_predicate_row["sentence_index"], token_index, height,
                                                          a_predicate, bool(predicate_node_row["primary_flag"]))

                        assert predicate_node_row["index_in_parent"] == a_predicate_node.index_in_parent
                        assert predicate_node_row["node_id"] == a_predicate_node.node_id
                        assert bool(predicate_node_row["primary_flag"]) == a_predicate_node.primary

                maxes = {}
                for a_table, a_index in [["argument", "argument_analogue_index"],
                                         ["proposition_link", "link_analogue_index"]]:
                    a_cursor.execute("""select max(%s) from %s where proposition_id = '%s';""" % (a_index, a_table, a_proposition.id))
                    max_rows = a_cursor.fetchall()
                    assert len(max_rows) == 1
                    max_index = max_rows[0]["max(%s)" % a_index]
                    maxes[a_index] = -1 if max_index is None else int(max_index)

                for a_argument_analogue_index in range(maxes["argument_analogue_index"] + 1):
                    a_argument_analogue = argument_analogue("", a_proposition)

                    assert a_argument_analogue_index == a_argument_analogue.index_in_parent

                    a_cursor.execute("""select * from argument where proposition_id = '%s' and argument_analogue_index = '%s';""" % (
                        a_proposition.id, a_argument_analogue_index))

                    for argument_row in a_cursor.fetchall():
                        if a_argument_analogue.type is not None:
                            assert a_argument_analogue.type == argument_row["type"]
                        else:
                            a_argument_analogue.type = argument_row["type"]

                        a_argument = argument("", a_argument_analogue)

                        #---- get the nodes for this argument ----#
                        a_cursor.execute("""select * from argument_node where argument_id = '%s';""" % (argument_row["id"]))

                        for a_argument_node_row in a_cursor.fetchall():
                            token_index, height = a_argument_node_row["node_id"].split(":")
                            argument_node(None, token_index, height, a_argument)

                        assert a_argument.split_argument_flag == (len(a_argument) > 1)


                for a_link_analogue_index in range(maxes["link_analogue_index"] + 1):

                    a_link_analogue = None
                    a_argument_analogue = None
                    a_cursor.execute("""select * from proposition_link where proposition_id = '%s' and link_analogue_index = '%s';""" % (
                        a_proposition.id, a_link_analogue_index))

                    for link_row in a_cursor.fetchall():
                        if a_link_analogue is None:
                            for b_argument_analogue in a_proposition.argument_analogues:
                                if b_argument_analogue.id == link_row["associated_argument_id"]:
                                    a_argument_analogue = b_argument_analogue
                            assert a_argument_analogue

                            a_link_analogue = link_analogue("", link_row["type"], a_proposition, a_argument_analogue)
                            assert a_link_analogue_index == a_link_analogue.index_in_parent
                        else:
                            assert a_link_analogue.type == link_row["type"]

                        a_link = link("", a_link_analogue)

                        a_cursor.execute("""select * from link_node where link_id = '%s';""" % (link_row["id"]))

                        for a_link_node_row in a_cursor.fetchall():
                            token_index, height = a_link_node_row["node_id"].split(":")
                            link_node(None, token_index, height, a_link)


                assert a_predicate_row["lemma"] == a_predicate.lemma
                assert a_predicate_row["pb_sense_num"] == a_predicate.pb_sense_num
                assert int(a_predicate_row["sentence_index"]) == a_predicate.sentence_index
                assert int(a_predicate_row["token_index"]) == a_predicate.token_index
                assert a_predicate_row["id"] == a_predicate.id
                assert a_predicate_row["proposition_id"] == a_proposition.id
                assert a_proposition_row["id"] == a_proposition.id
                assert a_proposition_row["quality"] == a_proposition.quality

                # compare modulo ';'/',' because that's not available when loading from db until after enrichment
                nosemi_db_enc_prop = a_proposition_row["encoded_proposition"].replace(";", ",")
                nosemi_loaded_enc_prop = a_proposition.enc_prop.replace(";", ",")
                if nosemi_db_enc_prop != nosemi_loaded_enc_prop:
                    on.common.log.report("proposition", "automatic enc_prop doesn't match original enc_prop",
                                         orig_enc_prop=nosemi_db_enc_prop,
                                         auto_enc_prop=nosemi_loaded_enc_prop)

                a_proposition_document.append(a_proposition)

            a_proposition_bank.append(a_proposition_document)

        sys.stderr.write("\n")
        return a_proposition_bank
