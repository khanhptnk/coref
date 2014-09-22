
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
------------------------------------------------
:mod:`name` -- Name-Entity Annotation
------------------------------------------------

See:

 - :class:`on.corpora.name.name_entity`
 - :class:`on.corpora.name.name_bank`

Correspondences:

 ===============================  ======================================================  ===========================================================================
 **Database Tables**              **Python Objects**                                      **File Elements**
 -------------------------------  ------------------------------------------------------  ---------------------------------------------------------------------------
 ``name_bank``                    :class:`name_bank`                                      All ``.name`` files in an :class:`on.corpora.subcorpus`
 None                             :class:`name_tagged_document`                           A ``.name`` file
 ``tree``                         :class:`on.corpora.tree.tree`                           A line in a ``.name`` file
 ``name_entity``                  :class:`name_entity`                                    A single ``ENAMEX``, ``TIMEX``, or ``NUMEX`` span
 None                             :class:`name_entity_set`                                All :class:`name_entity` instances for one :class:`on.corpora.tree.tree`
 ===============================  ======================================================  ===========================================================================

.. autoclass:: name_bank
.. autoclass:: name_tagged_document
.. autoclass:: name_entity
.. autoclass:: name_entity_set

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
from on.common.log import status

import on.common.util

import on.corpora
import on.corpora.tree

from collections import defaultdict
from on.common.util import insert_ignoring_dups
from on.corpora import abstract_bank





class name_entity_type(on.corpora.abstract_type_table):

    allowed = ["PERSON", "NORP", "FAC", "ORG",
               "GPE", "LOC", "PRODUCT", "EVENT",
               "WORK_OF_ART", "LAW", "LANGUAGE",
               "DATE", "TIME", "PERCENT", "MONEY",
               "QUANTITY", "ORDINAL", "CARDINAL"]

    references = [["name_entity", "type"]]

    sql_table_name = "name_entity_type"
    sql_create_statement = \
"""
create table name_entity_type
(
  id varchar(255) not null collate utf8_bin primary key
)
default character set utf8;
"""


    sql_insert_statement = \
"""insert into name_entity_type
(
  id
) values (%s)
"""





class name_entity(on.corpora.tree_alignable_sgml_span):
    """ A name annotation

    Contained by: :class:`name_entity_set`

    Attributes:

        .. attribute:: string
        .. attribute:: start_token_index
        .. attribute:: end_token_index
        .. attribute:: start_word_index
        .. attribute:: end_word_index
        .. attribute:: sentence_index
        .. attribute:: start_leaf

           An :class:`on.corpora.tree.tree` instance.  None until enrichment

        .. attribute:: end_leaf

           An :class:`on.corpora.tree.tree` instance.  None until enrichment

        .. attribute:: subtree

           An :class:`on.corpora.tree.tree` instance.  None until
           unrichment.  After enrichment, if we could not align this
           span with any node in the tree, it remains None.

        .. attribute:: subtree_id

           After enrichment, evaluates to :attr:`subtree` ``.id``.
           This value is written to the database, and so is available
           before enrichment when one is loading from the database.

        .. attribute:: type

           The type of this named entity, such as ``PERSON`` or
           ``NORP``.

    Before enrichment, generally either the token indecies or the word
    indecies will be set but not both.  After enrichment, both sets of
    indecies will work and will delegate their responses to
    :attr:`start_leaf` or :attr:`end_leaf` as appropriate.

    """

    def __init__(self, sentence_index, document_id, type, start_index, end_index,
                 string, indexing="word", start_char_offset=0, end_char_offset=0):

        on.corpora.tree_alignable_sgml_span.__init__(self, "named_entity")

        self.sentence_index = sentence_index
        self.document_id = document_id

        if indexing == "word":
            self.start_word_index = start_index
            self.end_word_index = end_index
        elif indexing == "token":
            self.start_token_index = start_index
            self.end_token_index = end_index
        else:
            raise Exception("name_entity indexing must be either 'token' or 'word', given %s." % indexing)

        self.string = string

        self.start_char_offset = start_char_offset
        self.end_char_offset = end_char_offset

        self.type = type
        if type not in name_entity_type.allowed:
            on.common.log.report("name", "invalid name entity type", sentence=sentence_index,
                                 primary_start_index=self.primary_start_index,
                                 primary_end_index=self.primary_end_index,
                                 indexing=indexing,
                                 type=type)
            self.valid = False
        else:
            self.valid = True


    def check_tree_alignment(self):
        """ if named entity span crosses tb bracketing boundary, this is bad.  report it """

        if not self.start_leaf or not self.end_leaf:
            raise Exception("name_entity.check_tree_alignment requires enrichment to have happened already")

        if self.subtree:
            return # if we align with a subtree, we definitely have no problems

        a_tree = self.start_leaf.get_root()
        mismatch = a_tree.get_bracketing_mismatch(self.start_leaf, self.end_leaf)
        if mismatch:
            new_start, new_end = a_tree.get_closest_aligning_span(self.start_leaf, self.end_leaf, strict_alignment=False)
            if new_start and new_end and (new_start.is_trace() or new_end.is_trace()):
                # the only reason we're not aligned is that we'd need
                # to start or end with a trace, and that's prohibited
                pass


    @property
    def id(self):
        return "%s@%s:%s:%s@%s" % (self.type, self.sentence_index,
                                   self.primary_start_index, self.primary_end_index, self.document_id)

    def __repr__(self):
        a_string = "<name_entity object: id: %s" % (self.id)

        a_string += " name_entity_type: %s; sentence_id: %s; start_index: %s; end_index: %s; string: '%s';%s>" %\
                    (self.type, self.sentence_index, self.primary_start_index, self.primary_end_index, self.string,
                     " INVALID" if not self.valid else "")
        return a_string


    sql_table_name = "name_entity"
    sql_create_statement = \
"""
create table name_entity
(
  id varchar(255) not null collate utf8_bin primary key,
  type varchar(255) not null,
  document_id varchar(255) not null,
  sentence_index int not null,
  start_word_index int not null,
  end_word_index int not null,
  start_char_offset int not null,
  end_char_offset int not null,
  subtree_id varchar(255),
  string longtext,
  foreign key (document_id) references document.id,
  foreign key (subtree_id) references tree.id
)
default character set utf8;
"""


    sql_insert_statement = \
"""insert into name_entity
(
  id,
  type,
  document_id,
  sentence_index,
  start_word_index,
  end_word_index,
  start_char_offset,
  end_char_offset,
  subtree_id,
  string
) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

    def write_to_db(self, cursor):
        if self.start_word_index is None or self.end_word_index is None:
            if (self.start_token_index or self.end_token_index) and not self.start_leaf and not self.end_leaf:
                raise Exception("Cannot write token-indexed name files to db until after enrichment")
            on.common.log.report("name", "bad indexing", id=self.id, document_id=self.document_id)
            return

        try:
            cursor.executemany("%s" % (self.__class__.sql_insert_statement),
                               [(self.id, self.type, self.document_id,
                                 self.sentence_index, self.start_word_index, self.end_word_index,
                                 self.start_char_offset, self.end_char_offset,
                                 self.subtree_id, self.string)])
        except Exception:
            on.common.log.report("name", "failed to write name entity to the db",
                                 id=self.id, type=self.type, string=self.string)

class name_entity_set(object):
    """ all the name entities for a single sentence

    Contained by: :class:`name_tagged_document`

    Contains: :class:`name_entity`

    """

    def __init__(self, a_document_id):
        self.name_entity_hash = defaultdict(list)
        self.document_id = a_document_id

    def append(self, a_name_entity):
        if a_name_entity is not None:
            self.name_entity_hash[a_name_entity.type].append(a_name_entity)

    def __repr__(self):
        a_string = "------------------------------------------------------------------------\n"
        a_string = "<name_entity_set object:"

        for key in self.name_entity_hash:
            for a_name_entity in self.name_entity_hash[key]:
                a_string = "%s \n\t%s" % (a_string, a_name_entity)

        a_string = a_string + "\n>"
        a_string = a_string + "\n" + "------------------------------------------------------------------------"
        return a_string

    def __len__(self):
        return len(self.name_entity_hash)

    @property
    def sentence_index(self):
        votes = defaultdict(int)

        for a_name_entity_list in self:
            for a_name_entity in a_name_entity_list:
                votes[a_name_entity.sentence_index] += 1

        best_votes = 0
        best_index = None

        for index, num_votes in votes.iteritems():
            if num_votes > best_votes or best_index is None:
                best_index = index

        return best_index

    @property
    def id(self):

        s_i = self.sentence_index
        if s_i is None:
            s_i = "Empty_Name_Entity_Set"
        return "%s@%s" % (s_i, self.document_id)

    def __getitem__(self, index):
        return self.name_entity_hash[list(sorted(self.name_entity_hash.keys()))[index]]

class name_tagged_document:
    """
    Contained by: :class:`name_bank`

    Contains: :class:`name_entity_set`

    """

    space_close_tag_re = re.compile("\s+(</(?:ENA|TI|NU)MEX>)")
    join_start_tag_re = re.compile("(<(?:ENA|TI|NU)MEX)\s+")
    embedded_tag_re = re.compile("</(?:ENA|TI|NU)MEX></(?:ENA|TI|NU)MEX>")
    sub_token_tag_re = re.compile("(</(?:ENA|TI|NU)MEX>)([^\s<]+)") # the < added so that </ENAMEX></ENAMEX> are not considered split NEs
    l_sub_token_tag_re = re.compile("(\S+)(<(?:ENA|NU|TI)MEX-TYPE=\".*?\">)([^<]+)")
    sub_token_two_ne_tag_re = re.compile("</(?:ENA|TI|NU)MEX>([^\s]*?)<(?:ENA|TI|NU)MEX.*?>")

    start_ne_re = re.compile('<(?:ENA|TI|NU)MEX-TYPE="([^"]*)"(?:.S_OFF="(.*?)")?(?:.E_OFF="(.*?)")?>')
    end_ne_re = re.compile("</(?:ENA|TI|NU)MEX>")

    def __init__(self, document_string, document_id, extension="name", indexing="word", a_cursor=None):

        def splitter(s):
            nosplit = False
            bits = []
            cur_bit = []
            for c in s:
                if c == '<':
                    nosplit = True
                if c == '>':
                    nosplit = False

                if c in [" ", "\n", "\t"] and not nosplit:
                    bits.append("".join(cur_bit))
                    cur_bit = []
                else:
                    cur_bit.append(c)
            if cur_bit:
                bits.append("".join(cur_bit))
            return bits

        document_string, num_changes = on.common.util.desubtokenize_annotations(document_string, add_offset_notations=True)
        document_string = on.common.util.clean_up_name_string(document_string)
        document_string = document_string.replace(' E_OFF="', '-E_OFF="').replace(' S_OFF="', '-S_OFF="')


        if "@ar@" in document_id:
            document_string = on.common.util.unicode2buckwalter(document_string)

        self.debug_document_source = None

        self.document_id = document_id
        self.document_string = document_string

        self.name_entity_sets = []  #---- this will be one for each sentence
        self.extension = extension

        if(a_cursor == None):
            self.debug_document_source = "\n".join(" ".join(a_token for a_token in a_line.split())
                                                   for a_line in re.sub("<[^>]*>", "", document_string.replace("<TURN>","-TURN-")).split("\n") if a_line)

            on.common.log.debug("processing document: %s" % (self.document_id), on.common.log.DEBUG, on.common.log.MAX_VERBOSITY)

            self.document_string = self.document_string.replace(u'\ufeff', '') # delete BOM if present

            #---- now lets remove the sgml tags surrounding the document ----#
            self.document_string = on.common.util.remove_doc_tags(self.document_string)
            self.document_string = on.common.util.remove_all_non_ne_sgml_tags(self.document_string)
            self.document_string = self.document_string.strip()


            if indexing == "word" and ("*T*" in self.document_string or
                                       "*PRO*" in self.document_string or
                                       "*pro*" in self.document_string or
                                       "*OP*" in self.document_string):
                indexing = "token"
                on.common.log.report("name", "pretty sure document is actually token indexed",
                                     "indexing was word, but we found traces in the name document",
                                     document_id=document_id)

            self.document_sentences = [x.strip() for x in self.document_string.split("\n") if x.strip()]

            on.common.log.debug(self.document_sentences, on.common.log.DEBUG, on.common.log.MAX_VERBOSITY)

            #---- these were the sed scripts used to check multiple names tagged on one token ----#
            #
            #  sed 's/MEX /MEX-/g' $CORPORA/on/ne/bbn_pcet/data/WSJtypes-subtypes/* | sed -f ~/bin/return.sed | grep 'EX>[^ ]<' | more
            #  sed 's/MEX /MEX-/g' $CORPORA/on/ne/bbn_pcet/data/WSJtypes-subtypes/* | sed -f ~/bin/return.sed | sed -n '/EX>[^ ]</p' | more
            #
            #-------------------------------------------------------------------------------------#

            sentence_index = 0
            for sentence in self.document_sentences:

                a_name_entity_set = name_entity_set(document_id)

                #---- there are some inconsistencies in whether the tags are attached on either side of the text.  this will normalize it ----#
                if(len(name_tagged_document.space_close_tag_re.findall(sentence)) > 0):
                    on.common.log.debug("""
    %s
    FOUND SPACE_CLOSE_TAG[%s]: %s
    %s
    """ % ("-"*80, self.document_id, sentence, "-"*80), on.common.log.DEBUG, on.common.log.MAX_VERBOSITY)

                    sentence = name_tagged_document.space_close_tag_re.sub("\g<1> ", sentence)

                #----------------------------------------------------------------------------------------#
                # lets attach the tags to the word they tag (this only connects the tag with the first
                # word, and assumes that the last word is attached after performing normalizations
                #----------------------------------------------------------------------------------------#
                sentence = name_tagged_document.join_start_tag_re.sub("\g<1>-", sentence)

                #---- there are a few embedded named entities.  we will just consider them to be one ----#
                #
                # Their laboratory credentials established , <ENAMEX-TYPE="PERSON">Boyer</ENAMEX> and
                # <ENAMEX-TYPE="PERSON"><ENAMEX-TYPE="ORGANIZATION:CORPORATION">Swanson</ENAMEX></ENAMEX> headed for Wall
                # Street in <TIMEX-TYPE="DATE:DATE">1980</TIMEX> .
                #
                # In a statement , <ENAMEX-TYPE="PERSON">Craig O. <ENAMEX-TYPE="ORGANIZATION:CORPORATION">McCaw</ENAMEX></ENAMEX> ,
                # <ENAMEX-TYPE="PER_DESC">chairman</ENAMEX> and <ENAMEX-TYPE="PER_DESC">chief executive officer</ENAMEX> of
                # <ENAMEX-TYPE="ORGANIZATION:CORPORATION">McCaw</ENAMEX> , said : `` We trust
                # <ENAMEX-TYPE="ORGANIZATION:CORPORATION">LIN</ENAMEX> will take no further actions that favor
                # <ENAMEX-TYPE="ORGANIZATION:CORPORATION">BellSouth</ENAMEX> . ''</ENAMEX></DOC>
                #
                #----------------------------------------------------------------------------------------#
                if(len(name_tagged_document.embedded_tag_re.findall(sentence)) > 0):
                    on.common.log.debug("""
    %s
    FOUND EMBEDDED_TAG[%s]: %s
   %s
    """ % ("-"*80, self.document_id, sentence, "-"*80), on.common.log.DEBUG, on.common.log.MAX_VERBOSITY)
                    old_sentence = sentence
                    sentence= re.sub("(<(?:ENA|TI|NU)MEX-TYPE=\"[^\"]*?\">)([^/]*?)(<(?:ENA|TI|NU)MEX-TYPE=\"[^\"]*?\">)([^/]*?)(</(?:ENA|TI|NU)MEX>)(</(?:ENA|TI|NU)MEX>)", "\g<1>\g<2>\g<4>\g<6>", sentence) # i am not sure if i am really removing the same NE type, or any sub-NE type. maybe should do the former

                    on.common.log.debug("""
    %s
    FIXED -- EMBEDDED_TAG[%s]: %s
    %s
    """ % ("-"*80, self.document_id, sentence, "-"*80), on.common.log.DEBUG, on.common.log.MAX_VERBOSITY)

                    if(old_sentence == sentence):
                        on.common.log.debug("""
    DID N'T REALLY FIX ANYTHING :(
    """, on.common.log.DEBUG, on.common.log.MAX_VERBOSITY)


                if True:
                    if( len(name_tagged_document.sub_token_two_ne_tag_re.findall(sentence)) > 0):

                        on.common.log.debug("""
        %s

    FOUND SUB_TOKEN_TWO_NE_TAG[%s]: %s
        %s
        """ % ("-"*80, self.document_id, sentence, "-"*80), on.common.log.DEBUG, on.common.log.MAX_VERBOSITY)

                        sentence = name_tagged_document.sub_token_two_ne_tag_re.sub("\g<1>", sentence)


                        on.common.log.debug("""
        %s
        FIXED -- SUB_TOKEN_TWO_NE_TAG[%s]: %s
        %s
        """ % ("-"*80, self.document_id, sentence, "-"*80), on.common.log.DEBUG, on.common.log.MAX_VERBOSITY)


                    #----- check whether the sentence contains a named entity that breaks a token ----#
                    if( len(name_tagged_document.sub_token_tag_re.findall(sentence)) > 0):
                        on.common.log.debug("""
        %s
        FOUND SUB_TOKEN_TAG[%s]: %s
        %s
        """ % ("-"*80, self.document_id, sentence, "-"*80), on.common.log.DEBUG, on.common.log.MAX_VERBOSITY)

                        sentence = name_tagged_document.sub_token_tag_re.sub("\g<2>\g<1>", sentence)
                        on.common.log.debug("""
        %s
        FIXED -- FOUND SUB_TOKEN_TAG[%s]: %s
        %s
        """ % ("-"*80, self.document_id, sentence, "-"*80), on.common.log.DEBUG, on.common.log.MAX_VERBOSITY)



                    #----- check whether the sentence contains a named entity that breaks a token ----#
                    if( len(name_tagged_document.l_sub_token_tag_re.findall(sentence)) > 0):
                        on.common.log.debug("""
        %s
        FOUND SUB_TOKEN_TAG[%s]: %s
        %s
        """ % ("-"*80, self.document_id, sentence, "-"*80), on.common.log.DEBUG, on.common.log.MAX_VERBOSITY)

                        sentence = name_tagged_document.l_sub_token_tag_re.sub("\g<2>\g<1>", sentence)
                        on.common.log.debug("""
        %s
        FIXED -- FOUND SUB_TOKEN_TAG[%s]: %s
        %s
        """ % ("-"*80, self.document_id, sentence, "-"*80), on.common.log.DEBUG, on.common.log.MAX_VERBOSITY)







                on.common.log.debug("SENTENCE: %s" % (sentence), on.common.log.DEBUG, on.common.log.MAX_VERBOSITY)
                self.document_sentences[sentence_index] = sentence


                tokens = sentence.split()
                on.common.log.debug(tokens, on.common.log.DEBUG, on.common.log.MAX_VERBOSITY)

                #------------------------------------------------------------------------------------------#
                # the following block assumes that there are no longer any embedded named entities
                # remaining in the document
                #------------------------------------------------------------------------------------------#



                add_to_db_flag = False
                is_fac = False

                ne_start_token_index = -1
                ne_end_token_index = -1
                for token_index, token in enumerate(tokens):
                    on.common.log.debug(token, on.common.log.DEBUG, on.common.log.MAX_VERBOSITY)
                    match = name_tagged_document.start_ne_re.findall(token)

                    ne_start_match = 0

                    if(len(match) > 0):
                        ne_start_match = ne_start_match + 1
                        ne_start_token_index = token_index

                        ne_type, s_off, e_off = match[0]
                        s_off = int(s_off) if s_off else 0
                        e_off = int(e_off) if e_off else 0

                        #---remove the sub-type and do other cleaning---#
                        ne_type = re.sub(":.*", "", ne_type)

                        for see, use in [ ["WORK-OF-ART", "WORK_OF_ART"],
                                          ["LOCATION", "LOC"],
                                          ["ORGANIZATION", "ORG"],
                                          ["PRODCUT", "PRODUCT"],
                                          ["FACILITY", "FAC"]]:
                            if ne_type == see:
                                ne_type = use


                        #---- if contains _DESC don't add it to the database ---#
                        if(len(re.findall("_DESC", ne_type)) > 0):
                            on.common.log.debug("%s: found _DESC" % (ne_type), on.common.log.DEBUG, on.common.log.MAX_VERBOSITY)
                        else:
                            on.common.log.debug("%s" % (ne_type), on.common.log.DEBUG, on.common.log.MAX_VERBOSITY)
                            add_to_db_flag = True


                        #---- if of type FAC_ mark as such ---#
                        if(len(re.findall("FAC_", ne_type)) > 0):
                            on.common.log.debug("%s: found FAC_" % (ne_type), on.common.log.DEBUG, on.common.log.MAX_VERBOSITY)
                            is_fac = True
                        else:
                            on.common.log.debug("%s" % (ne_type), on.common.log.DEBUG, on.common.log.MAX_VERBOSITY)


                    if(len(name_tagged_document.end_ne_re.findall(token)) > 0):

                        ne_end_token_index = token_index

                        #---- this means that we know the information on this named entity ----#
                        a_name_entity_string = " ".join(tokens[ne_start_token_index:ne_end_token_index+1])
                        a_name_entity_string = on.common.util.remove_sgml_tags(a_name_entity_string)

                        #---- only part of the names will be added to the database, unless explicitly specified ----#
                        if(add_to_db_flag == True):

                            #---- add it only if it is a fac and NO_FAC is not true ----#
                            if(is_fac == True):
                                pass
                            else:
                                if ne_start_token_index == -1 or ne_end_token_index == -1:
                                    raise Exception("bad indexing: ne_start_token_index=%s, ne_end_token_index=%s, token_index=%s" % (
                                        ne_start_token_index, ne_end_token_index, token_index))

                                a_name_entity = name_entity(sentence_index, self.document_id, ne_type, ne_start_token_index,
                                                            ne_end_token_index, a_name_entity_string.strip(), indexing=indexing,
                                                            start_char_offset=s_off, end_char_offset=e_off)

                                a_name_entity_set.append(a_name_entity)

                            # reset the flag
                            add_to_db_flag = False

                        ne_start_token_index = -1
                        ne_end_token_index = -1

                self.name_entity_sets.append(a_name_entity_set)
                on.common.log.debug(a_name_entity_set, on.common.log.DEBUG, on.common.log.MAX_VERBOSITY)

                sentence_index += 1
        else:
            pass


    def __getitem__(self, index):
        return self.name_entity_sets[index]

    def __len__(self):
        return len(self.name_entity_sets)

    def __repr__(self):
        return "name_tagged_document instance, id=%s,name_entity_sets:\n%s" % (
            self.document_id, on.common.util.repr_helper(enumerate(a_name_entity_set.id for a_name_entity_set in self)))

    def write_to_db(self, cursor):
        for a_name_entity_set in self.name_entity_sets:
            for a_name_entity_list in a_name_entity_set.name_entity_hash.itervalues():
                for a_name_entity in a_name_entity_list:
                    if a_name_entity.valid:
                        a_name_entity.write_to_db(cursor)
                    else:
                        on.common.log.report("name", "dropping invalid name entity", id=a_name_entity.id)


    def dump_view(self, a_cursor=None, out_dir=""):

        a_string = '<DOC DOCNO="%s">\n' % (self.document_id)

        if self.tree_document:
            sentence_tokens_list = list(self.tree_document.sentence_tokens_as_lists(make_sgml_safe=True, strip_traces=True))

        elif a_cursor:
            a_cursor.execute("""select * from sentence where document_id = '%s';""" % (self.document_id))
            sentence_rows = a_cursor.fetchall()

            sentence_tokens_list = []
            for sentence_row in sentence_rows:
                a_sentence_string = sentence_row["no_trace_string"]
                a_sentence_string = on.common.util.make_sgml_safe(a_sentence_string)
                sentence_tokens_list.append(a_sentence_string.split())

        else:
            raise Exception("Name documents can be dumped only if they have either been enriched or loaded from the database")


        if a_cursor:
            a_cursor.execute("""select * from name_entity where document_id = '%s';""" % (self.document_id))
            name_entity_rows = a_cursor.fetchall()
        else:
            name_entity_rows = [ dict(id=a_name_entity.id,
                                      type=a_name_entity.type,
                                      sentence_index=a_name_entity.sentence_index,
                                      start_word_index=a_name_entity.start_word_index,
                                      end_word_index=a_name_entity.end_word_index,
                                      start_char_offset=a_name_entity.start_char_offset,
                                      end_char_offset=a_name_entity.end_char_offset,
                                      document_id=self.document_id)

                                 for a_name_entity_set in self
                                 for a_name_entity_list in a_name_entity_set
                                 for a_name_entity in a_name_entity_list
                                 if a_name_entity.valid]

        for ner in name_entity_rows:
            try:
                if None in [ner["sentence_index"], ner["start_word_index"], ner["end_word_index"]]:
                    raise Exception("Inconsistent data -- name id = %s (%s)" % (ner["id"], ner) )

                si = int(ner["sentence_index"])
                s_ti = int(ner["start_word_index"])
                e_ti = int(ner["end_word_index"])

                offsets = ""
                if ner["start_char_offset"] != 0:
                    offsets += ' S_OFF="%s"' % ner["start_char_offset"]
                if ner["end_char_offset"] != 0:
                    offsets += ' E_OFF="%s"' % ner["end_char_offset"]

                sentence_tokens_list[si][s_ti] = '<ENAMEX TYPE="%s"%s>%s' % (ner["type"], offsets, sentence_tokens_list[si][s_ti])
                sentence_tokens_list[si][e_ti] = "%s</ENAMEX>" % (sentence_tokens_list[si][e_ti])

            except IndexError:
                on.common.log.report("name", "alignment issue with name entity; not writing", **ner)
                continue

        for sentence_tokens in sentence_tokens_list:
            a_string = "%s%s\n" % (a_string, " ".join(sentence_tokens))

        a_string = "%s</DOC>\n" % (a_string)



        #---- write view file -----#
        with codecs.open(on.common.util.output_file_name(self.document_id, self.extension, out_dir), "w", "utf-8") as f:
            f.write(a_string)


class name_bank(abstract_bank):
    """ Contains :class:`name_tagged_document` """

    def __init__(self, a_subcorpus, tag, a_cursor=None, extension="name", indexing="word"):
        abstract_bank.__init__(self, a_subcorpus, tag, extension)

        if(a_cursor == None):
            sys.stderr.write("reading the name bank [%s]..." % self.extension)

            for a_file in self.subcorpus.get_files(self.extension):
                sys.stderr.write(".")

                name_file = codecs.open(a_file.physical_filename, "r", "utf-8")
                try:
                    name_tagged_document_string = name_file.read()
                finally:
                    name_file.close()

                name_tagged_document_id = "%s@%s" % (a_file.document_id, self.subcorpus.id)

                a_name_tagged_document = name_tagged_document(name_tagged_document_string, name_tagged_document_id,
                                                              self.extension, indexing=indexing)
                self.append(a_name_tagged_document)
            sys.stderr.write("\n")
        else:
            pass




    def enrich_treebank(self, a_treebank, a_cursor=None):
        total_ne_node_mismatches = 0
        total_nes = 0

        total_ne_non_terminals = 0
        total_ne_terminals = 0

        abstract_bank.enrich_treebank(self, a_treebank)

        #--------------------------------------------------------------------------------#
        # now that we have initialized the names, we can go through them
        # one by one, and tag the nodes in the tree with those names
        #--------------------------------------------------------------------------------#

        #---- for each document in the list of name tagged documents ----#
        for a_name_tagged_document in self:
            sys.stderr.write(".")

            a_name_entity_sets = a_name_tagged_document.name_entity_sets
            a_tree_document = a_name_tagged_document.tree_document

            if len(a_name_entity_sets) > len(a_tree_document):
                on.common.log.report("name", " found a mismatch in number of elements in the lists SERIOUS",
                                     nes=len(a_name_entity_sets), tids=len(a_tree_document),
                                     sets=a_name_entity_sets)
                for a_name_entity_set in a_name_entity_sets:
                    for a_name_entity_list in a_name_entity_set:
                        for a_name_entity in a_name_entity_list:
                            a_name_entity.valid = False
                continue


            while len(a_name_entity_sets) < len(a_tree_document):
                a_name_tagged_document.name_entity_sets.append(name_entity_set(a_tree_document.document_id))



#             for a_tree, a_document_sentence in zip(a_tree_document, a_name_tagged_document.document_sentences):
#                 print "-===============------------================-"
#                 print a_tree.get_word_string
#                 print
#                 print
#                 print
#                 print a_document_sentence



            #---- for each sentence in the document ----#
            for sentence_no, (a_tree, a_name_entity_set) in enumerate(zip(a_tree_document, a_name_entity_sets)):
                #---- for each name type tagged in the sentence ----#
                for a_name_entity_type in a_name_entity_set.name_entity_hash:
                    #---- for each name instance in that type in the sentence ----#
                    for a_name_entity in a_name_entity_set.name_entity_hash[a_name_entity_type]:
                        #---- try to get a legal node that aligns with this name ----#
                        a_subtree_id = None

                        old_str = a_name_entity.string
                        old_swi = a_name_entity.start_word_index
                        old_ewi = a_name_entity.end_word_index
                        old_sti = a_name_entity.start_token_index
                        old_eti = a_name_entity.end_token_index

                        assert a_tree.get_sentence_index() == a_name_entity.sentence_index, (a_tree, a_name_entity)

                        a_name_entity.enrich_tree(a_tree)

                        if not a_name_entity.start_leaf or not a_name_entity.end_leaf:
                            continue


                        a_name_entity.check_tree_alignment()

                        a_subtree = a_name_entity.subtree
                        if a_subtree:
                            total_nes += 1

                            if a_subtree.is_leaf():
                                total_ne_terminals += 1
                            else:
                                total_ne_non_terminals += 1

                        #---- if there is no legal tree node aligning with the name ----#
                        else:
                            total_ne_node_mismatches += 1

                        new_str = a_name_entity.string

        #--------------------------------------------------------------------------------#
        # now that we have traversed all the names in the name bank,
        # we will show the summary statistics of how many of them had
        # nodes in the tree aligning with them, etc.
        #--------------------------------------------------------------------------------#
        sys.stderr.write("\n")

        if(on.common.log.DEBUG == True and on.common.log.VERBOSITY >= on.common.log.MAX_VERBOSITY):
            sys.stderr.write("total nes: " + str(total_nes) + "\n")
            sys.stderr.write("total ne-node mismatches: " + str(total_ne_node_mismatches) + "\n")
            sys.stderr.write("total ne-terminals: " + str(total_ne_terminals) + "\n")
            sys.stderr.write("total ne-non-terminals: " + str(total_ne_non_terminals) + "\n")

        #for a_name_tagged_document in self:
        #    for a_name_entity_set in a_name_tagged_document:
        #        for a_name_entity_list in a_name_entity_set.name_entity_hash.values():
        #            for a_name_entity in a_name_entity_list:
        #                print "ne", a_name_entity.id, a_name_entity.valid

        #for a_tree_document in a_treebank:
        #    for a_tree in a_tree_document:
        #        for a_leaf in a_tree:
        #            for a_name_entity in a_leaf.start_named_entity_list:
        #                print a_name_entity.id, a_name_entity.valid


    sql_table_name = "name_bank"
    sql_exists_table = "name_entity" # if a document has entries here then it has been name annotated

    ## @var SQL create statement for the syntactic_link table
    #
    sql_create_statement = \
"""
create table name_bank
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
insert into name_bank
(
  id,
  subcorpus_id,
  tag
) values(%s, %s, %s)
"""



    @classmethod
    def from_db(cls, a_subcorpus, tag, a_cursor, affixes=None):
        #---- create an empty proposition bank ----#
        sys.stderr.write("reading the name bank ...")
        a_name_bank = name_bank(a_subcorpus, tag, a_cursor)

        #---- now get document ids for this name_bank ----#
        a_cursor.execute("""select document.id from document where subcorpus_id = '%s';""" % (a_subcorpus.id))
        document_rows = a_cursor.fetchall()

        #---- and process each document ----#
        for document_row in document_rows:
            a_document_id = document_row["id"]

            if not on.common.util.matches_an_affix(a_document_id, affixes):
                continue

            sys.stderr.write(".")

            a_name_tagged_document = name_tagged_document("", a_document_id, a_name_bank.extension, a_cursor=a_cursor)

            a_cursor.execute("""select max(sentence_index) from name_entity where document_id = '%s' """ % a_document_id)
            max_rows = a_cursor.fetchall()

            assert len(max_rows) == 1
            max_rows = max_rows[0]

            max_sentence_index = -1 if max_rows["max(sentence_index)"] is None else int(max_rows["max(sentence_index)"])

            for sentence_index in range(max_sentence_index + 1):
                a_name_entity_set = name_entity_set(a_document_id)
                a_cursor.execute("""select * from name_entity where document_id = '%s' and sentence_index = %s """ % (a_document_id, sentence_index))
                for name_entity_row in a_cursor.fetchall():
                    a_name_entity = name_entity(sentence_index=name_entity_row["sentence_index"],
                                                document_id=name_entity_row["document_id"],
                                                type=name_entity_row["type"],
                                                start_index=name_entity_row["start_word_index"],
                                                end_index=name_entity_row["end_word_index"],
                                                string=name_entity_row["string"],
                                                indexing="word",
                                                start_char_offset=name_entity_row["start_char_offset"],
                                                end_char_offset=name_entity_row["end_char_offset"])
                    a_name_entity.subtree_id = name_entity_row["subtree_id"]
                    a_name_entity_set.append(a_name_entity)
                a_name_tagged_document.name_entity_sets.append(a_name_entity_set)
            a_name_bank.append(a_name_tagged_document)

        sys.stderr.write("\n")
        return a_name_bank

