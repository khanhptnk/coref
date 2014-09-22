
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
:mod:`coreference` -- Coreferential Entity Annotation
-------------------------------------------------------

See:

 - :class:`on.corpora.coreference.coreference_link`
 - :class:`on.corpora.coreference.coreference_bank`

Coreference annotation consists of indicating which mentions in a
text refer to the same entity.  The ``.coref`` file format looks
like this:

.. code-block xml::

  <DOC DOCNO="bn/cnn/00/cnn_0001@all@cnn@bn@en@on">
  <TEXT PARTNO="000">
  ...
  <COREF ID="000-8" TYPE="IDENT">Kevin Dunn</COREF> has the latest .
  ...
  <COREF ID="000-8" TYPE="IDENT">Kevin Dunn</COREF> , ITN .
  </TEXT>
  </DOC>



Correspondences:

 ===============================  ======================================================  =============================================================================
 **Database Tables**              **Python Objects**                                      **File Elements**
 ===============================  ======================================================  =============================================================================
 ``coreference_bank``             :class:`coreference_bank`                               All ``.coref`` files in an :class:`on.corpora.subcorpus`
 None                             :class:`coreference_document`                           A ``.coref`` file (a ``DOC`` span)
 ``tree.coreference_section``     :attr:`on.corpora.tree.tree.coref_section`              An annotation section of a ``.coref`` file (a ``TEXT`` span)
 ``tree``                         :class:`on.corpora.tree.tree`                           A line in a ``.coref`` file
 ``coreference_chain``            :class:`coreference_chain`                              All ``COREF`` spans with a given ``ID``
 ``coreference_chain.type``       :attr:`coreference_chain.type`                          The ``TYPE`` field of a coreference link (the same for all links in a chain)
 ``coreference_chain.speaker``    :attr:`coreference_chain.speaker`                       The ``TYPE`` field of a coreference chain (the same for all links in a chain)
 ``coreference_link``             :class:`coreference_link`                               A single ``COREF`` span
 ``coreference_link.type``        :attr:`coreference_link.type`                           The ``SUBTYPE`` field of a coreference link
 ===============================  ======================================================  =============================================================================

Note that coreference section information is stored very differently
the files than in the database and python objects.  For more details
see the :attr:`on.corpora.tree.tree.coref_section` documentation

Classes:

.. autoclass:: coreference_bank
.. autoclass:: coreference_document
.. autoclass:: coreference_chain
.. autoclass:: coreference_link

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

from on.common.log import status

import on.corpora
import on.corpora.tree

from collections import defaultdict
from on.common.util import insert_ignoring_dups
from on.corpora import abstract_bank


class coreference_link_type:

    allowed = ["HEAD", "ATTRIB", "IDENT"] # NP not allowed
    references = [["coreference_link", "type"]]

    sql_table_name = "coreference_link_type"

    sql_create_statement = \
"""
create table coreference_link_type
(
  id varchar(255) not null collate utf8_bin primary key
)
default character set utf8;
"""


    sql_insert_statement = \
"""insert into coreference_link_type
(
  id
) values (%s)
"""



class coreference_link(on.corpora.tree_alignable_sgml_span):
    """ A coreference annotation

    Contained by: :class:`coreference_chain`

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
           enrichment.  After enrichment, if we could not align this
           span with any node in the tree, it remains None.

        .. attribute:: subtree_id

           After enrichment, evaluates to :attr:`subtree` ``.id``.
           This value is written to the database, and so is available
           before enrichment when one is loading from the database.

        .. attribute:: type

           All coreference chains with type ``IDENT`` have coreference
           links with type ``IDENT``.  If the coreference chain has
           type ``APPOS`` (appositive) then one coreference link will
           be the ``HEAD`` while the other links will be ``ATTRIB``.

        .. attribute:: coreference_chain

           What :class:`on.corpora.coreference.coreference_chain`
           contains this link.

        .. attribute:: start_char_offset

           In the case of a token like 'Japan-China' we want to be
           able to tag 'Japan' and 'China' separately.  We do this by
           specifying a character offset from the beginning and end to
           describe how much of the token span we care about.  So in
           this case to tag only 'China' we would set
           start_char_offset to 6.  To tag only 'Japan' we would set
           end_char_offset to '6'.  If these offsets are 0, then, we
           use whole tokens.

           These correspond to the 'S_OFF' and 'E_OFF' attributes in
           the coref files.

           For the most complex cases, something like 'Hong
           Kong-Zhuhai-Macau', we specify both the start and the end
           offsets.  The coref structure looks like::

             <COREF>Hong <COREF><COREF>Kong-Zhuhai-Macau</COREF></COREF></COREF>

           And the offsets are E_OFF=13 for Hong Kong, S_OFF=5 and
           E_OFF=6 for 'Zhuhai', and S_OFF=12 for 'Macau'

        .. attribute:: end_char_offset

           See :attr:`coreference_link.start_char_offset`

    Before enrichment, generally either the token indices or the word
    indices will be set but not both.  After enrichment, both sets of
    indices will work and will delegate their responses to start_leaf
    or end_leaf as appropriate.

    """

    def __init__(self, type, coreference_chain, a_cursor=None,
                 start_char_offset=0, end_char_offset=0, precise=False):
        on.corpora.tree_alignable_sgml_span.__init__(self, "coreference_link")

        self.type = type

        self.coreference_chain = coreference_chain

        self.start_char_offset = start_char_offset
        self.end_char_offset = end_char_offset

        self.precise=precise

        self.valid = True

    @property
    def coreference_chain_id(self):
        return self.coreference_chain.id

    @property
    def id(self):
        return "%s@%s:%s:%s@%s" % (self.type, self.sentence_index,
                                   self.primary_start_index, self.primary_end_index,
                                   self.coreference_chain_id)

    def get_char_range(self):
        """ return tuples that let one compare start and end indices
        for different corefs.  If two corefs define the same span,
        they should have identical char ranges.  If one overlaps the
        other, they should have overlapping ranges """

        #
        # The '100' here is a hack.  It should be the length of the
        # token at self.end_token_index.  But that's not available
        # here.  This should work in all cases we currently use this
        # function for, but it's a little worrying.
        #
        # What this means is that the overlaps function is not fully
        # accurate at the character level.
        #
        return (self.start_token_index, self.start_char_offset), (self.end_token_index, 100 - self.end_char_offset)

    def overlaps(self, other_link):
        """ warning: not fully accurate at the character level ; see get_char_range """

        if int(self.sentence_index) != other_link.sentence_index:
            return False

        A_start, A_end = self.get_char_range()
        B_start, B_end = other_link.get_char_range()

        return not (A_end < B_start or B_end < A_start)


    def __repr__(self):
        return "<coreference_link object: id: %s; type: %s --- '%s'>"\
               % (self.id, self.type, self.string)


    sql_table_name = "coreference_link"

    sql_create_statement = \
"""
create table coreference_link
(
  id varchar(255) not null collate utf8_bin primary key,
  type varchar(255) not null,
  coreference_chain_id varchar(255) not null,
  sentence_index int not null,
  start_token_index int not null,
  end_token_index int not null,
  start_char_offset int not null,
  end_char_offset int not null,
  subtree_id varchar(255),
  string longtext,
  foreign key (type) references coreference_link_type.id,
  foreign key (coreference_chain_id) references coreference_chain.id,
  foreign key (subtree_id) references subtree.id
)
default character set utf8;
"""


    sql_insert_statement = \
"""insert into coreference_link
(
  id,
  type,
  coreference_chain_id,
  sentence_index,
  start_token_index,
  end_token_index,
  start_char_offset,
  end_char_offset,
  subtree_id,
  string
) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

    def write_to_db(self, cursor):

        data = [(self.id,
                 self.type,
                 self.coreference_chain_id,
                 self.sentence_index,
                 self.start_token_index,
                 self.end_token_index,
                 self.start_char_offset,
                 self.end_char_offset,
                 self.subtree_id,
                 self.string)]

        #---- insert the value in the table ----#

        try:
            cursor.executemany("%s" % (self.__class__.sql_insert_statement), data)
        except MySQLdb.Error, e:
            on.common.log.report("coreference", "error writing coreference link to database",
                                 link=self, error=e )



class coreference_chain_type:

    allowed = ["APPOS", "IDENT"]
    references = [["coreference_chain", "type"]]

    sql_table_name = "coreference_chain_type"

    sql_create_statement = \
"""
create table coreference_chain_type
(
  id varchar(255) not null collate utf8_bin primary key
)
default character set utf8;
"""


    sql_insert_statement = \
"""insert into coreference_chain_type
(
  id
) values (%s)
"""


class coreference_chain(object):
    """

    Contained by: :class:`coreference_document`

    Contains: :class:`coreference_link`

    Attributes:

      .. attribute:: identifier

         Which coref chain this is.  This value is unique to this
         document, though not across documents.

      .. attribute:: type

         Whether we represent an ``APPOS`` reference or an ``IDENT`` one.

      .. attribute:: section

         Which section of the coreference document we belong in.  See
         :attr:`on.corpora.tree.tree.coref_section` for more details.

      .. attribute:: document_id

         The id of the document that we belong to

      .. attribute:: coreference_links

         A list of :class:`coreference_link` instances.  Better to use
         ``[]`` or iteration on the chain than to use this list
         directly, though.

      .. attribute:: speaker

         A string or the empty string.  For coref chains that are
         coreferent with one of the speakers in the document, this
         will be set to the speaker's name.  To see which speakers are
         responsible for which sentences, either use the ``.speaker``
         file or look at the :attr:`on.corpora.tree.speaker_sentence`
         attribute of trees.  During the coreference annotation
         process the human annotators had access to the name of the
         speaker for each line.

         Note that the speaker attribute does not represent the person
         who spoke this sentence.

    """

    def __init__(self, type, identifier, section, document_id, a_cursor=None, speaker=""):
        self.coreference_links = []
        self.identifier = identifier

        if section is None or section == "None":
            section = "000"

        self.section = section
        self.document_id = document_id
        self.type = type

        if speaker is None:
            speaker = ""
        self.speaker = speaker

        self.valid = True


    @property
    def id(self):
        return "@".join(str(s) for s in [self.type, self.identifier, self.section, self.document_id])

    def __repr__(self):
        return "coreference chain instance, id=%s, links:" % (self.id) + "\n" + on.common.util.repr_helper(enumerate(link.id for link in self))

    def __getitem__(self, index):
        return self.coreference_links[index]

    def __delitem__(self, index):
        del self.coreference_links[index]

    def __len__(self):
        return len(self.coreference_links)

    sql_table_name = "coreference_chain"

    sql_create_statement = \
"""
create table coreference_chain
(
  id varchar(255) not null collate utf8_bin primary key,
  number varchar(128) not null,
  section varchar(16),
  document_id varchar(255) not null,
  type varchar(16) not null,
  speaker varchar(256) not null,
  foreign key (document_id) references document.id,
  foreign key (speaker) references speaker.name
)
default character set utf8;
"""


    sql_insert_statement = \
"""insert into coreference_chain
(
  id,
  number,
  section,
  document_id,
  type,
  speaker
) values (%s, %s, %s, %s, %s, %s)
"""

    def write_to_db(self, cursor):

        cursor.executemany("%s" % (self.__class__.sql_insert_statement), [
            (self.id, self.identifier, self.section, self.document_id, self.type, self.speaker)])


        #---- write the links to the database ----#
        for a_coreference_link in self:
            if a_coreference_link.valid:
                a_coreference_link.write_to_db(cursor)



class coreference_document:
    """
    Contained by: :class:`coreference_bank`

    Contains: :class:`coreference_chain`

    """

    def __init__(self, enc_doc_string, document_id, extension="coref", indexing="token", a_cursor=None,
                 adjudicated=True, messy_muc_input=False):



        enc_doc_string = enc_doc_string.replace(u'\ufeff', "")

        self.extension = extension
        self.coreference_chain_hash = {}
        self.doc_no = None
        self.date = ""
        self.document_id = document_id
        self.num_tokens_list = []  #---- This list stores the number of tokens in each sentence of the document
        self.sentence_tokens_list = []

        self.adjudicated = adjudicated

        self.tree_document = None # until enrichment

        self.section_names = None
        """ we only ever store section divisions when loaded from
        files.  This is because we need to store the information
        somewhere until we enrich the treebank.  Once we've one that,
        the information is in the trees.  So read it from a tree's
        coref_section field."""

        if indexing not in ["token", "word"]:
            raise Exception("Coreference document may only be token or word indexed, not %s." % indexing)

        if(a_cursor == None):

            #---- lets define some regular expressions here for the time being, then relocate them somewhere better ----#
            doc_no_re = re.compile(r'<DOC DOC(?:NO|ID)="(.*?)">')
            part_no_re = re.compile(r'<TEXT PART(?:NO|ID)="(.*?)">')
            sgml_tag_re = re.compile(r"<[^T].*?>", re.UNICODE)
            coref_chain_properties_re = re.compile(r'<COREF.ID="(.*?)".TYPE="(.*?)"(?:.SUBTYPE="(.*?)")?(?:.S_OFF="(.*?)")?(?:.E_OFF="(.*?)")?(?:.SPEAKER="(.*?)")?(?:.PRECISE="(.*?)")?>')
            date_re = re.compile(r"<DATE>(.*?)</DATE>")

            id_re = re.compile(r"\s+ID=")
            type_re = re.compile(r"\s+TYPE=")
            speaker_re = re.compile(r"\s+SPEAKER=")
            precise_re = re.compile(r"\s+PRECISE=")
            subtype_re = re.compile(r"\s+SUBTYPE=")
            s_off_re = re.compile(r"\s+S_OFF=")
            e_off_re = re.compile(r"\s+E_OFF=")

            header_re = re.compile(r"<[/]?HEADER>")
            body_re = re.compile(r"<[/]?BODY>")


            #---- retrieve the DOCNO, DATE ----#

            if messy_muc_input:
                for f, r in [["<DOC>", '<DOC DOCNO="unknown">']]:
                    enc_doc_string = enc_doc_string.replace(f,r)

                for t in "TEXT IN HL AUTHOR SO CO p s P S DATELINE PREAMBLE NWORDS DATE SLUG TRAILER G GV DD AN COMMENT".split():
                    enc_doc_string = enc_doc_string.replace("<%s>"%t, " ")
                    enc_doc_string = enc_doc_string.replace("</%s>"%t, " ")

                enc_doc_string = re.sub("<SLUG[^>]*>", " ", enc_doc_string)

                try:
                    enc_doc_string, _ = on.common.util.desubtokenize_annotations(enc_doc_string, add_offset_notations=True)
                except Exception:
                    print enc_doc_string
                    raise

                #print enc_doc_string

            self.doc_no = None
            list = doc_no_re.findall(enc_doc_string)
            if(len(list) > 0):
                self.doc_no = list[0].strip()

            self.date = None
            list = date_re.findall(enc_doc_string)
            if(len(list) > 0):
                self.date = list[0].strip()

            #---- sanity check ----#
            if( not self.doc_no or re.sub(".mrg", "", self.doc_no) != self.document_id.split("@")[0].split("/")[-1] ):
                #print >>sys.stderr, self.doc_no, self.document_id.split("@")[0].split("/")[-1]
                #print >>sys.stderr, self.doc_no, self.document_id
                on.common.log.warning("doc_no (%s) does not match with document_id (%s)" % (self.doc_no, self.document_id), on.common.log.MAX_VERBOSITY)

            enc_doc_string = "\n".join([x.strip() for x in enc_doc_string.split("\n") if x.strip()])

            #---- remove the DOC, DATE, </TEXT>, HEADER, BODY tags ----#
            enc_doc_string = doc_no_re.sub("", enc_doc_string).strip()

            if not messy_muc_input:
                enc_doc_string = date_re.sub("", enc_doc_string).strip()

            enc_doc_string = header_re.sub("", enc_doc_string).strip()
            enc_doc_string = body_re.sub("", enc_doc_string).strip()
            enc_doc_string = enc_doc_string.replace("</DOC>","").strip()

            # keeping <TEXT PARTNO="foo"> so we know what part to
            # label things as.

            #---- add space around the SGML tags ----#
            enc_doc_string = sgml_tag_re.sub(" \g<0> ", enc_doc_string).strip()

            self.coref_lines = enc_doc_string.split("\n")

            # find out which lines belong in which coref section and
            # delete coref section tags (TEXT)

            coref_sections = [] # [[start, end, name]]
            new_lines = []
            offset = 0 # correct for removing lines
            c_section = None
            c_section_start_line = None
            for idx, line in enumerate(self.coref_lines):
                nl = line
                if c_section and '</TEXT>' in nl:
                    nl = line.replace("</TEXT>", "").strip()

                    coref_sections.append([c_section_start_line, idx - offset - 1, c_section])
                    c_section = None
                    c_section_start_line = None

                if not c_section and '<TEXT PARTNO="' in nl:
                    c_section = nl.split('<TEXT PARTNO="')[1].split('"')[0]
                    nl = line.replace('<TEXT PARTNO="%s">' % c_section, "").strip()

                    c_section_start_line = idx - offset

                # as we're replacing tags with blank we might have killed some lines.
                # If so, we need to correct for this with an offset.  Later we subtract
                # this from the index and that tells us our true line number.
                if nl:
                    new_lines.append(nl)
                else:
                    offset += 1

            self.coref_lines = new_lines


            def get_coref_section_info(sentence_index):
                found = ()
                for start, end, name in coref_sections:
                    if start <= sentence_index <= end:
                        if not found:
                            found = (start, end, name)
                        else:
                            on.common.log.report("coreference", "Overlapping coref sections",
                                                 "document_id: %s\nsection A: (%d,%d,%s)\nsection B:(%d,%d,%s)" % (
                                (document_id, found[0], found[1], found[2], start, end, name)))

                if not found:
                    return (0, 0, None)
                return found

            # check if every line is in exactly one coref_section
            for idx, line in enumerate(self.coref_lines):
                if not get_coref_section_info(idx)[2]:
                    t_line = line
                    if len(t_line) > 30:
                        t_line = t_line[:30] + "..."
                    on.common.log.report("coreference", "Line not in coref section",
                                         "document_id: %s\nline: %d\ntext: %s" % (
                        document_id, idx, t_line))


            # check if coref sections' names match up:
            c_s_names = []
            for start, end, name in coref_sections:
                if "-" in name:
                    firstname, secondname = name.split("-")
                    c_s_names.append(int(firstname))
                    c_s_names.append(int(secondname))
                else:
                    c_s_names.append(int(name))

            c_s_names.sort()
            n_prev = None

            problem = False
            for n in c_s_names:
                if n_prev and n != n_prev + 1:
                    on.common.log.report("coreference", "Missing coref section SERIOUS",
                                         "document_id: %s\nbetween: %d and %d" % (
                                         document_id, n_prev, n))
                    problem = True
                n_prev = n

            if problem:
                return

            self.section_names = coref_sections
            self.num_sentences = len(self.coref_lines)

            #--------------------------------------------------------------------------------#
            # at this point the coreference_file_lines consist only of the sentences that
            # we care about and so the length of this array is the number of
            # sentences in this file
            #
            # We also know that at this point every line in the document belongs in a coref
            # section and are pretty sure that when we try to match this up with trees later
            # we have all the sentences.  We will check to be sure later.
            #--------------------------------------------------------------------------------#

            on.common.log.debug("processing document: %s" % (document_id), on.common.log.DEBUG, on.common.log.MAX_VERBOSITY)

            sentence_index=0
            for sentence_index in range(0, len(self.coref_lines)):

                self.coref_lines[sentence_index] = self.coref_lines[sentence_index].strip()

                opens = re.findall("<COREF", self.coref_lines[sentence_index])
                closes = re.findall("</COREF",  self.coref_lines[sentence_index])
                skip_line = False
                if len(opens) != len(closes):
                    skip_line = True

                #on.common.log.status("%s / %s" % (sentence_index,  len(self.coref_lines)))

                #---- make the SGML tags one non-space token ----#
                for a_re, a_replacement in [[id_re, "-ID="],
                                            [type_re, "-TYPE="],
                                            [speaker_re, "-SPEAKER="],
                                            [precise_re, "-PRECISE="],
                                            [subtype_re, "-SUBTYPE="],
                                            [s_off_re, "-S_OFF="],
                                            [e_off_re, "-E_OFF="]]:
                    self.coref_lines[sentence_index] = a_re.sub(a_replacement, self.coref_lines[sentence_index])

                self.coref_lines[sentence_index] = " ".join(self.coref_lines[sentence_index].split())

                a_plain_tokens_list =  sgml_tag_re.sub("", self.coref_lines[sentence_index]).split()

                self.num_tokens_list.append(len(a_plain_tokens_list))

                self.sentence_tokens_list.append(a_plain_tokens_list)

                token_index = 0
                token_list  = []

                coref_link_stack = []       #---- this keeps the token numbers at which coref started
                accepted_ids = set()
                for item in self.coref_lines[sentence_index].split():

                    if( item.startswith("<COREF") and item[-1] == ">" ):

                        if skip_line:
                             continue

                        cc_id, coref_chain_type, coref_link_type, cc_soff, cc_eoff, speaker, precise = coref_chain_properties_re.findall(item)[0]

                        # we use tmp_coref_chain so we can get the id
                        # for the coref chain to look up.  If that
                        # coref chain doesn't exist yet, then save
                        # tmp_coref chain as the first mention of it

                        tmp_coref_chain = coreference_chain(coref_chain_type, cc_id, get_coref_section_info(sentence_index)[2],
                                                            self.document_id, speaker=speaker)
                        if tmp_coref_chain.id not in self.coreference_chain_hash:
                            self.coreference_chain_hash[tmp_coref_chain.id] = tmp_coref_chain
                        coref_chain = self.coreference_chain_hash[tmp_coref_chain.id]

                        #---- now that we have dealt with the chain, lets create and add the coref link to the stack  ----#
                        if not coref_link_type:
                            coref_link_type = "IDENT"          #---- since there is no SUBTYPE, it is IDENT itself

                        if coref_link_type == "ATTRIBUTE":
                            coref_link_type = "ATTRIB"

                        coref_link = coreference_link(coref_link_type, coref_chain,
                                                      start_char_offset=int(cc_soff) if cc_soff else 0,
                                                      end_char_offset=int(cc_eoff) if cc_eoff else 0,
                                                      precise=bool(precise))

                        #---- the link still does not have a end index
                        if indexing == "token":
                            coref_link.start_token_index = token_index
                        elif indexing == "word":
                            coref_link.start_word_index = token_index

                        #---- and add it to the stack ----#
                        coref_link_stack.append(coref_link)


                    elif item.startswith("</COREF>") and item.endswith(">"):

                        if skip_line:
                            continue

                        #---- the coref chain id that this end tag belongs to should be on the top of the stack, lets get it ---#
                        try:
                            coref_link = coref_link_stack.pop()
                        except IndexError:
                            on.common.log.report("coreference", "too many close corefs",
                                                 document_id=document_id, sentence_index=sentence_index)
                            return

                        if indexing == "token":
                            start_index = coref_link.start_token_index
                            coref_link.end_token_index = token_index-1
                        elif indexing == "word":
                            start_index = coref_link.start_word_index
                            coref_link.end_word_index = token_index-1

                        coref_link.sentence_index = sentence_index
                        coref_link.string = " ".join(a_plain_tokens_list[start_index:token_index])

                        #---- now, add the fully specified link object to the chain ----#
                        coref_link.coreference_chain.coreference_links.append(coref_link)

                        accepted_ids.add(coref_link.coreference_chain.id)

                    else:
                        token_list.append(item)
                        token_index += 1

                assert not coref_link_stack


        else:
            pass


    def __getitem__(self, index):
        # because we only have a hash table but need to be consistent in our ordering, we must sort
        return self.coreference_chain_hash[list(sorted(self.coreference_chain_hash.keys()))[index]]

    def __len__(self):
        return len(self.coreference_chain_hash)


    def __repr__(self):
        return "coreference_document, id=%s, coreference_chains:\n%s" % (
            self.document_id, on.common.util.repr_helper(enumerate(a_coreference_chain.id for a_coreference_chain in self)))


    #---- this function modifies the coreference_chain_hash inline ----#
    def filter_overlapping_sub_links(self):
        on.common.log.debug("filtering coreference links in %s" % (self.document_id), on.common.log.DEBUG, on.common.log.MAX_VERBOSITY)

        on.common.log.debug("the number of coreference links BEFORE filtering: %d" %
                            (len(self.coreference_chain_hash)),
                            on.common.log.DEBUG, on.common.log.MAX_VERBOSITY)
        #---- for each coreference chain in the list of chains in this document ----#
        for coreference_chain_id in self.coreference_chain_hash.keys():
            coref_chain = self.coreference_chain_hash[coreference_chain_id]

            #---- lets print the chain before filtering ----#
            on.common.log.debug("-----------------------------------------------------", on.common.log.DEBUG, on.common.log.MAX_VERBOSITY)
            on.common.log.debug("the coref chain before filtering", on.common.log.DEBUG, on.common.log.MAX_VERBOSITY)

            for coreference_link in coref_chain:
                on.common.log.debug(coreference_link, on.common.log.DEBUG, on.common.log.MAX_VERBOSITY)
            on.common.log.debug(".....................................................", on.common.log.DEBUG, on.common.log.MAX_VERBOSITY)

            #---- lets process the links in the chain now ----#
            if len(coref_chain) > 1:
                on.common.log.debug("found a chain of length > 1", on.common.log.DEBUG, on.common.log.MAX_VERBOSITY)

                OVERLAP = True
                while OVERLAP:
                    OVERLAP = False

                    i=0
                    for i in range(0, len(coref_chain) - 1):
                        if( coref_chain[i].overlaps(coref_chain[i+1]) ):

                            OVERLAP = True
                            on.common.log.debug("found a overlapping pair in a > 1 link chain.  deleting the shorter link", on.common.log.DEBUG, on.common.log.MAX_VERBOSITY)

                            a = int(coref_chain[i].primary_end_index)
                            b = int(coref_chain[i].primary_start_index)
                            c = int(coref_chain[i+1].primary_end_index)
                            d = int(coref_chain[i+1].primary_start_index)

                            del coref_chain[i if (a-b) < (c-d) else i+1]
                            break

            #---- this separate "if" statement takes care of initially lone links, or lone links formed after deletion of overlapping links ----#
            if ( len(coref_chain) == 1):
                on.common.log.debug("the coreference chain should not contain only one link, deleting it.", on.common.log.DEBUG, on.common.log.MAX_VERBOSITY)
                del self.coreference_chain_hash[coreference_chain_id]


            #---- lets print the chain after filtering ----#
            on.common.log.debug("the coref chain after filtering", on.common.log.DEBUG, on.common.log.MAX_VERBOSITY)
            try:
                for a_coreference_link in self.coreference_chain_hash[coreference_chain_id]:
                    on.common.log.debug(a_coreference_link, on.common.log.DEBUG, on.common.log.MAX_VERBOSITY)
            except KeyError:
                on.common.log.debug("NULL", on.common.log.DEBUG, on.common.log.MAX_VERBOSITY)
            on.common.log.debug("-----------------------------------------------------", on.common.log.DEBUG, on.common.log.MAX_VERBOSITY)
        on.common.log.debug("the number of coreference links AFTER filtering: %d" % (len(self.coreference_chain_hash)), on.common.log.DEBUG, on.common.log.MAX_VERBOSITY)


    def write_to_db(self, cursor):
        for a_coreference_chain in self.coreference_chain_hash.itervalues():
            if a_coreference_chain.valid:
                a_coreference_chain.write_to_db(cursor)


    def build_section_names(self, a_cursor):
        # sn is in the format [(start, end, strname), ...]
        sn = []

        if a_cursor:
            a_cursor.execute("""select coref_section,id from tree where document_id = '%s' order by abs(id) asc;""" % self.document_id)
            tree_coref_sections = [tree_row["coref_section"] for tree_row in a_cursor.fetchall()]
        elif self.tree_document:
            tree_coref_sections = [a_tree.coref_section for a_tree in self.tree_document]
        else:
            return self.section_names

        last_id = -2
        start_line = 0
        for idx, coref_section in enumerate(tree_coref_sections):
            if coref_section != last_id:

                if last_id != -2:
                    sn.append([start_line, idx, str(len(sn))])

                last_id = coref_section
                start_line = idx

        if last_id != -2:
            sn.append([start_line, idx, str(len(sn))])

        return sn


    def dump_view(self, a_cursor=None, out_dir="", only_ident=False, muc_format=False):
        # muc_format forces only_ident

        docno = self.document_id if a_cursor else self.document_id.split("@")[0].split("/")[-1]

        if muc_format:
            coref_doc_string = "<DOC>\n<DOCNO> unknown </DOCNO>\n<TEXT>\n"
            section_names = None
            only_ident=True
        else:
            coref_doc_string = '<DOC DOCNO="%s">\n' % docno
            section_names = self.build_section_names(a_cursor)

        if(a_cursor != None):
            assert not muc_format
            a_cursor.execute("""select string from sentence where document_id = '%s' order by sentence_index ASC;""" % (self.document_id))

            sentence_tokens_list = [ on.common.util.make_sgml_safe(sentence_row["string"]).split()
                                     for sentence_row in a_cursor.fetchall()]
        elif self.tree_document:
            sentence_tokens_list = list(self.tree_document.sentence_tokens_as_lists(make_sgml_safe=True,
                                                                                    strip_traces=muc_format))
        else:
            raise Exception("Coreference documents may be dumped only from the database or after enrichment")


        def coref_link_sorter( (link_a, id_a), (link_b, id_b) ):

            # all smallest first
            #   sort first by sentence
            #   then by start token index
            #   then by end token index

            for compare in ['sentence_index', 'start_token_index', 'end_token_index']:
                a_compare = int(getattr(link_a, compare))
                b_compare = int(getattr(link_b, compare))
                d = cmp(a_compare, b_compare)
                if d != 0:
                    return d
            return 0

        coref_links_and_ids = [(cl, cc_id)
                               for (cc_id, cc) in self.coreference_chain_hash.iteritems()
                               for cl in cc]

        coref_links_and_ids.sort(coref_link_sorter)

        if muc_format:
            identifier_to_id_mapping = {}
            next_id = 1

        for coref_link, coref_chain_id in coref_links_and_ids:
            coref_chain = self.coreference_chain_hash[coref_chain_id]


            if not coref_link.valid or not coref_chain.valid:
                continue

            if only_ident and coref_chain.type == "APPOS":
                on.common.log.status("  > skipping APPOS")
                continue

            literal_chain_id = coref_chain_id.split("@")[1]
            literal_section_id = coref_chain_id.split("@")[2]

            add_section = literal_section_id.zfill(3)
            if literal_chain_id.startswith(add_section + "-"):
                coref_chain_id = literal_chain_id
            else:
                coref_chain_id = '-'.join([add_section, literal_chain_id])


            sentence_index = int(coref_link.sentence_index)

            if muc_format:
                start_token_index = int(coref_link.start_word_index)
                end_token_index = int(coref_link.end_word_index)
            else:
                start_token_index = int(coref_link.start_token_index)
                end_token_index = int(coref_link.end_token_index)

            if muc_format:
                identifier = next_id
                next_id += 1
            else:
                identifier = coref_chain.identifier

            start_coref='<COREF ID="%s"' % identifier
            start_coref += ' TYPE="%s"' % coref_chain.type
            if muc_format:
                if coref_chain.identifier in identifier_to_id_mapping:
                    start_coref += ' REF="%s"' % (identifier_to_id_mapping[coref_chain.identifier])
                identifier_to_id_mapping[coref_chain.identifier] = identifier
            else:

                if coref_chain.type != "IDENT":
                    start_coref += ' SUBTYPE="%s"' % coref_link.type

                if coref_link.start_char_offset != 0:
                    start_coref += ' S_OFF="%s"' % coref_link.start_char_offset

                if coref_link.end_char_offset != 0:
                    start_coref += ' E_OFF="%s"' % coref_link.end_char_offset

                if coref_chain.speaker:
                    start_coref += ' SPEAKER="%s"' % coref_chain.speaker.replace('"', "''")

                if coref_link.precise:
                    start_coref += ' PRECISE="True"'

            try:
                old_start = sentence_tokens_list[sentence_index][start_token_index]
                old_end = sentence_tokens_list[sentence_index][end_token_index] # just to test the indexing
            except IndexError:
                on.common.log.report("coreference", "can't write out coref tag; indexing issue",
                                     start=start_token_index, end=end_token_index, sentence=sentence_index, doc=docno)
                continue

            sentence_tokens_list[sentence_index][start_token_index] = "%s>%s" % (start_coref, old_start)
            sentence_tokens_list[sentence_index][end_token_index] = "%s</COREF>" % (sentence_tokens_list[sentence_index][end_token_index])


        cur_section_end = -1

        for idx, sentence_tokens in enumerate(sentence_tokens_list):
            if section_names and idx >= cur_section_end:
                for start, end, name in section_names:
                    if idx < end or (start == end and idx == end):
                        text = "</TEXT>\n" if cur_section_end != -1 else ""
                        coref_doc_string = '%s%s<TEXT PARTNO="%s">\n' \
                                           % (coref_doc_string, text, str(name).zfill(3))
                        cur_section_end = end
                        break
            coref_doc_string = "%s%s\n" % (coref_doc_string, " ".join(sentence_tokens))


        if cur_section_end != -1:
            coref_doc_string = "%s</TEXT>\n" % coref_doc_string

        if muc_format:
            coref_doc_string = "%s</TEXT>\n" % (coref_doc_string)

        coref_doc_string = "%s</DOC>\n" % (coref_doc_string)


        if a_cursor or out_dir:
            #---- write view file -----#
            with codecs.open(on.common.util.output_file_name(self.document_id, self.extension, out_dir),
                             "w", "utf-8") as f:
                f.write(coref_doc_string)
        else:
            return coref_doc_string

    def set_sections(self):
        document_id = self.tree_document.document_id
        if not hasattr(self, 'section_names') or not self.section_names:
            # not a problem : loaded from db

            return

        if len(self.tree_document) != self.section_names[-1][1] + 1:
            on.common.log.report("coreference", "line number mismatch",
                                 td=len(self.tree_document),
                                 sn=(self.section_names[-1][1] + 1),
                                 document_id=document_id)
        else:
            for idx, a_tree in enumerate(self.tree_document):
                for start, end, name in self.section_names:
                    if start <= idx <= end:
                        a_tree.coref_section = int(name.split("-")[0])
                if a_tree.coref_section == -1:
                    on.common.log.report("coreference", "failed to find coref section",
                                         document_id=document_id)

    def split_parts(self, a_tree_document, remove_traces=True, remove_code=True):
        assert len(a_tree_document) == len(self.coref_lines), "the number of trees in the tree document should match with the number of sentences in the coreference document"
        assert remove_traces == True, "remove_traces=False is not yet implemented"
        assert remove_code == True, "remove_code=False is not yet implemented"

        def is_open_coref_tag(a_token):
            if(a_token.count("<COREF") > 0):
                return True
            else:
                return False

        def is_close_coref_tag(a_token):
            if(a_token.count("</COREF") > 0):
                return True
            else:
                return False

        filtered_coreference_sgml_tokens_hash = {}
        filtered_leaves_hash = {}
        total_lines_ignored = 0
        i=0
        for i in range(0, len(self.coref_lines)):
            on.common.log.debug("self.coref_lines[i]: %s" % (self.coref_lines[i]), on.common.log.DEBUG, on.common.log.MIN_VERBOSITY)
            on.common.log.debug("a_tree_document[i]: %s" % (a_tree_document[i]), on.common.log.DEBUG, on.common.log.MIN_VERBOSITY)
            on.common.log.debug("a_tree_document[i].coref_section: %s" % (a_tree_document[i].coref_section), on.common.log.DEBUG, on.common.log.MIN_VERBOSITY)

            coreference_sgml_tokens_list = self.coref_lines[i].split()
            a_leaves = list(a_tree_document[i].leaves())

            a_coreference_sgml_string = ""
            a_word_string = ""

            j=0
            k=0
            while( j < len(coreference_sgml_tokens_list)):

                a_coreference_token = coreference_sgml_tokens_list[j]
                if(k < len(a_leaves)):
                    a_leaf = a_leaves[k]

                if(is_open_coref_tag(a_coreference_token)
                   or
                   is_close_coref_tag(a_coreference_token)):

                    a_coreference_sgml_string = a_coreference_sgml_string + " " + a_coreference_token
                else:
                    if(k < len(a_leaves)
                       and
                       a_leaf.tag != "CODE"
                       and
                       a_leaf.trace_type == None):


                        a_leaf.word = re.sub("/([.?-])", "\g<1>", a_leaf.word)

                        on.common.log.debug("a_coreference_token: %s" % (a_coreference_token), on.common.log.DEBUG, on.common.log.MIN_VERBOSITY)
                        on.common.log.debug("a_leaf.word: %s" % (a_leaf.word), on.common.log.DEBUG, on.common.log.MIN_VERBOSITY)

                        try:
                            assert a_coreference_token == a_leaf.word
                        except Exception:
                            a_coreference_sgml_string = ""
                            a_word_string = ""
                            total_lines_ignored = total_lines_ignored + 1
                            break



                        a_coreference_sgml_string = a_coreference_sgml_string + " " + a_coreference_token
                        a_word_string = a_word_string + " " + a_leaf.word

                    k=k+1
                j=j+1

            # remove empty links
            a_coreference_sgml_string = re.sub("<COREF-ID=\"[^\"]+\"-TYPE=\"[A-Z]+\">\s+</COREF>", "", a_coreference_sgml_string)
            a_coreference_sgml_string = re.sub("~([A-Z]+)", "\g<1>",  a_coreference_sgml_string)
            a_coreference_sgml_string = a_coreference_sgml_string.strip()

            if(not filtered_coreference_sgml_tokens_hash.has_key(a_tree_document[i].coref_section)):
                filtered_coreference_sgml_tokens_hash[a_tree_document[i].coref_section] = []

            filtered_coreference_sgml_tokens_hash[a_tree_document[i].coref_section].append(a_coreference_sgml_string)

            # strip the spaces around the word string
            a_word_string = a_word_string.strip()
            a_word_string = re.sub("~([A-Z]+)", "\g<1>",  a_word_string)

            if(not filtered_leaves_hash.has_key(a_tree_document[i].coref_section)):
                filtered_leaves_hash[a_tree_document[i].coref_section] = []

            filtered_leaves_hash[a_tree_document[i].coref_section].append(a_word_string)

        on.common.log.debug("len(filtered_coreference_sgml_tokens_hash): %s" % (len(filtered_coreference_sgml_tokens_hash)), on.common.log.DEBUG, on.common.log.MIN_VERBOSITY)
        on.common.log.debug("len(filtered_leaves_hash): %s" % (len(filtered_leaves_hash)), on.common.log.DEBUG, on.common.log.MIN_VERBOSITY)

        assert len(filtered_coreference_sgml_tokens_hash) == len(filtered_leaves_hash)

        a_sections = filtered_coreference_sgml_tokens_hash.keys()
        a_sections.sort()
        on.common.log.debug("a_sections: %s" % (a_sections), on.common.log.DEBUG, on.common.log.MIN_VERBOSITY)

        coreference_lines = []
        text_lines = []


        for a_section in a_sections:
            on.common.log.debug("\n".join(filtered_coreference_sgml_tokens_hash[a_section]), on.common.log.DEBUG, on.common.log.MIN_VERBOSITY)
            coreference_lines.append("\n".join(filtered_coreference_sgml_tokens_hash[a_section]))
            on.common.log.debug("-"*80, on.common.log.DEBUG, on.common.log.MIN_VERBOSITY)
            on.common.log.debug("\n".join(filtered_leaves_hash[a_section]), on.common.log.DEBUG, on.common.log.MIN_VERBOSITY)
            text_lines.append("\n".join(filtered_leaves_hash[a_section]))
            on.common.log.debug("="*80, on.common.log.DEBUG, on.common.log.MIN_VERBOSITY)

        on.common.log.debug("total_lines_ignored: %s" % (total_lines_ignored), on.common.log.DEBUG, on.common.log.MIN_VERBOSITY)

        return coreference_lines, text_lines, total_lines_ignored

class coreference_bank(abstract_bank):
    """ Contains: :class:`coreference_document` """

    def __init__(self, a_subcorpus, tag, a_cursor=None, extension="coref", indexing="token", messy_muc_input=False):
        abstract_bank.__init__(self, a_subcorpus, tag, extension)

        if(a_cursor == None):
            sys.stderr.write("reading the coreference bank [%s] ..." % self.extension)

            single_annotated_documents = set()

            sing_ann_fname = "%s/metadata/single_annotated_coref_files.txt" % self.subcorpus.top_dir
            if os.path.exists(sing_ann_fname):
                with open(sing_ann_fname) as inf:
                    for a_file_id in inf:
                        sing_language, sing_genre, sing_docid = a_file_id.strip().split()
                        assert sing_language[:2] == self.subcorpus.language_id
                        single_annotated_documents.add((sing_genre, sing_docid))

            for a_file in self.subcorpus.get_files(self.extension):
                sys.stderr.write(".")

                coreference_file = codecs.open(a_file.physical_filename, "r", "utf-8")
                on.common.log.debug(a_file.physical_filename, on.common.log.DEBUG, on.common.log.MAX_VERBOSITY)
                try:
                    coreference_document_string = coreference_file.read()
                finally:
                    coreference_file.close()

                coreference_document_id = "%s@%s" % (a_file.document_id, a_subcorpus.id)

                adjudicated = not any((("@%s@" % sing_genre) in coreference_document_id and
                                       ("/%s@" % sing_docid) in coreference_document_id)
                                      for sing_genre, sing_docid in single_annotated_documents)

                a_coreference_document = coreference_document(coreference_document_string, coreference_document_id,
                                                              indexing=indexing, adjudicated=adjudicated,
                                                              messy_muc_input=messy_muc_input)

                self.append(a_coreference_document)

            sys.stderr.write("\n")
        else:
            pass


    #--------------------------------------------------------------------------------#
    # this function takes a treebank, and aligns the links with the
    # nodes in the tree, and assigns coreference chain ids to the tree
    # nodes as well
    #--------------------------------------------------------------------------------#
    def enrich_treebank(self, a_treebank, a_cursor=None):
        abstract_bank.enrich_treebank(self, a_treebank)

        for a_coreference_document in self:

            a_tree_document = a_coreference_document.tree_document

            if not a_cursor:
                a_coreference_document.set_sections()

            if not a_coreference_document.sentence_tokens_list:
                a_coreference_document.sentence_tokens_list = list(a_tree_document.sentence_tokens_as_lists(make_sgml_safe=True))

            if len(a_coreference_document.sentence_tokens_list) != len(a_tree_document):
                for i, line in enumerate(a_coreference_document.sentence_tokens_list):
                    print "C", i, " ".join(line)[:25]
                for i, a_tree in enumerate(a_tree_document):
                    print "P", i, a_tree.get_word_string()[:25]

                on.common.log.report("coreference", "bad coref document SERIOUS",
                                     coref_sentences=len(a_coreference_document.sentence_tokens_list),
                                     parse_sentences=len(a_tree_document),
                                     coref_0=a_coreference_document.sentence_tokens_list[0],
                                     coref_1=a_coreference_document.sentence_tokens_list[1],
                                     coref_2=a_coreference_document.sentence_tokens_list[2],
                                     coref_m1=a_coreference_document.sentence_tokens_list[-1],
                                     coref_m2=a_coreference_document.sentence_tokens_list[-2],
                                     coref_m3=a_coreference_document.sentence_tokens_list[-3] )

                return

            mismatch_flag = False

            for a_index, a_tree in enumerate(a_tree_document):


                #if "-RRB-" in a_tree.get_word_string():
                #    print len(a_tree.get_word_string().split())

                a_tree_id = a_tree.id

                num_leaves = len(list(a_tree.leaves()))

                coref_list = a_coreference_document.sentence_tokens_list[a_index]
                parse_list = [l.word for l in a_tree.leaves()]

                if coref_list != parse_list:
                    if len(coref_list) == len(parse_list):

                        def meaningful_difference(parse_token, coref_token):

                            if parse_token.startswith("<") and \
                               parse_token.endswith(">") and \
                               coref_token.startswith("[") and \
                               coref_token.endswith("]") and \
                               parse_token.count(":") == 3:
                                return False

                            for punc in list(".-?"):
                                if parse_token == ("/" + punc) and coref_token == punc:
                                    return False

                            if coref_token == "-TURN-" and parse_token == "<TURN>":
                                return False

                            if parse_token == on.common.util.make_sgml_unsafe(coref_token):
                                return False

                            for r, s in [("<", "["), (">", "]"), ("-LCB-", "{"),
                                         ("-RCB-", "}"), ("-RRB-", ")"), ("-LRB-", "(")]:
                                parse_token = parse_token.replace(r, s)

                            return parse_token != coref_token

                        issues = ["tb:%s c:%s" % (a,b) for a, b
                                  in zip(parse_list, coref_list)
                                  if meaningful_difference(a,b)]

                        if issues:
                            on.common.log.report("coreference", "COREF/TREE TOKEN MISMATCH",
                                                 tree_id=a_tree_id,
                                                 coref=" ".join(coref_list),
                                                 parse=" ".join(parse_list),
                                                 issues=" ;".join(issues))
                    else:
                        a_coref_sstring = "|".join(coref_list)
                        sys.stderr.write("\ncoref: %s (%d)\n" % (
                            a_coref_sstring, len(coref_list)))

                        a_parse_sstring = "|".join(parse_list)

                        sys.stderr.write("parse: %s (%d) \n" % (
                            a_parse_sstring, len(parse_list)))

                        sys.stderr.write("\n\n")

                        sys.stderr.write("%s%s%s\n" % (
                            "Index".ljust(8),
                            "Coref".ljust(20),
                            "Parse"))

                        for i in range(max(len(coref_list), len(parse_list))):
                            try:
                                c = coref_list[i]
                            except IndexError:
                                c = "None"

                            try:
                                p = parse_list[i]
                            except IndexError:
                                p = "None"

                            sys.stderr.write("%s%s%s\n" % (
                                str(i).ljust(8),
                                c.ljust(20), p))


                        on.common.log.error("%s: found token mismatch" % (a_tree_id), False)
                        on.common.log.report("coreference",
                                             "TOKEN MISMATCH BETWEEN COREFERENCE AND TREE DOCUMENT",
                                             tree_id=a_tree_id, leaves=num_leaves,
                                             coref=a_coref_sstring,
                                             parse=a_parse_sstring)

                        mismatch_flag = True
                        on.common.log.error("change this to not continue", False)
                        continue

                #---- the code will not come here when there is a terminating error in the block above, and is useful when not, so just keeping it as it is ----#
                if mismatch_flag:
                    mismatch_flag = False
                    continue

            a_coreference_document.sentence_tokens_list = list(a_tree_document.sentence_tokens_as_lists(make_sgml_safe=True))


            for a_coreference_chain_id, a_coreference_chain in a_coreference_document.coreference_chain_hash.iteritems():
                info = [["document_id", a_coreference_document.document_id],
                        ["tree_document_length", len(a_tree_document)]]

                if len(a_coreference_chain) == 0:
                    a_coreference_chain.valid = False
                    on.common.log.report("coreference_minor", "ignoring chain of length 0",
                                         id=a_coreference_chain.id)
                    continue

                if len(a_coreference_chain) == 1 and not a_coreference_chain.speaker:
                    pass
                    #a_coreference_chain.valid = False
                    #continue

                for a_coreference_link in a_coreference_chain:

                    def complain(reason):
                        a_coreference_link.valid = False

                    try:
                        a_tree = a_tree_document[a_coreference_link.sentence_index]
                    except Exception:
                        complain("sentence index out of range")
                        continue

                    info.append(["tree", a_tree.pretty_print()])
                    #print a_coreference_chain
                    a_coreference_link.enrich_tree(a_tree)

                    if not a_coreference_link.start_leaf or not a_coreference_link.end_leaf:
                        complain("couldn't align start/end with trees")
                        continue
                    assert a_coreference_link.start_leaf.get_token_index() <= a_coreference_link.end_leaf.get_token_index()

                    a_subtree_id = a_coreference_link.subtree_id

            reported_links = []
            for a_tree in a_tree_document:
                for a_leaf in a_tree:
                    for a_coreference_link in a_leaf.start_coreference_link_list:
                        #assert a_coreference_link.coreference_chain in a_coreference_document
                        for b_coreference_link in a_leaf.start_coreference_link_list:
                            if (a_coreference_link not in reported_links and
                                b_coreference_link not in reported_links and
                                a_coreference_link is not b_coreference_link and
                                a_coreference_link.get_char_range() == b_coreference_link.get_char_range()):

                                if a_coreference_link.coreference_chain is not b_coreference_link.coreference_chain:
                                    has_hyphen = "hyph" if re.match(".*[a-z]-[a-zA-Z].*", a_coreference_link.string) else "nohyph"

                                    sentence = a_tree.get_word_string()
                                    chain_links_A = "\n".join(a_coref_link.string for a_coref_link in a_coreference_link.coreference_chain)
                                    chain_links_B = "\n".join(a_coref_link.string for a_coref_link in b_coreference_link.coreference_chain)
                                    span = a_coreference_link.string
                                    if a_tree.language == "ar":
                                        sentence = on.common.util.buckwalter2unicode(sentence)
                                        chain_links_A = on.common.util.buckwalter2unicode(chain_links_A)
                                        chain_links_B = on.common.util.buckwalter2unicode(chain_links_B)
                                        span = on.common.util.buckwalter2unicode(span)
                                else:
                                    if a_coreference_link.valid and b_coreference_link.valid:
                                        b_coreference_link.valid = False
                                        on.common.log.report("coreference", "dropping duplicate coref link",
                                                             id=a_coreference_link.coreference_chain.identifier,
                                                             document=a_tree_document.document_id,
                                                             part=a_tree.coref_section)

                                reported_links.append(a_coreference_link)
                                reported_links.append(b_coreference_link)


                        def report_chain(report_name, type):
                            chain_links = "\n".join(a_coref_link.string for a_coref_link in a_coreference_link.coreference_chain)
                            sentence =  a_tree.get_word_string()
                            span = a_coreference_link.string

                            if a_tree.language == "ar":
                                chain_links = on.common.util.buckwalter2unicode(chain_links)
                                sentence = on.common.util.buckwalter2unicode(sentence)
                                span = on.common.util.buckwalter2unicode(span)


                        if a_coreference_link.type not in coreference_link_type.allowed:
                            report_chain("invalid coreference link type", a_coreference_link.type)
                            #a_coreference_link.valid = False

                        if a_coreference_link.coreference_chain.type not in coreference_chain_type.allowed:# and a_coreference_link.coreference_chain.vaild:
                            report_chain("invalid coreference chain type", a_coreference_link.coreference_chain.type)
                            #a_coreference_link.coreference_chain.valid = False

        sys.stderr.write("\n")


    sql_table_name = "coreference_bank"
    sql_exists_table = "coreference_chain" # a table with both id and document id that is empty if there is no coref annotation

    ## @var SQL create statement for the syntactic_link table
    #
    sql_create_statement = \
"""
create table coreference_bank
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
insert into coreference_bank
(
  id,
  subcorpus_id,
  tag
) values(%s, %s, %s)
"""




    @classmethod
    def from_db(cls, a_subcorpus, tag, a_cursor, affixes=None):
        #---- create an empty proposition bank ----#
        sys.stderr.write("reading the coreference bank ...")
        a_coreference_bank = coreference_bank(a_subcorpus, tag, a_cursor)

        #---- now get document ids for this coreference_bank ----#
        a_cursor.execute("""select document.id from document where subcorpus_id = '%s';""" % (a_subcorpus.id))
        document_rows = a_cursor.fetchall()

        #---- and process each document ----#
        for document_row in document_rows:
            a_document_id = document_row["id"]

            if not on.common.util.matches_an_affix(a_document_id, affixes):
                continue

            sys.stderr.write(".")

            a_coreference_document = coreference_document("", a_document_id, a_coreference_bank.extension, a_cursor=a_cursor)

            a_cursor.execute("""select * from coreference_chain where document_id ='%s';""" % (a_document_id))
            coreference_chain_rows = a_cursor.fetchall()

            for coreference_chain_row in coreference_chain_rows:
                a_coreference_chain = coreference_chain(coreference_chain_row["type"], coreference_chain_row["number"],
                                                        coreference_chain_row["section"], a_document_id,
                                                        a_cursor=a_cursor,
                                                        speaker=coreference_chain_row["speaker"])
                assert coreference_chain_row["id"] == a_coreference_chain.id


                a_cursor.execute("""select * from coreference_link where coreference_chain_id = '%s';""" % (a_coreference_chain.id))
                coreference_link_rows = a_cursor.fetchall()

                for coreference_link_row in coreference_link_rows:
                    a_coreference_link = coreference_link(coreference_link_row["type"], a_coreference_chain, a_cursor,
                                                          start_char_offset=coreference_link_row["start_char_offset"],
                                                          end_char_offset=coreference_link_row["end_char_offset"])
                    for attr in ["sentence_index", "subtree_id", "start_token_index", "end_token_index",
                                 "string"]:
                        setattr(a_coreference_link, attr, coreference_link_row[attr])
                    assert a_coreference_link.id == coreference_link_row["id"]
                    assert a_coreference_link.start_token_index == int(coreference_link_row["start_token_index"])
                    assert a_coreference_link.end_token_index == int(coreference_link_row["end_token_index"])
                    a_coreference_chain.coreference_links.append(a_coreference_link)


                a_coreference_document.coreference_chain_hash[a_coreference_chain.id] = a_coreference_chain

            a_coreference_bank.append(a_coreference_document)

        sys.stderr.write("\n")
        return a_coreference_bank

