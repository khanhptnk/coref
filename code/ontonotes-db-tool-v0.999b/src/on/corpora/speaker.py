

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

"""
----------------------------------------------------------------------------------
:mod:`speaker` -- Speaker Metadata for Broadcast Conversation Documents
----------------------------------------------------------------------------------

See:

 - :class:`speaker_sentence`
 - :class:`speaker_bank`

Speaker metadata is additional information collected at the sentence
level about speakers before annotation.  The data is stored in
``.speaker`` files:

.. code-block:: bash

   $ head data/english/annotations/bc/cnn/00/cnn_0000.speaker
   0.0584225900682 12.399083739    speaker1        male    native
   0.0584225900682 12.399083739    speaker1        male    native
   0.0584225900682 12.399083739    speaker1        male    native
   0.0584225900682 12.399083739    speaker1        male    native
   0.0584225900682 12.399083739    speaker1        male    native
   12.3271665044   21.6321665044   paula_zahn      female  native
   12.3271665044   21.6321665044   paula_zahn      female  native
   12.3271665044   21.6321665044   paula_zahn      female  native
   12.3271665044   21.6321665044   paula_zahn      female  native
   12.3271665044   27.7053583252   paula_zahn      female  native

There is one ``.speaker`` line for each tree in the document, so above
is the speaker metadata for the first 10 trees in cnn_0000.  The
columns are ``start_time``, ``stop_time``, ``name``, ``gender``, and
``competency``.  These values are available in attributes of
:class:`speaker_sentence` with those names.

You might notice that the start and stop times don't make sense.  How
can speaker1 say five things where each begins at time 0.05 and ends
at time 12.4?  When speakers said a group of parsable statements in
quick succession, start and stop times were usually only recorded for
the group.  I'm going to refer to these groups as 'annotation
groups'.  An annotation group is roughly analogous to a sentence (by
which I mean a single tree); it represents a sequence of words that
the annotator doing the transcription grouped together.

Another place this is confusing is with paula_zahn's final sentence.
It has the same start time as her previous four sentences, but a
different end time.  This is because that tree contains words from two
different annotation groups.  When this happens, the ``.speaker`` line
will use the start_time of the initial group and the end_time of the
final group.  When this happens with the other columns (speakers
completing each other's sentences) we list all values separated by
commas, but this is rare.  One example would be tree 41 in english bc
msnbc document 0006 where George Bush completes one of Andrea
Mitchel's sentences.  With 'CODE' statements added to make it clear
where the breaks between speakers go, the tree looks like:

.. code-block:: scheme

   ( (NP (CODE <176.038501264:182.072501264:Andrea_Mitchel:42>)
         (NP (NNS Insights) (CC and) (NN analysis))
         (PP (IN from)
             (NP (NP (NP (NNP Bill) (NNP Bennett))
                     (NP (NP (NN radio) (NN host))
                         (CC and)
                         (NP (NP (NN author))
                             (PP (IN of)
                                 (NP-TTL (NP (NNP America))
                                         (NP (DT The) (NNP Last) (NNP Best) (NNP Hope)))))))
                 (CODE <182.072501264:185.713501264:Andrea_Mitchel:43>)
                 (NP (NP (NNP John) (NNP Harwood))
                     (PP (IN of)
                         (NP (NP (DT The)
                                 (NML (NNP Wall) (NNP Street))
                                 (NNP Journal))
                             (CC and)
                             (NP (NNP CNBC)))))
                 (CODE <185.713501264:188.098501264:Andrea_Mitchel:44>)
                 (NP (NP (NNP Dana) (NNP Priest))
                     (PP (IN of)
                         (NP (DT The) (NNP Washington) (NNP Post))))
                 (CODE <188.098501264:190.355501264:George_W_Bush:45>)
                 (CC And)
                 (NP (NP (NNP William) (NNP Safire))
                     (PP (IN of)
                         (NP (DT The)
                             (NML (NNP New) (NNP York))
                             (NNP Times))))))
         (. /.)))

This gives a speaker file that looks like:

.. code-block:: bash

   $ cat data/english/annotations/bc/msnbc/00/msnbc_0006.speaker
   ...
   160.816917439   163.569917439   Andrea_Mitchel  female  native
   163.569917439   173.243917439   George_W_Bush   male    native
   173.243917439   176.038501264   Andrea_Mitchel  female  native
   176.038501264   190.355501264   Andrea_Mitchel,Andrea_Mitchel,Andrea_Mitchel,George_W_Bush      female,female,female,male       native
   194.102780118   204.535780118   George_W_Bush   male    native
   204.535780118   212.240780118   George_W_Bush   male    native
   ...

Note that the information about when in the statement George Bush took
over for Andrea Mitchel is not retained.

This happens 14 times in the english bc data and not at all in the chinese.



Correspondences:

 ===========================  ======================================================  ===========================================================
 **Database Tables**          **Python Objects**                                      **File Elements**
 ===========================  ======================================================  ===========================================================
 None                         :class:`speaker_bank`                                   All ``.speaker`` files in an :class:`on.corpora.subcorpus`
 None                         :class:`speaker_document`                               A ``.speaker`` file
 ``speaker_sentence``         :class:`speaker_sentence`                               A line in a ``.speaker`` file
 ===========================  ======================================================  ===========================================================

.. autoclass:: speaker_bank
.. autoclass:: speaker_document
.. autoclass:: speaker_sentence

"""

# author: sameer pradhan

from __future__ import with_statement
import sys
import codecs

import on
import on.common.log
import on.common.util
import on.corpora
import on.corpora.tree
import on.corpora.proposition
import on.corpora.coreference
import on.corpora.name
import on.corpora.sense

from on.corpora import abstract_bank
from collections import defaultdict



class speaker_sentence(object):
    """
    Contained by: :class:`speaker_document`

    Attributes:

        .. attribute:: start_time

          What time this utterance or series of utterances began.  If some
          speaker says three things in quick succession, we may have
          parsed these as three separate trees but timing information
          could have only been recorded for the three as a block.

        .. attribute:: stop_time

          What time this utterance or series of utterances ended.  The
          same caveat as with :attr:`start_time` applies.

        .. attribute:: name

          The name of the speaker.  This might be something like
          'speaker_1' if the data was not entered.

        .. attribute:: gender

          The gender of the speaker.  Generally 'male' or 'female'.

        .. attribute:: competence

          The competency of the speaker in the language.  Generally 'native'.

    """

    def __init__(self, line_number, document_id, start_time, stop_time, name, gender, competence):
        self.tree = None # until enrichment

        self.line_number = line_number
        self.document_id = document_id
        self.start_time = start_time
        self.stop_time = stop_time
        self.name = name
        self.gender = gender
        self.competence = competence


    @property
    def id(self):
        return "%s@%s" % (self.line_number, self.document_id)

    def _get_line_number(self):
        if self.tree:
            return self.tree.get_sentence_index()
        return self._line_number
    def _set_line_number(self, val):
        if self.tree:
            raise Exception("after enrichment set tree instead")
        self._line_number = val
    line_number = property(_get_line_number, _set_line_number)

    def _get_document_id(self):
        if self.tree:
            return self.tree.document_id
        return self._document_id

    def _set_document_id(self, val):
        if self.tree:
            raise Exception("after enrichment set tree instead")
        self._document_id = val
    document_id = property(_get_document_id, _set_document_id)


    @staticmethod
    def from_string(speaker_line, line_number, document_id):
        bits = speaker_line.split()

        if len(bits) == 1 and bits[0].strip() == "-":
            return None

        if len(bits) != 5:
            on.common.log.error("Invalid line in speaker file: %s (%s)"
                                % (speaker_line, document_id))

        return speaker_sentence(line_number, document_id, bits[0], bits[1],
                                bits[2], bits[3], bits[4])

    def __repr__(self):
        return "<speaker sentence object: %s>" % ( "; ".join(
            ["%s = %s" % (at, getattr(self, at)) for at in
             "id line_number document_id start_time stop_time name gender competence".split()]))

    sql_table_name = "speaker_sentence"
    sql_create_statement = \
"""
create table speaker_sentence
(
    id              varchar(255) not null primary key,
    line_number     int not null,
    document_id     varchar(255) not null,
    start_time      double not null,
    stop_time       double not null,
    name            varchar(255) not null,
    gender          varchar(255) not null,
    competence      varchar(255) not null,
    foreign key (document_id) references document.id,
    foreign key (id) references tree.id
)
default character set utf8;
"""

    sql_insert_statement = \
"""
insert into speaker_sentence
(
    id,
    line_number,
    document_id,
    start_time,
    stop_time,
    name,
    gender,
    competence
) values (%s, %s, %s, %s, %s, %s, %s, %s)
"""

    def write_to_db(self, cursor):
        cursor.execute("%s" % (self.__class__.sql_insert_statement),
                        (self.id, self.line_number,
                         self.document_id, self.start_time,
                         self.stop_time, self.name, self.gender,
                         self.competence))


    def enrich_tree(self, a_tree):
        """Modify the trees in the tree document to have speaker information"""
        a_tree.speaker_sentence = self
        self.tree = a_tree

class speaker_document:
    """
    Contained by: :class:`speaker_bank`

    Contains: :class:`speaker_document`

    """

    def __init__(self, document_id, extension="speaker"):
        self.document_id = document_id
        self.extension=extension
        self.speaker_sentence_hash = {}
        self.by_line_number = {}
        self.speaker_sentence_id_list = []


    def append(self, ss):
        if not ss:
            return

        self.speaker_sentence_id_list.append(ss.id)
        self.speaker_sentence_hash[ss.id] = ss
        self.by_line_number[ss.line_number] = ss

    @staticmethod
    def from_file(speaker_file_lines, document_id, extension="speaker"):
        """ each speaker file line should be like:

        0.0584225900682 12.399083739    speaker1        male    native

        or

        12.3271665044   21.6321665044   paula_zahn      female  native

        or

        -
        """

        a_speaker_document = speaker_document(document_id, extension)

        for line_number, speaker_line in enumerate(speaker_file_lines):
            a_speaker_document.append(
                speaker_sentence.from_string(speaker_line, line_number, document_id))

        return a_speaker_document

    @staticmethod
    def from_db(document_id, a_cursor, extension="speaker"):
        a_speaker_document = speaker_document(document_id, extension)

        a_cursor.execute("""select *
                            from speaker_sentence
                            where speaker_sentence.document_id = '%s'""" % document_id)


        for a_speaker_sentence in [
            speaker_sentence(d_row["line_number"], d_row["document_id"],
                             d_row["start_time"], d_row["stop_time"], d_row["name"],
                             d_row["gender"], d_row["competence"])
            for d_row in a_cursor.fetchall()]:

            a_speaker_document.append(a_speaker_sentence)

        return a_speaker_document

    def __getitem__(self, index):
        return self.speaker_sentence_hash[self.speaker_sentence_id_list[index]]

    def __len__(self):
        return len(self.speaker_sentence_id_list)

    def __repr__(self):
        return "speaker_document instance, id=%s ; %d speaker sentences" % (
            self.document_id, len(self))

    def write_to_db(self, cursor):
        for a_speaker_sentence in self:
            a_speaker_sentence.write_to_db(cursor)

    def dump_view(self, a_cursor=None, out_dir=""):
        if not self:
            return

        with codecs.open(on.common.util.output_file_name(self.document_id, self.extension, out_dir), "w", "utf-8") as out_f:
            max_line_number = self[-1].line_number

            for line_number in range(max_line_number + 1):
                try:
                    ss = self.by_line_number[line_number]
                    out_f.write("%s %s %s %s %s\n" % (
                        ss.start_time, ss.stop_time, ss.name, ss.gender, ss.competence))
                except KeyError:
                    out_f.write("-\n")

class speaker_bank(abstract_bank):
    """
    Contains: :class:`speaker_document`

    """


    def __init__(self, a_subcorpus, tag, a_cursor=None, extension="speaker"):
        abstract_bank.__init__(self, a_subcorpus, tag, extension)

        if(a_cursor == None):
            sys.stderr.write("reading the speaker bank [%s] ..." % self.extension)
            for a_file in self.subcorpus.get_files(self.extension):
                sys.stderr.write(".")

                with codecs.open(a_file.physical_filename, "r", "utf-8") as f:
                    speaker_file_lines = f.readlines()

                self.append(speaker_document.from_file(speaker_file_lines, a_file.document_id + "@" + a_subcorpus.id, self.extension))
            sys.stderr.write("\n")
        else:
            pass

    sql_table_name = "speaker_sentence"

    def enrich_treebank(self, a_treebank):
        abstract_bank.enrich_treebank(self, a_treebank)

        for a_speaker_document in self:
            sys.stderr.write(".")

            for a_speaker_sentence in a_speaker_document:
                a_tree = a_speaker_document.tree_document[a_speaker_sentence.line_number]
                a_speaker_sentence.enrich_tree(a_tree)

        sys.stderr.write("\n")

    @classmethod
    def from_db(cls, a_subcorpus, tag, a_cursor, affixes=None):
        sys.stderr.write("reading the speaker bank ....")
        a_speaker_bank = speaker_bank(a_subcorpus, tag, a_cursor)

        a_cursor.execute("""select id from document
                            where document.subcorpus_id = '%s';""" % a_subcorpus.id)

        for a_document_id in [d_row["id"] for d_row in a_cursor.fetchall()]:

            if not on.common.util.matches_an_affix(a_document_id, affixes):
                continue

            sys.stderr.write(".")
            a_speaker_bank.append(speaker_document.from_db(a_document_id, a_cursor, a_speaker_bank.extension))

        sys.stderr.write("\n")
        return a_speaker_bank

