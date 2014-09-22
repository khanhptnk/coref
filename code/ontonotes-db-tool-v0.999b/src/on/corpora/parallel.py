
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
----------------------------------------------------------------------
:mod:`parallel` -- Alignment Metadata for Parallel Texts
----------------------------------------------------------------------

See:

 - :class:`parallel_sentence`
 - :class:`parallel_document`
 - :class:`parallel_bank`

Correspondences:

 ===========================  ======================================================  =====================================================================
 **Database Tables**          **Python Objects**                                      **File Elements**
 ===========================  ======================================================  =====================================================================
 None                         :class:`parallel_bank`                                  All ``.parallel`` files in an :class:`on.corpora.subcorpus`
 ``parallel_document``        :class:`parallel_document`                              The second line (original/translation line) in a ``.parallel`` file
 ``parallel_sentence``        :class:`parallel_sentence`                              All lines in a ``.parallel`` file after the first two (map lines)
 ===========================  ======================================================  =====================================================================

.. autoclass:: parallel_bank
.. autoclass:: parallel_document
.. autoclass:: parallel_sentence

"""

# author: sameer pradhan

from __future__ import with_statement
import os
import os.path
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

class parallel_sentence(object):

    def __init__(self, id_original, id_translation):
        self._trees = {"translation": None, "original" : None}

        self._ids = {}
        self.id_original = id_original
        self.id_translation = id_translation

    @property
    def id(self):
        return "%s.%s" % (self.id_translation, self.id_original)

    def __id_setter(trans_or_orig):
        def _set_id(self, val):
            if self._trees[trans_or_orig]:
                raise Exception("Cannot set id_%s after enrichment.  Set tree_%s instead." % (trans_or_orig, trans_or_orig))
            self._ids[trans_or_orig] = val
        return _set_id

    def __id_getter(trans_or_orig):
        def _get_id(self):
            if self._trees[trans_or_orig]:
                return self._trees[trans_or_orig].id
            return self._ids[trans_or_orig]
        return _get_id

    id_original = property(__id_getter("original"), __id_setter("original"))
    id_translation = property(__id_getter("translation"), __id_setter("translation"))

    @property
    def tree_translation(self):
        return self._trees["translation"]

    @property
    def tree_original(self):
        return self._trees["original"]

    def set_trees(self, new_original, new_translation):
        if not ((new_original and new_translation) or (not new_original and not new_translation)):
            raise Exception("set_trees: new_original and new_translation should be both None or both non-None; instead got o=%s and t=%s" % (
                None if not new_original else new_original.id,
                None if not new_translation else new_translation.id))

        assert (self.tree_original and self.tree_translation) or (not self.tree_original and not self.tree_translation)

        if self.tree_translation and self.tree_original:
            self.tree_translation.originals.remove(self.tree_original)
            self.tree_original.translations.remove(self.tree_translation)

        if new_original and new_translation:
            new_original.translations.append(new_translation)
            new_translation.originals.append(new_original)
        else:
            self._ids["original"] = self.id_original
            self._ids["translation"] = self.id_translation

        self._trees["original"] = new_original
        self._trees["translation"] = new_translation

    @staticmethod
    def from_string(map_line, original_document_id, translated_document_id):
        bits = map_line.split()

        if len(bits) != 3 or bits[0] != "map":
            on.common.log.error("Invalid line in parallel file: %s (%s)"
                                % (map_line, translated_document_id))

        return parallel_sentence("%s@%s" % (bits[1], original_document_id),
                                 "%s@%s" % (bits[2], translated_document_id))

    def __repr__(self):
        return "<parallel sentence object: id=%s ; id_original=%s ; id_translation=%s>"\
               % (self.id, self.id_original, self.id_translation)

    sql_table_name = "parallel_sentence"
    sql_create_statement = \
"""
create table parallel_sentence
(
    id varchar(255) not null primary key,
    sentence_id_original varchar(255) not null,
    sentence_id_translation varchar(255) not null,
    foreign key (sentence_id_original) references sentence.id,
    foreign key (sentence_id_translation) references sentence.id
)
default character set utf8;
"""

    sql_insert_statement = \
"""
insert into parallel_sentence
(
    id,
    sentence_id_original,
    sentence_id_translation
) values (%s, %s, %s)
"""

    def write_to_db(self, cursor):
        cursor.execute("%s" % (self.__class__.sql_insert_statement),
                        (self.id, self.id_original, self.id_translation))


    def enrich_tree_document(self, original_tree_document,
                             translation_tree_document):
        """ find the appropriate trees in the original and translated
        documents and let them know that they are related.  Note that
        it's fine for a sentence to have multiple originals and for a
        sentence to have multiple translations. """

        for a_id, a_td in [[self.id_original, original_tree_document],
                           [self.id_translation, translation_tree_document]]:
            if a_id not in a_td.tree_ids:
                on.common.log.warning("id %s not found in tree document %s %r\nThis is probably not the tree document this .parallel file was annotated against." % (
                    a_id, a_td.document_id, [tid.split("@")[0] for tid in a_td.tree_ids]))
                return

        self.set_trees(original_tree_document.tree_hash[self.id_original],
                       translation_tree_document.tree_hash[self.id_translation])


class parallel_document(object):

    def __init__(self, id_original, id_translation, extension="parallel"):
        self._tree_documents = {"translation" : None, "original" : None}
        self._ids = {}
        self.id_original = id_original
        self.id_translation = id_translation

        self.parallel_sentence_hash = {}
        self.parallel_sentence_id_list = []
        self.extension = extension


    @property
    def id(self):
        return "%s.%s" % (self.id_translation, self.id_original)

    @property
    def document_id(self):
        return self.id_translation

    def __id_setter(trans_or_orig):
        def _set_id(self, a_id):
            if self._tree_documents[trans_or_orig]:
                raise Exception("Cannot set id_%s after enrichment; set tree_%s instead." % (trans_or_orig, trans_or_orig))
            self._ids[trans_or_orig] = a_id
        return _set_id

    def __id_getter(trans_or_orig):
        def _get_id(self):
            if self._tree_documents[trans_or_orig]:
                return self._tree_documents[trans_or_orig].document_id
            return self._ids[trans_or_orig]
        return _get_id

    id_original = property(__id_getter("original"), __id_setter("original"))
    id_translation = property(__id_getter("translation"), __id_setter("translation"))

    @property
    def original_tree_document(self):
        return self._tree_documents["original"]

    @property
    def translation_tree_document(self):
        return self._tree_documents["translation"]


    def set_tree_documents(self, new_original, new_translation):
        if not ((new_original and new_translation) or (not new_original and not new_translation)):
            raise Exception("expected both None or both non-None")

        assert (self.original_tree_document and self.translation_tree_document) or (not self.original_tree_document and not self.translation_tree_document)

        if new_original and new_translation:
            new_original.translations.append(new_translation)

            if new_translation.original:
                raise Exception("Got a document with more than one original: %s (%s ; %s)"
                                % (new_translation.id, new_translation.original.id, new_original.id))

            new_translation.original = new_original

        if self.original_tree_document and self.translation_tree_document:
            self.original_tree_document.translations.remove(self.translation_tree_document)
            self.translation_tree_document.original = None

        if not new_original and not new_translation and self.original_tree_document and self.translation_tree_document:
            self._ids["original"] = self.id_original
            self._ids["translation"] = self.id_translation

        self._tree_documents["original"] = new_original
        self._tree_documents["translation"] = new_translation

    def enrich_tree_documents(self, original_document, translation_document):

        self.set_tree_documents(original_document, translation_document)

        for a_parallel_sentence in self:
            a_parallel_sentence.enrich_tree_document(original_document, translation_document)

    @staticmethod
    def from_file(parallel_file_lines, translated_file_path, translated_subcorpus_id, extension="parallel"):
        """ parallel file lines should look something like:

        translated document
        original chinese bc/cctv/00/cctv_0000
        map 0 0
        map 0 1
        ...

        """

        if not parallel_file_lines[0].startswith("translated document"):
            on.common.log.error("parallel_document: given bad file with initial line %s (in %s)"
                                % (parallel_file_lines[0], translated_subcorpus_id))

        original_file_path = parallel_file_lines[1].split()[2]
        original_lang_abbr = parallel_file_lines[1].split()[1]

        original_subcorpus_id = "@".join(translated_subcorpus_id.split("@")[:-2] +
                                         [original_lang_abbr, translated_subcorpus_id.split("@")[-1]])


        original_document_id = "%s@%s"   % ( original_file_path, original_subcorpus_id )
        translated_document_id = "%s@%s" % ( translated_file_path, translated_subcorpus_id )

        translated_fid = translated_file_path.split("/")[-1].split("_")[-1]
        original_fid = original_file_path.split("/")[-1].split("_")[-1]

        if "@%s@" % translated_fid in original_document_id:
            original_document_id = original_document_id.replace("@%s@" % translated_fid, "@%s@" % original_fid)
        elif "@%s@" % translated_fid[:2] in original_document_id:
            original_document_id = original_document_id.replace("@%s@" % translated_fid[:2], "@%s@" % original_fid[:2])


        a_parallel_document = parallel_document(original_document_id, translated_document_id, extension)


        # now we go through the rest of the lines in the .parallel
        # file and build a large number of sentence to sentence
        # mappings.  Note there may not be any at all if the
        # annotators didn't mark sentence to sentence mappings for
        # this document
        for line in [l.strip() for l in parallel_file_lines[2:] if l.strip()]:
            a_parallel_document.append( parallel_sentence.from_string(line, original_document_id, translated_document_id))

        return a_parallel_document

    @staticmethod
    def from_db(translation_document_id, a_cursor, extension="parallel"):

        a_cursor.execute("select document_id_original from parallel_document where document_id_translation = '%s'" % translation_document_id)
        rows = a_cursor.fetchall()

        if len(rows) != 1:
            on.common.log.error("Database coherency problem: multiple originals for translation %s" % translation_document_id)

        original_document_id = rows[0]["document_id_original"]

        a_parallel_document = parallel_document(original_document_id, translation_document_id, extension)


        a_cursor.execute("""select *
                            from parallel_sentence
                            join sentence
                            on sentence.id = parallel_sentence.sentence_id_translation
                            where sentence.document_id = '%s'""" % translation_document_id)

        for o, t in [(d_row["sentence_id_original"], d_row["sentence_id_translation"])
                     for d_row in a_cursor.fetchall()]:
            a_parallel_document.append(parallel_sentence(o, t))

        return a_parallel_document

    def __getitem__(self, index):
        return self.parallel_sentence_hash[self.parallel_sentence_id_list[index]]

    def __len__(self):
        return len(self.parallel_sentence_id_list)

    def append(self, item):
        self.parallel_sentence_id_list.append(item.id)
        self.parallel_sentence_hash[item.id] = item

    def __repr__(self):
        return "parallel_document instance: id=%s ; id_original=%s ; id_translation=%s ; %d parallel_sentences>"\
               % (self.id, self.id_original, self.id_translation, len(self))

    sql_table_name = "parallel_document"
    sql_create_statement = \
"""
create table parallel_document
(
    id varchar(255) not null,
    document_id_original varchar(255) not null,
    document_id_translation varchar(255) not null,
    foreign key (document_id_original) references document.id,
    foreign key (document_id_translation) references document.id
)
default character set utf8;
"""

    sql_insert_statement = \
"""
insert into parallel_document
(
    id,
    document_id_original,
    document_id_translation
) values (%s, %s, %s)
"""

    def write_to_db(self, cursor):
        cursor.execute("%s" % (self.sql_insert_statement),
                       (self.id, self.id_original, self.id_translation))

        for a_parallel_sentence in self:
            a_parallel_sentence.write_to_db(cursor)

    def dump_view(self, a_cursor=None, out_dir = ""):

        original_lang    = self.id_original.split(   "@")[-2]
        translation_lang = self.id_translation.split("@")[-2]

        original_path    = self.id_original.split(   "@")[0]
        translation_path = self.id_translation.split("@")[0]

        with codecs.open(on.common.util.output_file_name(self.id_translation, self.extension, out_dir), "w", "utf-8") as t_f:
            t_f.write("translated document\n")
            t_f.write("original %s %s\n" % (original_lang, original_path))
            for a_parallel_sentence in self:
                t_f.write("map %s %s\n" % (a_parallel_sentence.id_original.split(   "@")[0],
                                           a_parallel_sentence.id_translation.split("@")[0]))

        original_file_name = on.common.util.output_file_name(self.id_original, self.extension, out_dir)

        existing_lines = []
        existed = os.path.exists(original_file_name)
        if existed:
            with codecs.open(original_file_name, "r", "utf-8") as i_f:
                for line in i_f:
                    if line.startswith("translation"):
                        existing_lines.append(line)
        with codecs.open(original_file_name, "a", "utf-8") as o_f:
            if not existed:
                o_f.write("original document\n")
            to_write = "translation %s %s\n" % (translation_lang, translation_path)
            if to_write not in existing_lines:
                o_f.write(to_write)

class parallel_bank(abstract_bank):
    def __init__(self, a_subcorpus, tag, a_cursor=None, extension="parallel"):
        abstract_bank.__init__(self, a_subcorpus, tag, extension)

        self.matching_parallel_banks = []
        self.matching_treebanks = []
        self.matching_subcorpora = []

        if(a_cursor == None):
            sys.stderr.write("reading the parallel bank [%s] ..." % self.extension)
            for a_file in self.subcorpus.get_files(self.extension):
                sys.stderr.write(".")

                with codecs.open(a_file.physical_filename, "r", "utf-8") as f:
                    parallel_file_lines = f.readlines()

                if parallel_file_lines[0].startswith("original document"):
                    """ we want to map translated documents back to their originals.
                    There are two mapping files, which means there is redundant information,
                    and we just need to do thing with the parallel files that represent
                    translations. """
                    continue

                self.append(parallel_document.from_file(parallel_file_lines, a_file.document_id, a_subcorpus.id, self.extension))
            sys.stderr.write("\n")
        else:
            pass

    sql_table_name = "parallel_document"
    sql_exists_field = "document_id_translation"


    def enrich_treebank(self, a_translation_treebank):
        """ because we need both the original and the translation, we
        will ask the subcorpus for originals for this treebank, which
        may result in loading more treebanks.

        """

        if not self:
            return # we don't contain any documents because we're the original's parallel bank

        sys.stderr.write("finding original trees to prepare for parallel bank enrichment....")

        for a_parallel_document in self:
            try:
                a_original_subcorpus = self.subcorpus.find_subcorpus_for_document_id(a_parallel_document.id_original)
            except KeyError:
                continue

            a_original_treebank = a_original_subcorpus[a_translation_treebank.extension]

            if a_original_subcorpus not in self.matching_subcorpora:
                self.matching_subcorpora.append(a_original_subcorpus)

            if a_original_treebank not in self.matching_treebanks:
                self.matching_treebanks.append(a_original_treebank)

        sys.stderr.write("  found %d original treebanks.\n" % len(self.matching_treebanks))

        if not self.matching_treebanks:
            on.common.log.status("warning: did not find *any* original treebanks")
            return

        sys.stderr.write("enriching treebanks with tree-to-tree parallel data ....")

        for a_parallel_document in self:
            sys.stderr.write(".")

            assert a_parallel_document.id_translation in a_translation_treebank

            original_treebank_candidates = [tb for tb in self.matching_treebanks if a_parallel_document.id_original in tb]
            if len(original_treebank_candidates) != 1:
                on.common.log.warning("the original document (%s) for document %s is %s; unable to enrich." % (
                    a_parallel_document.id_original, a_parallel_document.id_translation,
                    "loaded more than once (%d times)" % len(original_treebank_candidates) if original_treebank_candidates else "not loaded"))
                continue

            a_original_treebank = original_treebank_candidates[0]

            a_parallel_document.enrich_tree_documents(a_original_treebank.get_document(a_parallel_document.id_original),
                                                      a_translation_treebank.get_document(a_parallel_document.id_translation))

        sys.stderr.write("\n")


    @classmethod
    def from_db(cls, a_subcorpus, tag, a_cursor, affixes=None):
        sys.stderr.write("reading the parallel bank ....")
        a_parallel_bank = parallel_bank(a_subcorpus, tag, a_cursor)

        #---- now get all document ids for this subcorpus which are translations of other documents ----#
        a_cursor.execute("""select document_id_translation
                            from parallel_document
                            join document
                            on parallel_document.document_id_translation = document.id
                            where document.subcorpus_id = '%s';""" % a_subcorpus.id)

        for a_document_id in [d_row["document_id_translation"] for d_row in a_cursor.fetchall()]:

            if not on.common.util.matches_an_affix(a_document_id, affixes):
                continue

            sys.stderr.write(".")
            a_parallel_bank.append(parallel_document.from_db(a_document_id, a_cursor, a_parallel_bank.extension))

        sys.stderr.write("\n")
        return a_parallel_bank

