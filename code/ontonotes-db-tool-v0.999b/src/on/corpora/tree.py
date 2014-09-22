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
----------------------------------------------------
:mod:`tree` -- Syntactic Parse Annotation
----------------------------------------------------

See:

 - :class:`on.corpora.tree.tree`
 - :class:`on.corpora.tree.treebank`

Correspondences:

  ===========================  ======================================================  =========================================================
  **Database Tables**          **Python Objects**                                      **File Elements**
  ===========================  ======================================================  =========================================================
  ``treebank``                 treebank                                                All ``.parse`` files for a :class:`on.corpora.subcorpus`
  None                         tree_document                                           A ``.parse`` file
  ``tree``                     tree                                                    An S-expression in a ``.parse`` file
  ``syntactic_link``           syntactic_link                                          The numbers after '-' and '=' in trees
  ``lemma``                    lemma                                                   ``.lemma`` files (arabic only)
  ===========================  ======================================================  =========================================================

.. autoclass:: treebank
.. autoclass:: tree_document
.. autoclass:: tree
.. autoclass:: lemma
.. autoclass:: syntactic_link
.. autoclass:: compound_function_tag
.. autoexception:: tree_exception


"""




from __future__ import with_statement

# standard python imports
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
import commands
import tempfile
import itertools




# xml specific imports
from xml.etree import ElementTree
import xml.etree.cElementTree as ElementTree





# custom package imports
import on

import on.common.log
import on.common.util
from on.common.util import bunch

import on.corpora

from collections import defaultdict
from on.common.util import wrap
from on.corpora import abstract_bank


# this class represents the phrase type of a node in the tree
class phrase_type:
    references = [["tree", "phrase_type"]]

    allowed = [ "ADJP",
                "ADVP",
                "CONJP",
                "EDITED",
                "FRAG",
                "INTJ",
                "LST",
                "META",
                "NAC",
                "NML",
                "NP",
                "NX",
                "PP",
                "PRN",
                "PRT",
                "QP",
                "RRC",
                "S",
                "SBAR",
                "SBARQ",
                "SINV",
                "SQ",
                "TOP",
                "UCP",
                "VP",
                "WHADJP",
                "WHADVP",
                "WHNP",
                "WHPP",
                "X",

                # english callhome data uses EMBED for cases where a
                # speaker B interrupts another speaker A to say
                # something without disrupting the sentence structure
                # of A.  A's sentence is parsed as a single tree, B's
                # interruption is also parsed, and B's tree is
                # embedded in A's and tagged EMBED
                "EMBED",


                # chinese
                "IP",  # simple clause headed by I (INFL)
                "CP",  # clause headed by C (complementizer)
                "CLP", # classifier phrase
                "DP",  # determiner phrase
                "DNP", # phrase formed by "XP + DEG"
                "DVP", # phrase formed by "XP + DEV"
                "LCP", # phrase formed by "XP + LC"
                "VCD", # coordinated verb compound
                "VCP", # verb compounds formed by VV + VC
                "VNV", # verb compounds formed by A-not-A or A-one-A
                "VPT", # potential form V-de-R or V-bu-R
                "VRD", # verb resultative compound
                "VSB",  # verb compounds formed by a modifier + a head

                # chinese, not in any documentation, new for BC

                "FLR", # filler
                "INC", # incomplete
                "DFL" # disfluency


                ]

    sql_table_name = "phrase_type"
    sql_create_statement = \
"""
create table phrase_type
(
  id varchar(255) not null collate utf8_bin primary key
)
default character set utf8;
"""


    sql_insert_statement = \
"""insert into phrase_type
(
  id
)
values (%s)
"""


# this class represents the part of speech of the leaves in
# the tree
class pos_type:
    references = [["tree", "part_of_speech"]]

    allowed = { "AFX":  "AFX",
                "CC":   "CC",
                "CD":   "CD",
                "DT":   "DT",
                "EX":   "EX",
                "FW":   "FW",
                "IN":   "IN",
                "JJ":   "JJ",
                "JJR":  "JJR",
                "JJS":  "JJS",
                "LS":   "LS",
                "MD":   "MD",
                "NN":   "NN",
                "NNS":  "NNS",
                "NNP":  "NNP",
                "NNPS": "NNPS",
                "PDT":  "PDT",
                "POS":  "POS",
                "PRP":  "PRP",
                "PRP$": "PRP$",
                "RB":   "RB",
                "RBR":  "RBR",
                "RBS":  "RBS",
                "RP":   "RP",
                "SYM":  "SYM",
                "TO":   "TO",
                "UH":   "UH",
                "VB":   "VB",
                "VBD":  "VBD",
                "VBG":  "VBG",
                "VBN":  "VBN",
                "VBP":  "VBP",
                "VBZ":  "VBZ",
                "WDT":  "WDT",
                "WP":   "WP",
                "WP$":  "WP$",
                "WRB":  "WRB",

                "ADD": "ADD", # URLs and email addresses

                "TYPO": "TYPO",

                # english punctuation and other non text tags

                "``" : "``",
                "''" : "''",
                "#" : "#",
                "$": "$",
                "-LRB-": "-LRB-", # (
                "-RRB-": "-RRB-", # )
                "-LSB-": "-LSB-", # [
                "-RSB-": "-RSB-", # ]
                ",": ",",
                ".": ".",
                ":": ":",
                "HYPH": "HYPH",
                "CODE": "CODE",
                "-NONE-": "-NONE-",
                "LST" : "LST", # list marker
                "NFP" : "NFP", # non functional punctuation

                "XX" :  "XX",

                "X" : "X", # unknown, uncertain or unbracketable

                # chinese types, with their mapping back penn tags
                "NT":   "NN",
                "NR":   "NNP",
                "LC":   None,
                "PN":   "PRP",
                "VV":   "VB",
                "VA":   "VB",
                "VC":   "VB",
                "VE":   "VB",
                "AD":   "RB",
                "P" :   "IN",
                "M" :   None,
                "CS":   "IN",
                "IJ":   "UH",
                "ON":   None,
                "PU":   None,
                "LB":   None,
                "SB":   None,
                "BA":   None,

                # new chinese ones, all without mappings calculated, from parseguide2000

                "AS":   None, # aspect marker
                "DEC":  None, # DE for relative clauses
                "DEG":  None, # associative DE
                "DER":  None, # ?? in V-de const. and V-de-R
                "DEV":  None, # ?? as the head of a DVP
                "ETC":  None, # tags for ?? and ???? in coordination phrases
                "MSP":  None, # some particles
                "OD":   None, # ordinal numbers
                "SP":   None, # sentence-final particle

                # arabic types, with their mapping back to penn tags

                # JJR
                "ADJ_COMP": "JJR",
                "ADJ_COMP+CASE_DEF_ACC": "JJR",
                "ADJ_COMP+CASE_DEF_GEN": "JJR",
                "ADJ_COMP+CASE_DEF_NOM": "JJR",
                "ADJ_COMP+CASE_INDEF_ACC": "JJR",
                "ADJ_COMP+CASE_INDEF_GEN": "JJR",
                "ADJ_COMP+CASE_INDEF_NOM": "JJR",
                "ADJ_COMP+NSUFF_FEM_SG+CASE_INDEF_NOM": "JJR",
                "DET+ADJ_COMP": "JJR",
                "DET+ADJ_COMP+CASE_DEF_ACC": "JJR",
                "DET+ADJ_COMP+CASE_DEF_GEN": "JJR",
                "DET+ADJ_COMP+CASE_DEF_NOM": "JJR",
                "DET+ADJ_COMP+NSUFF_MASC_DU_GEN": "JJR",
                "DET+ADJ_COMP+NSUFF_MASC_PL_GEN": "JJR",
                "DET+ADJ_COMP+NSUFF_MASC_PL_NOM": "JJR",

                # NOUN_QUANT (nominal quantifier)
                "DET+NOUN_QUANT+CASE_DEF_ACC": "NOUN_QUANT",
                "DET+NOUN_QUANT+CASE_DEF_GEN": "NOUN_QUANT",
                "DET+NOUN_QUANT+CASE_DEF_NOM": "NOUN_QUANT",
                "DET+NOUN_QUANT+NSUFF_FEM_SG+CASE_DEF_ACC": "NOUN_QUANT",
                "DET+NOUN_QUANT+NSUFF_FEM_SG+CASE_DEF_GEN": "NOUN_QUANT",
                "DET+NOUN_QUANT+NSUFF_FEM_SG+CASE_DEF_NOM": "NOUN_QUANT",
                "NOUN_QUANT": "NOUN_QUANT",
                "NOUN_QUANT+CASE_DEF_ACC": "NOUN_QUANT",
                "NOUN_QUANT+CASE_DEF_GEN": "NOUN_QUANT",
                "NOUN_QUANT+CASE_DEF_NOM": "NOUN_QUANT",
                "NOUN_QUANT+CASE_INDEF_ACC": "NOUN_QUANT",
                "NOUN_QUANT+CASE_INDEF_GEN": "NOUN_QUANT",
                "NOUN_QUANT+CASE_INDEF_NOM": "NOUN_QUANT",
                "NOUN_QUANT+NSUFF_FEM_SG+CASE_DEF_ACC": "NOUN_QUANT",
                "NOUN_QUANT+NSUFF_FEM_SG+CASE_DEF_GEN": "NOUN_QUANT",
                "NOUN_QUANT+NSUFF_FEM_SG+CASE_DEF_NOM": "NOUN_QUANT",
                "NOUN_QUANT+NSUFF_FEM_SG+CASE_INDEF_ACC": "NOUN_QUANT",
                "NOUN_QUANT+NSUFF_FEM_SG+CASE_INDEF_GEN": "NOUN_QUANT",
                "NOUN_QUANT+NSUFF_MASC_DU_ACC_POSS": "NOUN_QUANT",
                "NOUN_QUANT+NSUFF_MASC_DU_GEN": "NOUN_QUANT",
                "NOUN_QUANT+NSUFF_MASC_DU_GEN_POSS": "NOUN_QUANT",
                "NOUN_QUANT+NSUFF_MASC_DU_NOM_POSS": "NOUN_QUANT",

                # ADJ_NUM (ordinal number/numerical adjective)
                "ADJ_NUM": "ADJ_NUM",
                "ADJ_NUM+CASE_DEF_ACC": "ADJ_NUM",
                "ADJ_NUM+CASE_DEF_GEN": "ADJ_NUM",
                "ADJ_NUM+CASE_DEF_NOM": "ADJ_NUM",
                "ADJ_NUM+CASE_INDEF_ACC": "ADJ_NUM",
                "ADJ_NUM+CASE_INDEF_GEN": "ADJ_NUM",
                "ADJ_NUM+CASE_INDEF_NOM": "ADJ_NUM",
                "ADJ_NUM+NSUFF_FEM_DU_GEN": "ADJ_NUM",
                "ADJ_NUM+NSUFF_FEM_SG+CASE_DEF_ACC": "ADJ_NUM",
                "ADJ_NUM+NSUFF_FEM_SG+CASE_DEF_GEN": "ADJ_NUM",
                "ADJ_NUM+NSUFF_FEM_SG+CASE_DEF_NOM": "ADJ_NUM",
                "ADJ_NUM+NSUFF_FEM_SG+CASE_INDEF_ACC": "ADJ_NUM",
                "ADJ_NUM+NSUFF_FEM_SG+CASE_INDEF_GEN": "ADJ_NUM",
                "ADJ_NUM+NSUFF_FEM_SG+CASE_INDEF_NOM": "ADJ_NUM",
                "DET+ADJ_NUM": "ADJ_NUM",
                "DET+ADJ_NUM+CASE_DEF_ACC": "ADJ_NUM",
                "DET+ADJ_NUM+CASE_DEF_GEN": "ADJ_NUM",
                "DET+ADJ_NUM+CASE_DEF_NOM": "ADJ_NUM",
                "DET+ADJ_NUM+NSUFF_FEM_PL+CASE_DEF_NOM": "ADJ_NUM",
                "DET+ADJ_NUM+NSUFF_FEM_PL+CASE_DEF_GEN": "ADJ_NUM",
                "DET+ADJ_NUM+NSUFF_FEM_SG": "ADJ_NUM",
                "DET+ADJ_NUM+NSUFF_FEM_SG+CASE_DEF_ACC": "ADJ_NUM",
                "DET+ADJ_NUM+NSUFF_FEM_SG+CASE_DEF_GEN": "ADJ_NUM",
                "DET+ADJ_NUM+NSUFF_FEM_SG+CASE_DEF_NOM": "ADJ_NUM",
                "DET+ADJ_NUM+NSUFF_MASC_DU_ACC": "ADJ_NUM",
                "DET+ADJ_NUM+NSUFF_MASC_DU_GEN": "ADJ_NUM",
                "DET+ADJ_NUM+NSUFF_MASC_DU_NOM": "ADJ_NUM",
                "DET+ADJ_NUM+NSUFF_MASC_PL_ACCGEN": "ADJ_NUM",
                "DET+ADJ_NUM+NSUFF_MASC_PL_GEN": "ADJ_NUM",

                # VBG (used here for verbal nouns/gerunds)
                "DET+NOUN.VN": "VBG",
                "DET+NOUN.VN+CASE_DEF_ACC": "VBG",
                "DET+NOUN.VN+CASE_DEF_GEN": "VBG",
                "DET+NOUN.VN+CASE_DEF_NOM": "VBG",
                "DET+NOUN.VN+NSUFF_FEM_DU_GEN": "VBG",
                "DET+NOUN.VN+NSUFF_FEM_SG+CASE_DEF_ACC": "VBG",
                "DET+NOUN.VN+NSUFF_FEM_SG+CASE_DEF_GEN": "VBG",
                "DET+NOUN.VN+NSUFF_FEM_SG+CASE_DEF_NOM": "VBG",
                "DET+NOUN.VN+NSUFF_MASC_DU_NOM": "VBG",
                "DET+NOUN.VN+NSUFF_MASC_PL_ACC": "VBG",
                "DET+NOUN.VN+NSUFF_MASC_PL_ACCGEN": "VBG",
                "DET+NOUN.VN+NSUFF_MASC_PL_GEN": "VBG",
                "DET+NOUN.VN+NSUFF_MASC_PL_NOM": "VBG",
                "NOUN.VN": "VBG",
                "NOUN.VN+CASE_DEF_ACC": "VBG",
                "NOUN.VN+CASE_DEF_GEN": "VBG",
                "NOUN.VN+CASE_DEF_NOM": "VBG",
                "NOUN.VN+CASE_INDEF_ACC": "VBG",
                "NOUN.VN+CASE_INDEF_GEN": "VBG",
                "NOUN.VN+CASE_INDEF_NOM": "VBG",
                "NOUN.VN+NSUFF_FEM_DU_ACC": "VBG",
                "NOUN.VN+NSUFF_FEM_PL+CASE_DEF_ACC": "VBG",
                "NOUN.VN+NSUFF_FEM_PL+CASE_DEF_ACCGEN": "VBG",
                "NOUN.VN+NSUFF_FEM_PL+CASE_DEF_GEN": "VBG",
                "NOUN.VN+NSUFF_FEM_PL+CASE_DEF_NOM": "VBG",
                "NOUN.VN+NSUFF_FEM_SG": "VBG",
                "NOUN.VN+NSUFF_FEM_SG+CASE_DEF_ACC": "VBG",
                "NOUN.VN+NSUFF_FEM_SG+CASE_DEF_GEN": "VBG",
                "NOUN.VN+NSUFF_FEM_SG+CASE_DEF_NOM": "VBG",
                "NOUN.VN+NSUFF_FEM_SG+CASE_INDEF_ACC": "VBG",
                "NOUN.VN+NSUFF_FEM_SG+CASE_INDEF_GEN": "VBG",
                "NOUN.VN+NSUFF_FEM_SG+CASE_INDEF_NOM": "VBG",
                "NOUN.VN+NSUFF_MASC_DU_GEN": "VBG",
                "NOUN.VN+NSUFF_MASC_PL_ACC": "VBG",
                "NOUN.VN+NSUFF_MASC_PL_ACCGEN": "VBG",
                "NOUN.VN+NSUFF_MASC_PL_NOM": "VBG",

                # VN (used for verbal nominals/active or passive participles)
                "ADJ.VN": "VN",
                "ADJ.VN+CASE_DEF_GEN": "VN",
                "ADJ.VN+CASE_DEF_NOM": "VN",
                "ADJ.VN+CASE_INDEF_ACC": "VN",
                "ADJ.VN+CASE_INDEF_GEN": "VN",
                "ADJ.VN+CASE_INDEF_NOM": "VN",
                "ADJ.VN+NSUFF_FEM_DU_ACC": "VN",
                "ADJ.VN+NSUFF_FEM_SG": "VN",
                "ADJ.VN+NSUFF_FEM_SG+CASE_DEF_ACC": "VN",
                "ADJ.VN+NSUFF_FEM_SG+CASE_DEF_GEN": "VN",
                "ADJ.VN+NSUFF_FEM_SG+CASE_INDEF_ACC": "VN",
                "ADJ.VN+NSUFF_FEM_SG+CASE_INDEF_GEN": "VN",
                "ADJ.VN+NSUFF_FEM_SG+CASE_INDEF_NOM": "VN",
                "ADJ.VN+NSUFF_MASC_DU_ACC": "VN",
                "ADJ.VN+NSUFF_MASC_DU_NOM_POSS": "VN",
                "ADJ.VN+NSUFF_MASC_DU_GEN": "VN",
                "ADJ.VN+NSUFF_MASC_PL_ACC": "VN",
                "ADJ.VN+NSUFF_MASC_PL_ACCGEN": "VN",
                "ADJ.VN+NSUFF_MASC_PL_GEN": "VN",
                "ADJ.VN+NSUFF_MASC_PL_NOM": "VN",
                "DET+ADJ.VN": "VN",
                "DET+ADJ.VN+CASE_DEF_ACC": "VN",
                "DET+ADJ.VN+CASE_DEF_GEN": "VN",
                "DET+ADJ.VN+CASE_DEF_NOM": "VN",
                "DET+ADJ.VN+NSUFF_FEM_DU_ACC": "VN",
                "DET+ADJ.VN+NSUFF_FEM_DU_GEN": "VN",
                "DET+ADJ.VN+NSUFF_FEM_PL+CASE_DEF_GEN": "VN",
                "DET+ADJ.VN+NSUFF_FEM_PL+CASE_DEF_NOM": "VN",
                "DET+ADJ.VN+NSUFF_FEM_SG": "VN",
                "DET+ADJ.VN+NSUFF_FEM_SG+CASE_DEF_ACC": "VN",
                "DET+ADJ.VN+NSUFF_FEM_SG+CASE_DEF_GEN": "VN",
                "DET+ADJ.VN+NSUFF_FEM_SG+CASE_DEF_NOM": "VN",
                "DET+ADJ.VN+NSUFF_MASC_DU_ACC": "VN",
                "DET+ADJ.VN+NSUFF_MASC_DU_GEN": "VN",
                "DET+ADJ.VN+NSUFF_MASC_PL_ACC": "VN",
                "DET+ADJ.VN+NSUFF_MASC_PL_ACCGEN": "VN",
                "DET+ADJ.VN+NSUFF_MASC_PL_GEN": "VN",
                "DET+ADJ.VN+NSUFF_MASC_PL_NOM": "VN",

                # NN
                "ABBREV": "NN",
                "DET+ABBREV": "NN",
                "DET+FOREIGN": "NN",
                "DET+NOUN": "NN",
                "DET+NOUN+CASE_DEF_ACC": "NN",
                "DET+NOUN+CASE_DEF_GEN": "NN",
                "DET+NOUN+CASE_DEF_NOM": "NN",
                "DET+NOUN+NSUFF_FEM_SG": "NN",
                "DET+NOUN+NSUFF_FEM_SG+CASE_DEF_ACC": "NN",
                "DET+NOUN+NSUFF_FEM_SG+CASE_DEF_GEN": "NN",
                "DET+NOUN+NSUFF_FEM_SG+CASE_DEF_NOM": "NN",
                "DIALECT": "NN",
                "DET+DIALECT": "NN",
                "FOREIGN": "NN",
                "GRAMMAR_PROBLEM": "NN",
                "LATIN": "NN",
                "NOUN": "NN",
                "NOUN+CASE_DEF_ACC": "NN",
                "NOUN+CASE_DEF_GEN": "NN",
                "NOUN+CASE_DEF_NOM": "NN",
                "NOUN+CASE_INDEF_ACC": "NN",
                "NOUN+CASE_INDEF_GEN": "NN",
                "NOUN+CASE_INDEF_NOM": "NN",
                "NOUN+NSUFF_FEM_SG": "NN",
                "NOUN+NSUFF_FEM_SG+CASE_DEF_ACC": "NN",
                "NOUN+NSUFF_FEM_SG+CASE_DEF_GEN": "NN",
                "NOUN+NSUFF_FEM_SG+CASE_DEF_NOM": "NN",
                "NOUN+NSUFF_FEM_SG+CASE_INDEF_ACC": "NN",
                "NOUN+NSUFF_FEM_SG+CASE_INDEF_GEN": "NN",
                "NOUN+NSUFF_FEM_SG+CASE_INDEF_NOM": "NN",
                "NOUN+NSUFF_MASC_SG_ACC_INDEF": "NN",
                "TYPO": "NN",
                "DET+TYPO": "NN",

                # NNS
                "DET+NOUN+NSUFF_FEM_DU_ACC": "NNS",
                "DET+NOUN+NSUFF_FEM_DU_ACCGEN": "NNS",
                "DET+NOUN+NSUFF_FEM_DU_GEN": "NNS",
                "DET+NOUN+NSUFF_FEM_DU_GEN_POSS": "NNS",
                "DET+NOUN+NSUFF_FEM_DU_NOM": "NNS",
                "DET+NOUN+NSUFF_FEM_PL": "NNS",
                "DET+NOUN+NSUFF_FEM_PL+CASE_DEF_ACC": "NNS",
                "DET+NOUN+NSUFF_FEM_PL+CASE_DEF_ACCGEN": "NNS",
                "DET+NOUN+NSUFF_FEM_PL+CASE_DEF_GEN": "NNS",
                "DET+NOUN+NSUFF_FEM_PL+CASE_DEF_NOM": "NNS",
                "DET+NOUN+NSUFF_MASC_DU": "NNS",
                "DET+NOUN+NSUFF_MASC_DU_ACC": "NNS",
                "DET+NOUN+NSUFF_MASC_DU_ACCGEN": "NNS",
                "DET+NOUN+NSUFF_MASC_DU_GEN": "NNS",
                "DET+NOUN+NSUFF_MASC_DU_GEN_POSS": "NNS",
                "DET+NOUN+NSUFF_MASC_DU_NOM": "NNS",
                "DET+NOUN+NSUFF_MASC_PL_ACC": "NNS",
                "DET+NOUN+NSUFF_MASC_PL_ACCGEN": "NNS",
                "DET+NOUN+NSUFF_MASC_PL_GEN": "NNS",
                "DET+NOUN+NSUFF_MASC_PL_GEN_POSS": "NNS",
                "DET+NOUN+NSUFF_MASC_PL_NOM": "NNS",
                "NOUN+NSUFF_FEM_DU_ACC": "NNS",
                "NOUN+NSUFF_FEM_DU_ACCGEN": "NNS",
                "NOUN+NSUFF_FEM_DU_ACCGEN_POSS": "NNS",
                "NOUN+NSUFF_FEM_DU_ACC_POSS": "NNS",
                "NOUN+NSUFF_FEM_DU_GEN": "NNS",
                "NOUN+NSUFF_FEM_DU_GEN_POSS": "NNS",
                "NOUN+NSUFF_FEM_DU_NOM": "NNS",
                "NOUN+NSUFF_FEM_DU_NOM_POSS": "NNS",
                "NOUN+NSUFF_FEM_PL": "NNS",
                "NOUN+NSUFF_FEM_PL+CASE_DEF_ACC": "NNS",
                "NOUN+NSUFF_FEM_PL+CASE_DEF_ACCGEN": "NNS",
                "NOUN+NSUFF_FEM_PL+CASE_DEF_GEN": "NNS",
                "NOUN+NSUFF_FEM_PL+CASE_DEF_NOM": "NNS",
                "NOUN+NSUFF_FEM_PL+CASE_INDEF_ACC": "NNS",
                "NOUN+NSUFF_FEM_PL+CASE_INDEF_ACCGEN": "NNS",
                "NOUN+NSUFF_FEM_PL+CASE_INDEF_GEN": "NNS",
                "NOUN+NSUFF_FEM_PL+CASE_INDEF_NOM": "NNS",
                "NOUN+NSUFF_MASC_DU": "NNS",
                "NOUN+NSUFF_MASC_DU_ACC": "NNS",
                "NOUN+NSUFF_MASC_DU_ACC_POSS": "NNS",
                "NOUN+NSUFF_MASC_DU_ACCGEN": "NNS",
                "NOUN+NSUFF_MASC_DU_ACCGEN_POSS": "NNS",
                "NOUN+NSUFF_MASC_DU_GEN": "NNS",
                "NOUN+NSUFF_MASC_DU_GEN_POSS": "NNS",
                "NOUN+NSUFF_MASC_DU_NOM": "NNS",
                "NOUN+NSUFF_MASC_DU_NOM_POSS": "NNS",
                "NOUN+NSUFF_MASC_PL_ACC": "NNS",
                "NOUN+NSUFF_MASC_PL_ACC_POSS": "NNS",
                "NOUN+NSUFF_MASC_PL_ACCGEN": "NNS",
                "NOUN+NSUFF_MASC_PL_ACCGEN_POSS": "NNS",
                "NOUN+NSUFF_MASC_PL_GEN": "NNS",
                "NOUN+NSUFF_MASC_PL_GEN_POSS": "NNS",
                "NOUN+NSUFF_MASC_PL_NOM": "NNS",
                "NOUN+NSUFF_MASC_PL_NOM_POSS": "NNS",

                # NNP
                "DET+NOUN_PROP": "NNP",
                "DET+NOUN_PROP+CASE_DEF_ACC": "NNP",
                "DET+NOUN_PROP+CASE_DEF_GEN": "NNP",
                "DET+NOUN_PROP+CASE_DEF_NOM": "NNP",
                "DET+NOUN_PROP+NSUFF_FEM_SG": "NNP",
                "DET+NOUN_PROP+NSUFF_FEM_SG+CASE_DEF_ACC": "NNP",
                "DET+NOUN_PROP+NSUFF_FEM_SG+CASE_DEF_GEN": "NNP",
                "DET+NOUN_PROP+NSUFF_FEM_SG+CASE_DEF_NOM": "NNP",
                "NOUN_PROP": "NNP",
                "NOUN_PROP+CASE_DEF_ACC": "NNP",
                "NOUN_PROP+CASE_DEF_GEN": "NNP",
                "NOUN_PROP+CASE_DEF_NOM": "NNP",
                "NOUN_PROP+CASE_INDEF_ACC": "NNP",
                "NOUN_PROP+CASE_INDEF_GEN": "NNP",
                "NOUN_PROP+CASE_INDEF_NOM": "NNP",
                "NOUN_PROP+NSUFF_FEM_SG": "NNP",
                "NOUN_PROP+NSUFF_FEM_SG+CASE_DEF_ACC": "NNP",
                "NOUN_PROP+NSUFF_FEM_SG+CASE_DEF_GEN": "NNP",
                "NOUN_PROP+NSUFF_FEM_SG+CASE_DEF_NOM": "NNP",
                "NOUN_PROP+NSUFF_FEM_SG+CASE_INDEF_ACC": "NNP",
                "NOUN_PROP+NSUFF_FEM_SG+CASE_INDEF_GEN": "NNP",
                "NOUN_PROP+NSUFF_FEM_SG+CASE_INDEF_NOM": "NNP",

                # NNPS
                "DET+NOUN_PROP+NSUFF_FEM_DU_GEN": "NNPS",
                "DET+NOUN_PROP+NSUFF_FEM_DU_NOM": "NNPS",
                "DET+NOUN_PROP+NSUFF_FEM_PL": "NNPS",
                "DET+NOUN_PROP+NSUFF_FEM_PL+CASE_DEF_ACCGEN": "NNPS",
                "DET+NOUN_PROP+NSUFF_FEM_PL+CASE_DEF_GEN": "NNPS",
                "DET+NOUN_PROP+NSUFF_MASC_DU_ACCGEN" : "NNPS",
                "DET+NOUN_PROP+NSUFF_MASC_DU_GEN": "NNPS",
                "DET+NOUN_PROP+NSUFF_MASC_PL_ACC": "NNPS",
                "DET+NOUN_PROP+NSUFF_MASC_PL_GEN": "NNPS",
                "DET+NOUN_PROP+NSUFF_MASC_PL_NOM": "NNPS",
                "NOUN_PROP+NSUFF_FEM_PL+CASE_DEF_GEN": "NNPS",
                "NOUN_PROP+NSUFF_FEM_PL+CASE_INDEF_ACCGEN": "NNPS",
                "NOUN_PROP+NSUFF_FEM_PL+CASE_INDEF_GEN": "NNPS",
                "NOUN_PROP+NSUFF_MASC_PL_GEN_POSS": "NNPS",
                "NOUN_PROP+NSUFF_MASC_PL_NOM": "NNPS",

                # JJ
                "ADJ": "JJ",
                "ADJ+CASE_DEF_ACC": "JJ",
                "ADJ+CASE_DEF_GEN": "JJ",
                "ADJ+CASE_DEF_NOM": "JJ",
                "ADJ+CASE_INDEF_ACC": "JJ",
                "ADJ+CASE_INDEF_GEN": "JJ",
                "ADJ+CASE_INDEF_NOM": "JJ",
                "ADJ+NSUFF_FEM_DU_ACC": "JJ",
                "ADJ+NSUFF_FEM_DU_GEN": "JJ",
                "ADJ+NSUFF_FEM_DU_NOM": "JJ",
                "ADJ+NSUFF_FEM_DU_NOM_POSS": "JJ",
                "ADJ+NSUFF_FEM_PL": "JJ",
                "ADJ+NSUFF_FEM_PL+CASE_DEF_ACCGEN": "JJ",
                "ADJ+NSUFF_FEM_PL+CASE_DEF_NOM": "JJ",
                "ADJ+NSUFF_FEM_PL+CASE_INDEF_ACC": "JJ",
                "ADJ+NSUFF_FEM_PL+CASE_INDEF_ACCGEN": "JJ",
                "ADJ+NSUFF_FEM_DU_ACCGEN": "JJ",
                "ADJ+NSUFF_FEM_PL+CASE_INDEF_GEN": "JJ",
                "ADJ+NSUFF_FEM_SG": "JJ",
                "ADJ+NSUFF_FEM_SG+CASE_DEF_ACC": "JJ",
                "ADJ+NSUFF_FEM_SG+CASE_DEF_GEN": "JJ",
                "ADJ+NSUFF_FEM_SG+CASE_DEF_NOM": "JJ",
                "ADJ+NSUFF_FEM_SG+CASE_INDEF_ACC": "JJ",
                "ADJ+NSUFF_FEM_SG+CASE_INDEF_GEN": "JJ",
                "ADJ+NSUFF_FEM_SG+CASE_INDEF_NOM": "JJ",
                "ADJ+NSUFF_MASC_DU_ACC": "JJ",
                "ADJ+NSUFF_MASC_DU_ACCGEN": "JJ",
                "ADJ+NSUFF_MASC_DU_ACCGEN_POSS": "JJ",
                "ADJ+NSUFF_MASC_DU_GEN": "JJ",
                "ADJ+NSUFF_MASC_DU_GEN_POSS": "JJ",
                "ADJ+NSUFF_MASC_DU_NOM": "JJ",
                "ADJ+NSUFF_MASC_DU_NOM_POSS": "JJ",
                "ADJ+NSUFF_MASC_PL_ACC": "JJ",
                "ADJ+NSUFF_MASC_PL_ACC_POSS": "JJ",
                "ADJ+NSUFF_MASC_PL_ACCGEN": "JJ",
                "ADJ+NSUFF_MASC_PL_ACCGEN_POSS": "JJ",
                "ADJ+NSUFF_MASC_PL_GEN": "JJ",
                "ADJ+NSUFF_MASC_PL_GEN_POSS": "JJ",
                "ADJ+NSUFF_MASC_PL_NOM": "JJ",
                "ADJ+NSUFF_MASC_PL_NOM_POSS": "JJ",
                "DET+ADJ": "JJ",
                "DET+ADJ+CASE_DEF_ACC": "JJ",
                "DET+ADJ+CASE_DEF_GEN": "JJ",
                "DET+ADJ+CASE_DEF_NOM": "JJ",
                "DET+ADJ+NSUFF_FEM_DU_ACC": "JJ",
                "DET+ADJ+NSUFF_FEM_DU_ACCGEN": "JJ",
                "DET+ADJ+NSUFF_FEM_DU_ACCGEN_POSS": "JJ",
                "DET+ADJ+NSUFF_FEM_DU_GEN": "JJ",
                "DET+ADJ+NSUFF_FEM_DU_NOM": "JJ",
                "DET+ADJ+NSUFF_FEM_PL+CASE_DEF_ACC": "JJ",
                "DET+ADJ+NSUFF_FEM_PL+CASE_DEF_ACCGEN": "JJ",
                "DET+ADJ+NSUFF_FEM_PL+CASE_DEF_GEN": "JJ",
                "DET+ADJ+NSUFF_FEM_PL+CASE_DEF_NOM": "JJ",
                "DET+ADJ+NSUFF_FEM_SG": "JJ",
                "DET+ADJ+NSUFF_FEM_SG+CASE_DEF_ACC": "JJ",
                "DET+ADJ+NSUFF_FEM_SG+CASE_DEF_GEN": "JJ",
                "DET+ADJ+NSUFF_FEM_SG+CASE_DEF_NOM": "JJ",
                "DET+ADJ+NSUFF_MASC_DU_ACC": "JJ",
                "DET+ADJ+NSUFF_MASC_DU_ACCGEN": "JJ",
                "DET+ADJ+NSUFF_MASC_DU_GEN": "JJ",
                "DET+ADJ+NSUFF_MASC_DU_NOM": "JJ",
                "DET+ADJ+NSUFF_MASC_PL_ACC": "JJ",
                "DET+ADJ+NSUFF_MASC_PL_ACC_POSS": "JJ",
                "DET+ADJ+NSUFF_MASC_PL_ACCGEN": "JJ",
                "DET+ADJ+NSUFF_MASC_PL_ACCGEN_POSS": "JJ",
                "DET+ADJ+NSUFF_MASC_PL_GEN": "JJ",
                "DET+ADJ+NSUFF_MASC_PL_GEN_POSS": "JJ",
                "DET+ADJ+NSUFF_MASC_PL_NOM": "JJ",

                # RB
                "ADV": "RB",
                "ADV+CASE_INDEF_ACC": "RB",

                # CC
                "CONJ": "CC",

                # DT
                "DEM_PRON": "DT",
                "DEM_PRON_F": "DT",
                "DEM_PRON_FD": "DT",
                "DEM_PRON_FS": "DT",
                "DEM_PRON_MD": "DT",
                "DEM_PRON_MP": "DT",
                "DEM_PRON_MS": "DT",
                "DET": "DT",

                # RP
                "CONNEC_PART": "RP",
                "EMPHATIC_PART": "RP",
                "FOCUS_PART": "RP",
                "FUT_PART": "RP",
                "INTERROG_PART": "RP",
                "JUS_PART": "RP",
                "NEG_PART": "RP",
                "PART": "RP",
                "RC_PART": "RP",
                "RESTRIC_PART": "RP",
                "VERB_PART": "RP",
                "VOC_PART": "RP",

                # IN
                "PREP": "IN",
                "SUB_CONJ": "IN",

                # VBP (old present tense tag used for imperfect verbs)
                "FUT+IV1P+IV+IVSUFF_MOOD:I": "VBP",
                "FUT+IV1S+IV+IVSUFF_MOOD:I": "VBP",
                "FUT+IV2MP+IV+IVSUFF_SUBJ:MP_MOOD:I": "VBP",
                "FUT+IV2MS+IV+IVSUFF_MOOD:I": "VBP",
                "FUT+IV3FD+IV+IVSUFF_SUBJ:D_MOOD:I": "VBP",
                "FUT+IV3FS+IV+IVSUFF_MOOD:I": "VBP",
                "FUT+IV3MD+IV+IVSUFF_SUBJ:D_MOOD:I": "VBP",
                "FUT+IV3MP+IV+IVSUFF_SUBJ:MP_MOOD:I": "VBP",
                "FUT+IV3MS+IV+IVSUFF_MOOD:I": "VBP",
                "IV": "VBP",
                "IV+IVSUFF_SUBJ:3FS": "VBP",
                "IV1P+IV": "VBP",
                "IV1P+IV+IVSUFF_MOOD:I": "VBP",
                "IV1P+IV+IVSUFF_MOOD:J": "VBP",
                "IV1P+IV+IVSUFF_MOOD:S": "VBP",
                "IV1S+IV": "VBP",
                "IV1S+IV+IVSUFF_MOOD:I": "VBP",
                "IV1S+IV+IVSUFF_MOOD:J": "VBP",
                "IV1S+IV+IVSUFF_MOOD:S": "VBP",
                "IV2D+IV+IVSUFF_SUBJ:D_MOOD:I": "VBP",
                "IV2D+IV+IVSUFF_SUBJ:D_MOOD:SJ": "VBP",
                "IV2FP+IV+IVSUFF_SUBJ:FP": "VBP",
                "IV2FS+IV+IVSUFF_SUBJ:2FS_MOOD:I": "VBP",
                "IV2FS+IV+IVSUFF_SUBJ:2FS_MOOD:SJ": "VBP",
                "IV2MP+IV+IVSUFF_SUBJ:MP_MOOD:I": "VBP",
                "IV2MP+IV+IVSUFF_SUBJ:MP_MOOD:SJ": "VBP",
                "IV2MS+IV": "VBP",
                "IV2MS+IV+IVSUFF_MOOD:I": "VBP",
                "IV2MS+IV+IVSUFF_MOOD:J": "VBP",
                "IV2MS+IV+IVSUFF_MOOD:S": "VBP",
                "IV3FD+IV+IVSUFF_SUBJ:D_MOOD:I": "VBP",
                "IV3FD+IV+IVSUFF_SUBJ:D_MOOD:SJ": "VBP",
                "IV3FP+IV+IVSUFF_SUBJ:FP": "VBP",
                "IV3FS+IV": "VBP",
                "IV3FS+IV+IVSUFF_MOOD:I": "VBP",
                "IV3FS+IV+IVSUFF_MOOD:J": "VBP",
                "IV3FS+IV+IVSUFF_MOOD:S": "VBP",
                "IV3FS+IV+IVSUFF_SUBJ:3FS": "VBP",
                "IV3MD+IV+IVSUFF_SUBJ:D_MOOD:I": "VBP",
                "IV3MD+IV+IVSUFF_SUBJ:D_MOOD:SJ": "VBP",
                "IV3MP+IV+IVSUFF_MOOD:I": "VBP",
                "IV3MP+IV+IVSUFF_MOOD:S": "VBP",
                "IV3MP+IV+IVSUFF_SUBJ:MP_MOOD:I": "VBP",
                "IV3MP+IV+IVSUFF_SUBJ:MP_MOOD:SJ": "VBP",
                "IV3MS+IV": "VBP",
                "IV3MS+IV+IVSUFF_MOOD:I": "VBP",
                "IV3MS+IV+IVSUFF_MOOD:J": "VBP",
                "IV3MS+IV+IVSUFF_MOOD:S": "VBP",
                "JUS+IV1P+IV+IVSUFF_MOOD:J": "VBP",
                "JUS+IV2FS+IV+IVSUFF_SUBJ:2FS_MOOD:SJ": "VBP",
                "JUS+IV2MS+IV+IVSUFF_MOOD:J": "VBP",
                "JUS+IV3FS+IV+IVSUFF_MOOD:J": "VBP",
                "JUS+IV3MP+IV+IVSUFF_MOOD:J": "VBP",
                "JUS+IV3MP+IV+IVSUFF_SUBJ:MP_MOOD:SJ": "VBP",
                "JUS+IV3MS+IV+IVSUFF_MOOD:J": "VBP",
                "PSEUDO_VERB": "VBP",
                "VERB": "VBP",

                # VBN (passive verb, using old past participle tag)
                "FUT+IV3FS+IV_PASS+IVSUFF_MOOD:I": "VBN",
                "FUT+IV3MD+IV_PASS+IVSUFF_SUBJ:D_MOOD:I": "VBN",
                "FUT+IV3MP+IV_PASS+IVSUFF_SUBJ:MP_MOOD:I": "VBN",
                "FUT+IV3MS+IV_PASS+IVSUFF_MOOD:I": "VBN",
                "FUT+IV3MS+IV_PASS+IVSUFF_MOOD:S": "VBN",
                "IV1P+IV_PASS+IVSUFF_MOOD:I": "VBN",
                "IV1P+IV_PASS+IVSUFF_MOOD:S": "VBN",
                "IV1S+IV_PASS+IVSUFF_MOOD:I": "VBN",
                "IV1S+IV_PASS+IVSUFF_MOOD:S": "VBN",
                "IV2FS+IV_PASS+IVSUFF_MOOD:I": "VBN",
                "IV2FS+IV_PASS+IVSUFF_MOOD:S": "VBN",
                "IV2MS+IV_PASS+IVSUFF_MOOD:I": "VBN",
                "IV2MS+IV_PASS+IVSUFF_MOOD:J": "VBN",
                "IV2MS+IV_PASS+IVSUFF_MOOD:S": "VBN",
                "IV3FS+IV_PASS": "VBN",
                "IV3FS+IV_PASS+IVSUFF_MOOD:I": "VBN",
                "IV3FS+IV_PASS+IVSUFF_MOOD:J": "VBN",
                "IV3FS+IV_PASS+IVSUFF_MOOD:S": "VBN",
                "IV3MD+IV_PASS+IVSUFF_SUBJ:D_MOOD:I": "VBN",
                "IV3MD+IV_PASS+IVSUFF_SUBJ:D_MOOD:SJ": "VBN",
                "IV3MP+IV_PASS+IVSUFF_MOOD:I": "VBN",
                "IV3MP+IV_PASS+IVSUFF_SUBJ:MP_MOOD:I": "VBN",
                "IV3MP+IV_PASS+IVSUFF_SUBJ:MP_MOOD:SJ": "VBN",
                "IV3MS+IV_PASS": "VBN",
                "IV3MS+IV_PASS+IVSUFF_MOOD:I": "VBN",
                "IV3MS+IV_PASS+IVSUFF_MOOD:J": "VBN",
                "IV3MS+IV_PASS+IVSUFF_MOOD:S": "VBN",
                "IV_PASS": "VBN",
                "JUS+IV3MS+IV_PASS+IVSUFF_MOOD:J": "VBN",
                "PV_PASS": "VBN",
                "PV_PASS+PVSUFF_SUBJ:1P": "VBN",
                "PV_PASS+PVSUFF_SUBJ:1S": "VBN",
                "PV_PASS+PVSUFF_SUBJ:3FD": "VBN",
                "PV_PASS+PVSUFF_SUBJ:3FP": "VBN",
                "PV_PASS+PVSUFF_SUBJ:3FS": "VBN",
                "PV_PASS+PVSUFF_SUBJ:3MD": "VBN",
                "PV_PASS+PVSUFF_SUBJ:3MP": "VBN",
                "PV_PASS+PVSUFF_SUBJ:3MS": "VBN",

                # UH
                "INTERJ": "UH",

                # PRP
                "CVSUFF_DO:1P": "PRP",
                "CVSUFF_DO:1S": "PRP",
                "CVSUFF_DO:3FS": "PRP",
                "CVSUFF_DO:3MP": "PRP",
                "CVSUFF_DO:3MS": "PRP",
                "IVSUFF_DO:1P": "PRP",
                "IVSUFF_DO:1S": "PRP",
                "IVSUFF_DO:2FS": "PRP",
                "IVSUFF_DO:2MP": "PRP",
                "IVSUFF_DO:2MS": "PRP",
                "IVSUFF_DO:3D": "PRP",
                "IVSUFF_DO:3FS": "PRP",
                "IVSUFF_DO:3MP": "PRP",
                "IVSUFF_DO:3MS": "PRP",
                "PRON": "PRP",
                "PRON_1P": "PRP",
                "PRON_1S": "PRP",
                "PRON_2D": "PRP",
                "PRON_2FP": "PRP",
                "PRON_2FS": "PRP",
                "PRON_2MP": "PRP",
                "PRON_2MS": "PRP",
                "PRON_3D": "PRP",
                "PRON_3FP": "PRP",
                "PRON_3FS": "PRP",
                "PRON_3MP": "PRP",
                "PRON_3MS": "PRP",
                "PVSUFF_DO:1P": "PRP",
                "PVSUFF_DO:1S": "PRP",
                "PVSUFF_DO:2FS": "PRP",
                "PVSUFF_DO:2MP": "PRP",
                "PVSUFF_DO:2MS": "PRP",
                "PVSUFF_DO:3D": "PRP",
                "PVSUFF_DO:3FS": "PRP",
                "PVSUFF_DO:3MP": "PRP",
                "PVSUFF_DO:3MS": "PRP",

                # PRP$
                "POSS_PRON_1P": "PRP$",
                "POSS_PRON_1S": "PRP$",
                "POSS_PRON_2FP": "PRP$",
                "POSS_PRON_2FS": "PRP$",
                "POSS_PRON_2MP": "PRP$",
                "POSS_PRON_2MS": "PRP$",
                "POSS_PRON_3D": "PRP$",
                "POSS_PRON_3FP": "PRP$",
                "POSS_PRON_3FS": "PRP$",
                "POSS_PRON_3MP": "PRP$",
                "POSS_PRON_3MS": "PRP$",

                # WRB
                "INTERROG_ADV": "WRB",
                "REL_ADV": "WRB",

                # WP
                "EXCLAM_PRON": "WP",
                "INTERROG_PRON": "WP",
                "INTERROG_PRON+CASE_DEF_GEN": "WP",
                "INTERROG_PRON+CASE_DEF_NOM": "WP",
                "INTERROG_PRON+NSUFF_FEM_SG+CASE_DEF_GEN": "WP",
                "REL_PRON": "WP",
                "REL_PRON+CASE_DEF_ACC": "WP",
                "REL_PRON+CASE_DEF_GEN": "WP",
                "REL_PRON+CASE_DEF_NOM": "WP",
                "REL_PRON+CASE_INDEF_ACC": "WP",
                "REL_PRON+NSUFF_FEM_SG+CASE_DEF_GEN": "WP",
                "REL_PRON+NSUFF_MASC_SG_ACC_INDEF": "WP",

                # VBD
                "PV": "VBD",
                "PV+PVSUFF_3MS": "VBD",
                "PV+PVSUFF_SUBJ:1P": "VBD",
                "PV+PVSUFF_SUBJ:1S": "VBD",
                "PV+PVSUFF_SUBJ:2FS": "VBD",
                "PV+PVSUFF_SUBJ:2MP": "VBD",
                "PV+PVSUFF_SUBJ:2MS": "VBD",
                "PV+PVSUFF_SUBJ:3FD": "VBD",
                "PV+PVSUFF_SUBJ:3FP": "VBD",
                "PV+PVSUFF_SUBJ:3FS": "VBD",
                "PV+PVSUFF_SUBJ:3MD": "VBD",
                "PV+PVSUFF_SUBJ:3MP": "VBD",
                "PV+PVSUFF_SUBJ:3MS": "VBD",

                # VB (used for imperative here)
                "CV": "VB",
                "CV+CVSUFF_SUBJ:2FS": "VB",
                "CV+CVSUFF_SUBJ:2MP": "VB",
                "CV+CVSUFF_SUBJ:2MS": "VB",

                # CD
                "DET+NOUN_NUM+CASE_DEF_ACC": "CD",
                "DET+NOUN_NUM+CASE_DEF_GEN": "CD",
                "DET+NOUN_NUM+NSUFF_FEM_SG+CASE_DEF_ACC": "CD",
                "DET+NOUN_NUM+NSUFF_FEM_SG+CASE_DEF_GEN": "CD",
                "DET+NOUN_NUM+NSUFF_FEM_SG+CASE_DEF_NOM": "CD",
                "DET+NOUN_NUM+NSUFF_MASC_DU_NOM": "CD",
                "DET+NOUN_NUM+NSUFF_MASC_PL_ACC": "CD",
                "DET+NOUN_NUM+NSUFF_MASC_PL_GEN": "CD",
                "DET+NOUN_NUM+NSUFF_MASC_PL_NOM": "CD",
                "NOUN_NUM": "CD",
                "NOUN_NUM+CASE_DEF_ACC": "CD",
                "NOUN_NUM+CASE_DEF_GEN": "CD",
                "NOUN_NUM+CASE_DEF_NOM": "CD",
                "NOUN_NUM+CASE_INDEF_ACC": "CD",
                "NOUN_NUM+CASE_INDEF_GEN": "CD",
                "NOUN_NUM+CASE_INDEF_NOM": "CD",
                "NOUN_NUM+NSUFF_FEM_DU_ACC": "CD",
                "NOUN_NUM+NSUFF_FEM_DU_GEN": "CD",
                "NOUN_NUM+NSUFF_FEM_DU_GEN_POSS": "CD",
                "NOUN_NUM+NSUFF_FEM_PL+CASE_DEF_ACC": "CD",
                "NOUN_NUM+NSUFF_FEM_PL+CASE_DEF_ACCGEN": "CD",
                "NOUN_NUM+NSUFF_FEM_PL+CASE_DEF_GEN": "CD",
                "NOUN_NUM+NSUFF_FEM_PL+CASE_DEF_NOM": "CD",
                "NOUN_NUM+NSUFF_FEM_PL+CASE_INDEF_NOM": "CD",
                "NOUN_NUM+NSUFF_FEM_SG+CASE_DEF_ACC": "CD",
                "NOUN_NUM+NSUFF_FEM_SG+CASE_DEF_GEN": "CD",
                "NOUN_NUM+NSUFF_FEM_SG+CASE_DEF_NOM": "CD",
                "NOUN_NUM+NSUFF_FEM_SG+CASE_INDEF_ACC": "CD",
                "NOUN_NUM+NSUFF_FEM_SG+CASE_INDEF_GEN": "CD",
                "NOUN_NUM+NSUFF_FEM_SG+CASE_INDEF_NOM": "CD",
                "NOUN_NUM+NSUFF_MASC_DU_ACC_POSS": "CD",
                "NOUN_NUM+NSUFF_MASC_DU_ACCGEN_POSS" : "CD",
                "NOUN_NUM+NSUFF_MASC_DU_GEN": "CD",
                "NOUN_NUM+NSUFF_MASC_DU_GEN_POSS": "CD",
                "NOUN_NUM+NSUFF_MASC_DU_NOM": "CD",
                "NOUN_NUM+NSUFF_MASC_DU_NOM_POSS": "CD",
                "NOUN_NUM+NSUFF_MASC_PL_ACC": "CD",
                "NOUN_NUM+NSUFF_MASC_PL_ACC_POSS": "CD",
                "NOUN_NUM+NSUFF_MASC_PL_ACCGEN": "CD",
                "NOUN_NUM+NSUFF_MASC_PL_GEN": "CD",
                "NOUN_NUM+NSUFF_MASC_PL_NOM": "CD",

                # arabic punctuation

                "PUNC": None,
                "NUMERIC_COMMA": None # to be treated in the same way as the PUNC tag

                }

    sql_table_name = "pos_type"
    sql_create_statement = \
"""
create table pos_type
(
  id varchar(255) not null collate utf8_bin primary key
)
default character set utf8;
"""


    sql_insert_statement = \
"""insert into pos_type
(
  id
) values (%s)
"""

# this is the trace type of a node in the tree
class syntactic_link_type:
    allowed = ["*T*",   # trace of A' movement
               "*",     # trace of A movement
               "*U*",
               "*?*",   # other unknown empty categories
               "*NOT*",
               "*RNR*", # right node raising
               "*ICH*", # insert constituent here
               "*EXP*",
               "*PPA*", # permanent predictable ambiguity
               "0",     # null complementizer (we turn '*0*' into '0')
               "*PRO*", # used in control structures
               "*pro*", # dropped argument
               "*OP*"   # operator
               ]

    # If we have something like:
    #
    #    (NP-1 (-NONE- XXX))
    #    ...
    #    (-NONE- YYY-1)
    #
    # then XXX is acting as a target and YYY is acting as an origin.
    # Some trace forms are only allowed to function as one or the
    # other.  If they are used incorrectly, this is an error we need
    # to fix.
    #

    never_origin = ["*U*", "*?*", "0", "*OP*"]
    never_target = ["*T*", "*U*", "*?*", "*RNR*", "*ICH*", "*EXP*", "*PPA*"]

    references = [["tree", "syntactic_link_type"]]

    sql_table_name = "syntactic_link_type"
    sql_create_statement = \
"""
create table syntactic_link_type
(
  id varchar(255) not null collate utf8_bin primary key
)
default character set utf8;
"""


    sql_insert_statement = \
"""insert into syntactic_link_type
(
  id
) values (%s)
"""


# this is the function tag type associated with a node in the tree
class function_tag_type:
    references = [["compound_function_tag", "type"]]

    allowed = [ "ADV",
                "BNF", "CLF", "CLR", "DIR", "DTV", "ETC", "EXT",
                "HLN", "IMP", "LGS", "LOC", "MNR", "NOM", "OBJ",
                "PRD", "PRP", "PUT", "SBJ", "SEZ", "TMP", "TPC",
                "TTL", "UNF", "VOC",

                # additional ones for chinese (unsorted)
                "Q",    # question
                "IO",   # indirect object
                "FOC",  # focus
                "CND",  # condition
                "IJ",   # interjective
                "APP",  # appositive
                "PN",   # proper name
                "SHORT",# short form
                "WH"    # wh-phrase
                ]

    sql_table_name = "function_tag_type"
    sql_create_statement = \
"""
create table function_tag_type
(
  id varchar(255) not null collate utf8_bin primary key
)
default character set utf8;
"""


    sql_insert_statement = \
"""insert into function_tag_type
(
  id
) values (%s)
"""



class lemma_type(on.corpora.abstract_open_type_table):
    type_hash = defaultdict(int)

    sql_table_name = "lemma_type"
    sql_create_statement = \
"""
create table lemma_type
(
  id varchar(255) not null collate utf8_bin primary key
)
default character set utf8;
"""


    sql_insert_statement = \
"""insert into lemma_type
(
  id
)
values (%s)
"""

# sometimes there can be more than one function tag associated
# with a node.  This class keeps track of the individual types
# in a compound function type
class compound_function_tag:
    def __init__(self, a_function_tag_string, subtree):
        self.id = "%s@%s" % (a_function_tag_string, subtree.id)
        self.type = a_function_tag_string
        self.function_tag_types = a_function_tag_string.split("-")

        for a_function_tag_type in self:
            if a_function_tag_type not in function_tag_type.allowed:
                subtree.get_root().bad_data("invalid function tag",
                                            ["function tag type", a_function_tag_type])

    sql_table_name = "compound_function_tag"
    sql_create_statement = \
"""
create table compound_function_tag
(
  id varchar(255) not null,
  type varchar(255) not null
)
default character set utf8;
"""

    sql_insert_statement = \
"""insert into compound_function_tag
(
  id,
  type
) values (%s, %s)
"""

    def __getitem__(self, idx):
        return self.function_tag_types[idx]

    # this is the method that writes the function tag types to the database
    def write_to_db(self, cursor):

        # insert the value in the table
        cursor.executemany("%s" % (self.__class__.sql_insert_statement),
                           [(self.id, a_function_tag_type)
                            for a_function_tag_type in self])



# the class that keeps track of the trace and sem-link chains in the tree
class syntactic_link:
    """ Links between tree nodes

    Example:

      .. code-block:: scheme

         (TOP (SBARQ (WHNP-1 (WHADJP (WRB How)
                                     (JJ many))
                             (NNS ups)
                             (CC and)
                             (NNS downs))
                     (SQ (MD can)
                         (NP-SBJ (CD one)
                                 (NN woman))
                         (VP (VB have)
                             (NP (-NONE- *T*-1))))
                     (. /?)))

      The node ``(-NONE- *T*-1)`` is a syntactic link back to ``(WHNP-1 (WHADJP (WRB How) (JJ many)) (NNS ups) (CC and) (NNS downs))``.

    Links have an identity subtree ``(How many ups and downs)`` and a
    reference subtree ``(-NONE- *T*-1)`` and are generally thought of
    as a link from the reference back to the identity.

    """

    def __init__(self, type, word, reference_subtree_id, identity_subtree_id):
        self.type = type
        self.word = word  # this is the actual word (usually a trace word or a relative pronoun) that points to the actual entity
        self.reference_subtree_id = reference_subtree_id
        self.identity_subtree_id = identity_subtree_id

        if(self.word != None and len(self.word.split()) != 1):
            on.common.log.warning("the slink reference is a multi-word string '%s'.  this should probably not happen" % (word))

        if(self.word != None):
            self.id = "%s@%s@%s" % (self.type, re.sub(" ", "<space>", self.word), self.reference_subtree_id)
        else:
            self.id = "%s@%s@%s" % (self.type, self.word, self.reference_subtree_id)



    sql_table_name = "syntactic_link"

    # sql create statement for the syntactic_link table
    sql_create_statement = \
"""
create table syntactic_link
(
  id varchar(255) not null,
  type varchar(255) not null,
  word  varchar(255),
  reference_subtree_id  varchar(255) not null,
  identity_subtree_id  varchar(255) not null,
  foreign key (reference_subtree_id) references tree.id,
  foreign key (identity_subtree_id) references tree.id
)
default character set utf8;
"""



    # sql insert statement for the syntactic_link table
    sql_insert_statement = \
"""
insert into syntactic_link
(
  id,
  type,
  word,
  reference_subtree_id,
  identity_subtree_id
) values(%s, %s, %s, %s, %s)
"""

    def write_to_db(self, cursor):

        cursor.executemany("%s" % (self.__class__.sql_insert_statement), [(
            self.id, self.type, self.word, self.reference_subtree_id, self.identity_subtree_id)])








class lemma:
    """ arabic trees have extra lemma information """

    def __init__(self, input_string, b_transliteration, comment, index, offset, unvocalized_string,
                 vocalized_string, vocalized_input, pos, gloss, lemma, coarse_sense, leaf_id):

        self.input_string = input_string
        self.b_transliteration = b_transliteration
        self.comment = comment
        self.index = index
        self.offset = offset
        self.unvocalized_string = unvocalized_string
        self.vocalized_string = vocalized_string
        self.vocalized_input = vocalized_input
        self.pos = pos
        self.gloss = gloss
        self.lemma = lemma
        self.coarse_sense = coarse_sense
        self.leaf_id = leaf_id

        self.id = "%s@%s" % (self.lemma, self.leaf_id)

    sql_table_name = "lemma"

    def __repr__(self):
        return "\n".join(["lemma instance:",
                          "  input_string: " + self.input_string,
                          "  vocalized_input: " + self.vocalized_input,
                          "  unvocalized_string: " + self.unvocalized_string,
                          "  vocalized_string: " + self.vocalized_string,
                          "  gloss: " + self.gloss,
                          "  index: %s" % self.index,
                          "  offset: %s" % self.offset])

    def __str__(self):
        tr = ["INPUT STRING:%s" % self.input_string,
              "    IS_TRANS:%s" % self.b_transliteration,
              "     COMMENT:%s" % self.comment,
              "       INDEX:%s" % self.index,
              "     OFFSETS:%s" % self.offset,
              " UNVOCALIZED:%s" % self.unvocalized_string,
              "   VOCALIZED:%s" % self.vocalized_string,
              "  VOC_STRING:%s" % self.vocalized_input,
              "         POS:%s" % self.pos,
              "       GLOSS:%s" % self.gloss]

        if self.lemma != "lemma_not_set":
            if self.coarse_sense:
                lemma_str = "%s_%s" % (self.lemma, self.coarse_sense)
            else:
                lemma_str = self.lemma

            tr.append("       LEMMA: [%s]" % lemma_str)

        return "\n".join(tr)


    @staticmethod
    def from_db(a_leaf_id, a_cursor):
        a_cursor.execute("SELECT * FROM lemma WHERE leaf_id = '%s'" % a_leaf_id)
        rows = a_cursor.fetchall()

        if not rows:
            return None

        if len(rows) != 1:
            assert all(row["lemma"] == rows[0]["lemma"] for row in rows), \
                   "\n".join(", ".join(": ".join(a) for a in row.iteritems()) for row in rows)

        r = rows[0]

        return lemma(r["input_string"],
                     r["b_transliteration"],
                     r["comment"],
                     r["lemma_index"],
                     r["lemma_offset"],
                     r["unvocalized_string"],
                     r["vocalized_string"],
                     r["vocalized_input"],
                     r["pos"],
                     r["gloss"],
                     r["lemma"],
                     r["coarse_sense"],
                     r["leaf_id"])

    # sql create statement for the syntactic_link table
    sql_create_statement = \
"""
create table lemma
(
  id varchar(255) not null,
  input_string varchar(255),
  b_transliteration varchar(255),
  comment varchar(255),
  lemma_index varchar(255),
  lemma_offset varchar(255),
  unvocalized_string varchar(255),
  vocalized_string varchar(255),
  vocalized_input varchar(255),
  pos varchar(255),
  gloss varchar(255),
  lemma varchar(255),
  coarse_sense varchar(16),
  leaf_id varchar(255),
  foreign key (leaf_id) references tree.id
)
default character set utf8;
"""


    # sql insert statement for the syntactic_link table
    sql_insert_statement = \
"""
insert into lemma
(
  id,
  input_string,
  b_transliteration,
  comment,
  lemma_index,
  lemma_offset,
  unvocalized_string,
  vocalized_string,
  vocalized_input,
  pos,
  gloss,
  lemma,
  coarse_sense,
  leaf_id
) values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

    def write_to_db(self, cursor):
        data = [(self.id, self.input_string, self.b_transliteration, self.comment, self.index,
                 self.offset, self.unvocalized_string, self.vocalized_string, self.vocalized_input,
                 self.pos, self.gloss, self.lemma, self.coarse_sense, self.leaf_id)]

        cursor.executemany("%s" % (self.__class__.sql_insert_statement), data)





class tree(object):
    """ root trees, internal nodes, and leaves are all trees.

    Contained by: :class:`tree_document` if a root tree, :class:`tree` otherwise
    Contains: None if a leaf, :class:`tree` otherwise

    Attributes:

        Always available:

            .. attribute:: parent

              The parent of this tree.  If None then we are the root

            .. attribute:: lemma

              Applicable only to Arabic leaves.  The morphological lemma of
              the word as a string.  In Chinese the word is the lemma, so use
              :attr:`word`.  In English the best you can do is use either the
              :attr:`~on.corpora.sense.on_sense.lemma` attribute of
              :attr:`on_sense` or the
              :attr:`~on.corpora.proposition.proposition.lemma` attribute of
              :attr:`proposition` .

              See Also: :attr:`lemma_object`

            .. attribute:: lemma_object

              Applicable only to Arabic leaves.  There is a lot more
              information in the ``.lemma`` file for each leaf than just the
              lemma string, so if available a :class:`lemma` instance is here.

            .. attribute:: word

              Applicable only to leaves.  The word of text corresponding to the
              leaf.  To extract all the words for a tree, see
              :meth:`get_word_string` .

              For arabic, the word of a tree is always the vocalized
              unicode representation.  For other representations, see
              the :meth:`get_word` method.

            .. attribute:: tag

              Every node in the tree has a :attr:`tag`, which represents part
              of speech, phrase type, or function type information.  For
              example, the leaf ``(NNS cabbages)`` has a tag of ``NNS`` while
              the subtree ``(NP (NNS cabbages))`` has a tag of ``NP``.

            .. attribute:: children

              A list of the child nodes of this tree.  For leaves this will be
              the empty list.

            .. attribute:: reference_leaves

              A list of trace leaves in this tree that point to this subtree

            .. attribute:: identity_subtree

              The subtree in this tree that this trace leaf points to

        Available only after enrichment:


            The following attributes represent the annotation.  They are set
            during the enrichment process, which happens automatically unless
            you are invoking things manually at a low level.  You must, of
            course, specify that a bank is to be loaded for its annotations to
            be available.  For example, if the configuration
            variable ``corpora.banks`` is set to ``"parse sense"``, then leaves
            will have :attr:`on_sense` attributes but not :attr:`proposition`
            attributes.

            Each annotation variable specifies it's level, the bank that sets
            it, and the class whose instance it is set to.  Leaf level
            annotation applies only to leaves, tree level annotation only to
            sentences, and subtree annotation to any subtree in between,
            including leaves.

            Order is not significant in any of the lists.

            .. attribute:: on_sense

              Leaf level, sense bank, :class:`~on.corpora.sense.on_sense`

            .. attribute:: proposition

              Subtree level, proposition bank, :class:`~on.corpora.proposition.proposition`

              This is attached to the same subtree as the primary predicate
              node of the proposition.

            .. attribute:: predicate_node_list

              Subtree level, proposition bank, list of
              :class:`~on.corpora.proposition.predicate_node`

            .. attribute:: argument_node_list

              Subtree level, proposition bank, list of
              :class:`~on.corpora.proposition.argument_node`

            .. attribute:: link_node_list

              Subtree level, proposition bank, list of
              :class:`~on.corpora.proposition.link_node`

            .. attribute:: named_entity

              Subtree level, name bank, :class:`~on.corpora.name.name_entity`

            .. attribute:: start_named_entity_list

              Leaf level, name bank, list of :class:`~on.corpora.name.name_entity`

              Name entities whose initial word is this leaf.

            .. attribute:: end_named_entity_list

              Leaf level, name bank, list of :class:`~on.corpora.name.name_entity`

              Name entities whose final word is this leaf.

            .. attribute:: coreference_link

              Subtree level, coreference bank,
              :class:`~on.corpora.coreference.coreference_link`

            .. attribute:: coreference_chain

              Subtree level, coreference bank,
              :class:`~on.corpora.coreference.coreference_chain`

              The coreference chain that :attr:`coreference_link` belongs to.

            .. attribute:: start_coreference_link_list

              Leaf level, coreference bank, list of
              :class:`~on.corpora.coreference.coreference_link`

              Coreference links whose initial word is this leaf.

            .. attribute:: end_coreference_link_list

              Leaf level, coreference bank, list of
              :class:`~on.corpora.coreference.coreference_link`

              Coreference links whose final word is this leaf.


            .. attribute:: coref_section

              Tree level, coreference bank, ``string``

              The Broadcast Conversation documents, because they are
              very long, were divided into sections for coreference
              annotation.  We tried to break them up at natural
              places, those where the show changed topic, to minimize
              the chance of cross-section coreference.  The annotators
              then did standard coreference annotation on each section
              separately as if it were its own document.  Post
              annotation, we merged all sections into one ``.coref``
              file, with each section as a ``TEXT`` span.  So you can
              have a pair of references to John Smith in section 1 and
              another pair of references in section 2, but they form
              two separate chains.  That is, every coreference chain
              is within only one coreference section.

            .. attribute:: translations

              Tree level, parallel_bank, list of :class:`~on.corpora.tree.tree`

              Trees (in other subcorpora) that are translations of this tree

            .. attribute:: originals

              Tree level, parallel_bank, list of :class:`~on.corpora.tree.tree`

              Trees (in other subcorpora) that are originals of this tree

            .. attribute:: speaker_sentence

              Tree level, speaker bank,
              :class:`~on.corpora.speaker.speaker_sentence`

    Methods:

        .. automethod:: is_noun
        .. automethod:: is_verb
        .. automethod:: is_aux
        .. automethod:: is_leaf
        .. automethod:: is_trace
        .. automethod:: is_root
        .. automethod:: is_punct
        .. automethod:: is_conj
        .. automethod:: is_trace_indexed
        .. automethod:: is_trace_origin
        .. automethod:: get_root
        .. automethod:: get_subtree
        .. automethod:: get_leaf_by_word_index
        .. automethod:: get_leaf_by_token_index
        .. automethod:: get_subtree_by_span
        .. automethod:: pretty_print
        .. automethod:: get_word
        .. automethod:: get_word_string
        .. automethod:: get_trace_adjusted_word_string
        .. automethod:: get_plain_sentence
        .. automethod:: pointer
        .. automethod:: fix_trace_index_locations
        .. automethod:: get_sentence_index
        .. automethod:: get_word_index
        .. automethod:: get_token_index
        .. automethod:: get_height
        .. automethod:: leaves
        .. automethod:: subtrees
        .. automethod:: __getitem__
        .. automethod:: get_other_leaf


    """

    EMPTY_NODE_MATCHER = re.compile(r"""\(         # open paren
                                        [^()\s]+   # contents
                                        \)         # close paren""", re.VERBOSE)

    LEAF_NODE_MATCHER = re.compile(r"""\(          # open paren
                                       ([^()\s]+)  # tag
                                       \s+         # whitespace
                                       ([^()\s]+)  # tagged word
                                       \)\s*       # close paren""", re.VERBOSE)

    NODE_START_MATCHER = re.compile(r"""\(         # open paren
                                        ([^()\s]+) # tag
                                        \s+        # whitespace""", re.VERBOSE)

    NODE_END_MATCHER = re.compile(r"""\)\s*        # close paren""", re.VERBOSE)

    TAG_INDEX_MATCHER = re.compile(".*(-\d*)")

    CODE_CLEANER = re.compile(r"(<|-LAB-)[^:>]*:[^:>]*:([^:>]*)(:[^:>]*)?(>|-RAB-)")


    # moved from inside the process traces method
    PT_identity_index_re = re.compile("-(\d+)$")
    PT_gapping_index_re = re.compile("=(\d+)$")
    PT_all_indexes_re = re.compile("([-=]\d+)+$")

    PT_dash_re = re.compile("-")
    PT_phrase_function_tags_re = re.compile("^(.*?)(?:-(.*))?$")
    PT_trace_type_and_reference_index_re = re.compile("(.*\*)(?:-(\d+))?$")








    def __init__(self, tag, word=None, document_tag="gold"):

        self.tag = tag       # the tag associated with each node and leaf in the treebank tree
                             # this is usually a composite of phrase type, function tag,
                             # trace index, etc., joined together by a "-"


        #normal python should be faster than REs
        # fix DATE tags in Serif parse trees
        if self.tag == "DATE":
            self.tag = "NP"
        self.tag = self.tag.replace("DATE-", "")


        self.word = word     # value of the word associated with the root of this tree and is
                             # only valid for leaves, for all non leaves this is None

        self.start = None    # node span start position
        self.end = None      # Node span end position (exclusive)

        self.document_word_index = -1 # this is the index of the word in the entire document (-1 is for traces and such which do not occur in the original document)


        self.coref_section = -1 # external doc

        self.start_character = None
        self.end_character = None

        self.parent = None # external doc

        self.id = None       # the (sub)tree id.  it is defined as start_word_index,height
                             # tuple as in propbank

        self.document_id = None   # document id, initially None

        self.children = []   # list of child (sub)trees

        self.lemma = None # external doc


        self.lemma_object = None    # lemma object of the word (when available, else None)


        self.subtree_ids = [] # list of all the subtree ids

        self.child_index = None  # index of the child -- from left to right -- of its parent
                                 # it is necessary to construct the tree from database tables.

        self.compound_function_tag = [] # the function tag which can represent multiple pieces of information
                                          # concatenated with a "-"

        self.part_of_speech = None # part of speech of a node

        self.phrase_type = None # phrase type of the node

        self.sentence_id = None

        self.paragraph_id = None

        self.headline_flag = None

        self.syntactic_links = [] # this attribute is only valid for a top-level tree, and lists the syntactic
                                  # links in the tree

        self.identity_index = None # the number with which a node is identified in the process of pointing to it

        self.reference_index = None # indicates the identity index of the node of which this trace is trace of

        # these two python links replace the old identity_index2tree_id_hash and reference_index2tree_id_hash
        self.reference_leaves = []
        self.identity_subtree = None

        self.document_tag = document_tag # the tag of the containing tree document

        self.named_entity = None
        self.start_named_entity_list = [] # named entities starting at this node
        self.end_named_entity_list = []   # named entities ending with this node

        self.coreference_link = None
        self.start_coreference_link_list = [] # coreference links starting at this node
        self.end_coreference_link_list = []   # coreference links ending with this node

        self.proposition = None # proposition object is attached to the subtree of the primary predicate
        self.predicate_node_list = [] # subtrees can be shared by arguments of different propositions
        self.argument_node_list = []
        self.link_node_list = []

        self.on_sense = None

        # translations and originals are only populated if the
        # translation appears before the original in corpus.load.  For
        # example:
        #
        #   corpus.load=chinese-bc-cnn,english-bc-cnn corpus.banks=parse,parallel
        #
        self.translations = [] # will contain trees that are translations of this tree
        self.originals = []   # will contain the originals for this tree

        self.speaker_sentence = None

        self.language = None

        self._leaves = None   # used by method leaves
        self._tokens = None   # used by method tokens
        self._subtrees = None # used by method subtrees

    def _get_speaker_sentence(self):
        return self.get_root()._speaker_sentence

    def _set_speaker_sentence(self, val):
        if not self.is_root():
            raise Exception("You can only set the speaker sentence for root trees")
        self._speaker_sentence = val

    speaker_sentence = property(_get_speaker_sentence, _set_speaker_sentence)

    def _incomparable(self, other):
        return type(self) != type(other) or not self.is_leaf() or not other.is_leaf() or self.document_id != other.document_id

    def __cmp__(self, other):
        if self._incomparable(other):
            if self is other:
                return 0
            return -1 # this means nonleaf_a < nonleaf_b   *and*   nonleaf_b < nonleaf_a.  oh well.

        return cmp((self.get_sentence_index(), self.get_token_index()),
                   (other.get_sentence_index(), other.get_token_index()))


    @property
    def name_type(self):
        if self.named_entity:
            return self.named_entity.type
        return None

    @property
    def coreference_chain(self):
        if self.coreference_link:
            return self.coreference_link.coreference_chain
        return None

    @property
    def coreference_chain_id(self):
        if self.coreference_chain:
            return self.coreference_chain.id
        return None

    @property
    def coreference_link_id(self):
        if self.coreference_link:
            return self.coreference_link.id
        return None

    def _raw_trace_type(self):
        return self.word.replace("=", "-").split("-")[0].replace("*0*", "0")

    @property
    def trace_type(self):
        if not self.is_trace():
            return None
        return self._raw_trace_type()


    @property
    def subtrees_list(self):
        return list(self.subtrees())

    def bad_data(self, reason, *args, **kwargs):
        # only allowed keyword argument is pretty_print

        if "pretty_print" not in kwargs or kwargs["pretty_print"]:
            n_args = args + (["tree", self.pretty_print()], )
        else:
            n_args = args
        assert not kwargs or kwargs.keys() == ["pretty_print"]


        lang = self.get_root().language
        if not lang:
            lang = self.get_root().id.split("@")[-2]

        document_id = self.document_id
        if not document_id and self.id and "@" in self.id:
            document_id = self.id.split("@", 1)[-1]

        document_tag = self.get_root().document_tag

        try:
            base_path = document_id.split("@")[0]
            language = {"en":"english", "ch":"chinese", "ar":"arabic"}[lang]
            repo_path = "on/%s/annotations/parse/%s.%s_parse" % (language, base_path, document_tag)
        except Exception:
            repo_path = "Resolution failed -- this is bad"

        reason = "%s__%s" % (document_tag, reason)


    # helper function used by the method that generates a tree from
    # its parenthesized representation
    @classmethod
    def from_string_helper(cls, syntactic_parse, word_count, document_tag="gold"):

        leaf_match = cls.LEAF_NODE_MATCHER.match(syntactic_parse)
        if leaf_match:

            leaf = tree(leaf_match.group(1), leaf_match.group(2), document_tag=document_tag)
            leaf.start = word_count
            leaf.end = word_count + 1
            return leaf, syntactic_parse[leaf_match.end():], leaf.end

        try:
            node_start_match = cls.NODE_START_MATCHER.match(syntactic_parse)
            result = tree(node_start_match.group(1),document_tag=document_tag)  # create a tree with the required non-terminal tag, and a default word value of None, since none is specified here
        except Exception:
            print syntactic_parse
            print len(syntactic_parse)
            print tree.pretty_print_tree_string(syntactic_parse)
            raise

        result.start = word_count
        remainder = syntactic_parse[node_start_match.end():]

        node_end_match = cls.NODE_END_MATCHER.match(remainder)
        while not node_end_match:
            try:
                child, remainder, word_count = cls.from_string_helper(remainder, word_count)
            except Exception:
                print "\nparent:", syntactic_parse
                raise
            child.parent = result
            result.children.append(child)
            node_end_match = cls.NODE_END_MATCHER.match(remainder)
        result.end = word_count

        return result, remainder[node_end_match.end():], word_count




    def fix_trace_index_locations(self):
        """Reconcile the two forms of trace index notation; set up syntactic link pointers

        All trees distributed with ontonotes have been through this process.

        There are two forms used in the annotation, one by ann taylor,
        the other by the ldc.

        terminology note: we're using 'word' and 'tag' as in (tag word) and (tag (tag word))

        In the original Penn Treebank system, a trace is a link
        between exactly two tree nodes, with the target index on the
        tag of the parent and the reference index on the word of the
        trace.  If there are more than one node in a trace, they're
        chained, with something like:

        .. code-block:: scheme

          (NP-1 (NP target)) ...
             (NP-2 (-NONE- *-1)) ...
               (NP (-NONE- *-2)) ...

        In the LDC system a trace index can apply to arbitrarily many
        nodes and all indecies are on the tag of the parent.  So this
        same situation would be notated as:

        .. code-block:: scheme

          (NP-1 (NP target)) ...
            (NP-1 (-NONE- *)) ...
              (NP-1 (-NONE- *)) ...

        We're leaving everything in the original Penn Treebank format
        alone, but changing the ldc format to a hybrid mode where
        there can be multiple nodes in a trace chain, but the
        reference indecies are on the words:

        .. code-block:: scheme

          (NP-1 (NP target)) ...
            (NP (-NONE- *-1)) ...
              (NP (-NONE- *-1)) ...


        There are a few tricky details:

         - We have to be able to tell by looking at a tree which format it's in

            - if there are ever words with trace indicies, this mean's we're using the original Penn Treebank format

         - We might not be able to tell what the target is

            - The ideal case has one or more -NONE- tags on traces and exactly one without -NONE-
            - If there are more than one without -NONE- then we need to pick one to be the target.  Choose the leftmost
            - If there is none without -NONE- then we also need to pick one to be the target.  Again choose the leftmost.


        We also need to deal with gapping.  This is for sentences like:

          'Mary likes Bach and Susan, Bethoven'

        These are notated as:

        .. code-block:: scheme

          (S (S (NP-SBJ=1 Mary)
                (VP likes
                   (NP=2 Bach)))
              and
             (S (NP-SBJ=1 Susan)
                 ,
                 (NP=2 Beethoven)))

        in the LDC version, and as:

        .. code-block:: scheme

          (S (S (NP-SBJ-1 Mary)
                (VP likes
                   (NP-2 Bach)))
              and
             (S (NP-SBJ=1 Susan)
                 ,
                 (NP=2 Beethoven)))

        in the original Penn Treebank version.

        We convert them all to the original Penn Treebank version, with the target having
        a single hyphen and references having the double hyphens.

        There can also be trees with both gapping and normal traces, as in:

        .. code-block:: scheme

          (NP fears
            (SBAR that
              (S (S (NP-SBJ=2 the Thatcher government)
                    (VP may (VP be (PP-PRD=3 in (NP turmoil)))))
                  and
                 (S (NP-SBJ-1=2 (NP Britain 's) Labor Party)
                    (VP=3 positioned
                      (S (NP-SBJ-1 *)
                         (VP to (VP regain (NP (NP control)
                                           (PP of (NP the government)))))))))))

        So we need to deal with things like '``NP-SBJ-1=2``' properly.


        Also set up leaf.reference_leaves and leaf.identity_subtree

        """

        def safe_replace(s, old, new):
            return re.sub(old + "(?=($|-|=))", new, s)

        def get_numeric_suffix(s, splitter='-'):
            """ given ('foo-1', '-', []) yield 1 """

            r = s
            if splitter !=  '=':
                r = r.split('=')[0]
            r = r.split(splitter)[-1]

            return r if r.isdigit() and r != s else None

        def get_numeric_suffixes(a_tree):
            bits = a_tree.tag.replace("=", "-").split('-')[1:]
            bits.reverse()

            suff = []

            for x in bits:
                if not x.isdigit():
                    return suff
                suff.append(x)
            return suff

        def get_child_word_trace_number(a_tree):
            if len(a_tree.children) != 1 or not a_tree.children[0].is_leaf():
                return None

            num_suff = get_numeric_suffix(a_tree.children[0].word)

            if num_suff and a_tree.children[0].tag != "-NONE-":
                return None

            return num_suff

        def possible_trace_numbers(a_tree):
            return [ x for x in set([get_child_word_trace_number(a_tree)] + get_numeric_suffixes(a_tree)) if x ]

        def get_nodes_by_trace_number(a_tree, find_gapping):
            """ recursively generate lists for each trace index of all
            the nodes that trace index applies to.  Because of how the
            list is generated, it's in left to right order"""

            nbtn = defaultdict(list)

            def get_nodes_by_trace_number_helper(a_node):
                for trace_number in possible_trace_numbers(a_node):
                    nbtn[trace_number].append(a_node)

                for child in a_node.children:
                    get_nodes_by_trace_number_helper(child)

            get_nodes_by_trace_number_helper(a_tree)

            for trace_number in nbtn.keys():
                """ if we we're supposed to look for gapping, return
                only those where we found gapping.  And vice versa """

                found_gapping = False
                for node in nbtn[trace_number]:
                    possible_gapping_bits = node.tag.split('=')
                    if len(possible_gapping_bits) == 2 and \
                           possible_gapping_bits[1].split('-')[0] == trace_number:
                        found_gapping = True

                if find_gapping != found_gapping:
                    del nbtn[trace_number]
                elif len(nbtn[trace_number]) == 1:
                    self.bad_data("single element %s chain" % ("gap" if find_gapping else "trace"),
                                  ["node", nbtn[trace_number][0].to_string()])

                    del nbtn[trace_number]

            return nbtn

        def child_tag_none(a_tree):
            if not len(a_tree.children) == 1:
                return False
            return a_tree.children[0].tag == "-NONE-"

        trace_nodes_by_index = get_nodes_by_trace_number(self, find_gapping=False)
        gap_nodes_by_index = get_nodes_by_trace_number(self, find_gapping=True)

        gap_indexes = set(gap_nodes_by_index.keys())
        trace_indexes = set(trace_nodes_by_index.keys())

        if len(gap_indexes) + len(trace_indexes) != len(gap_indexes.union(trace_indexes)):
            on.common.log.report("parse", "same index for gapping and tracing",
                                 "Tree using same index number for gapping and tracing: %s in tree %s" %
                                 (self.to_string(), self.id))

        for trace_number, nodes in trace_nodes_by_index.iteritems():

            changes_ok = True
            if any(get_child_word_trace_number(node) for node in nodes):
                """ if we see something like (NP (-NONE- *t*-1)) then
                this trace chain is already in the right format and we
                should leave it alone. """
                changes_ok = False

            def child_trace_type(a_subtree):
                if len(a_subtree.children) != 1:
                    return None
                return a_subtree.children[0].trace_type

            def remove_non_targets(s):
                return [x for x in s if child_trace_type(x) not in syntactic_link_type.never_target]


            possible_targets = remove_non_targets(node for node in nodes if not child_tag_none(node))
            if not possible_targets:
                possible_targets = remove_non_targets(node for node in nodes if get_child_word_trace_number(node) != trace_number)

            if len(possible_targets) == 1:
                target = possible_targets[0]
            elif len(possible_targets) > 1:
                for a_subtree in possible_targets:
                    if child_trace_type(a_subtree) in syntactic_link_type.never_origin:
                        target = a_subtree
                        break
                else:
                    self.bad_data("IGNORE THIS FILE warning ambiguous trace target",
                                  ["possible targets", "\n".join([pt.to_string() for pt in possible_targets])])
                    continue
            else:
                target = nodes[0]


            if changes_ok:
                changes = []
                for node in nodes:
                    if node is target:
                        """ the target is already in the right format """
                        continue

                    if not node.children[0].word:
                        raise Exception("traces: null word at %s to attach trace to in %s -- %s" %
                                        (node.to_string(), self.to_string(), self.id))

                    was = node.to_string()
                    node.children[0].word += "-" + trace_number
                    node.tag = safe_replace(node.tag, "-" + trace_number, "")
                    changes.append("[%s to %s]" % (was,  node.to_string()))

                if changes:
                    on.common.log.report("tree", "changed trace",
                                         changed_trace="%s {target = %s} -- %s" % (", ".join(changes), target, self.id))
                else:
                    raise Exception("Code should not reach here: %s %s %s" %
                                    (target.to_string(), "in tree id", self.id ))

            for node in nodes:
                if node is target:
                    continue

                if get_child_word_trace_number(node) != trace_number:
                     self.bad_data("alternate case of ambiguous trace target",
                                   ["best target", target.pretty_print()],
                                   ["alternate target", node.pretty_print()],
                                   ["other nodes", "\n".join(x.pretty_print()
                                                             for x in nodes
                                                             if x is not target and x is not node)],
                                   ["trace number", trace_number])
                     continue

                source = node.children[0]
                assert source.is_leaf()
                source.identity_subtree = target
                target.reference_leaves.append(source)


        for trace_number, nodes in gap_nodes_by_index.iteritems():

            node = nodes[0]
            new_tag = safe_replace(node.tag, "=" + trace_number, "-" + trace_number)
            if new_tag != node.tag:
                was = node.to_string()
                node.tag = new_tag
                on.common.log.report("tree", "changed gap", "Changed gap: [%s to %s] in %s" % (was, node.to_string(), self.id))





    def is_aux(self, prop=False):
        """ Does this leaf represent an auxilliary verb ?

        Note: only makes sense if english

        All we do is say that a leaf is auxilliary if:

         - it is a verb
         - the next leaf (skipping adverbs) is also a verb

        This does not deal with all cases.  For example, in the
        sentence 'Have you eaten breakfast?', the initial 'have' is an
        auxilliary verb, but we report it as not so. There should be
        no false positives, but we don't get all the cases.

        If the argument 'prop' is true, then we use a less restrictive
        definition of auxilliary that represents closer what is legal
        for proptaggers to tag.  That is, if we have a verb following
        a verb, and the second verb is under an NP, don't count the
        first verb as aux.

        """

        have_do_be_may_can_should_morphs = [
            "am", "is", "are", "'m", "'re", "ai", "shall", "should",
            "be", "being", "been", "was", "were", "will", "would",
            "has", "have", "had", "do", "does", "did", "can", "could",
            "may", "might", "must", "ought"]

        def initial_leaf(a_node):
            while a_node.children:
                a_node = a_node.children[0]
            return a_node

        def next_non_rb():
            """ find whether the first node following the leaf that is not an rb node"""

            def is_adverbial(a_node):
                return initial_leaf(a_node).tag == "RB"

            for a_sibling in self.parent.children[self.child_index+1:]:
                if not is_adverbial(a_sibling):
                    return initial_leaf(a_sibling)

            return None

        if not self.language == "en":
            return False # tree.is_aux only makes sense for english

        if not self.is_verb() or not self.word in have_do_be_may_can_should_morphs:
            return False

        a_sibling = next_non_rb()

        if not a_sibling:
            return False

        if not prop:
            return a_sibling.is_verb()

        if not a_sibling.is_verb():
            return False

        while self not in a_sibling.leaves() and not a_sibling.is_root():
            if a_sibling.tag == "NP":
                return False
            a_sibling = a_sibling.parent
        return True

    def is_verb(self):
        """ Is the part of speech of this leaf VN or VBX for some X assuming the Penn Treebank's tagset? """

        #if self.language == "ar":
        #    bits = re.split(r'\W+', self.part_of_speech)
        #    return "CV" in bits or "IV" in bits or "PV" in bits or "VERB" in bits

        try:
            penn_tb_equiv_pos = pos_type.allowed[self.part_of_speech]
            return penn_tb_equiv_pos.startswith("VB") or penn_tb_equiv_pos.startswith("VN")
        except (KeyError, AttributeError):
            return False

    def is_noun(self):
        """ Is the part of speech of this leaf NN or NNS assuming the Penn Treebank's tagset? """

        try:
            return pos_type.allowed[self.part_of_speech] in ["NN", "NNS"]
        except KeyError:
            return False

    def is_adjective(self):
        """ Is the part of speech of this leaf JJ assuming the Penn Treebank's tagset? """

        try:
            return pos_type.allowed[self.part_of_speech] in ["JJ"]
        except KeyError:
            return False

    @classmethod
    def from_string(cls, syntactic_parse, id=None, document_tag="gold"):
        """Get a tree from a parenthesized string.

        This is the standard way to load a tree from files

        """

        if not syntactic_parse.strip():
            raise Exception("from_string called on empty parse; id=%s" % id)

        # clear all empty nodes
        syntactic_parse = cls.EMPTY_NODE_MATCHER.sub(r'', syntactic_parse)

        try:
            result, remainder, word_count = cls.from_string_helper(syntactic_parse, 0, document_tag=document_tag)
        except Exception:
            try:
                tree = cls.pretty_print_tree_string(syntactic_parse)
            except Exception:
                tree = "{unparseable}"

            parens = 0
            dropped_below_zero = False
            for c in syntactic_parse:
                if c == "(":
                    parens += 1
                elif c == ")":
                    parens -= 1
                    if parens < 0:
                        dropped_below_zero = True
            ended_zero = (parens == 0)

            on.common.log.report("parse", "error in from_string", tree=tree, string=syntactic_parse,
                                 id=id, dropped_below_zero=dropped_below_zero, ended_zero=ended_zero)
            raise

        result.parent = None
        result.id = id

        result.fix_trace_index_locations() # also sets up reference_leaves and identity_subtree


        return result





    # the method that generates the tree object from the database
    @classmethod
    def from_db(cls, a_root_tree_id, a_cursor):
        # get the root tree information
        a_cursor.execute("""select * from tree where id = %s;""", (a_root_tree_id))

        if(a_cursor.rowcount > 1):
            raise Exception("this should not happen, there should be a uniq tree id")

        row = a_cursor.fetchone()
        a_root_tree = tree(row["tag"])
        for attr in "start end coref_section phrase_type function_tag_id".split():
            setattr(a_root_tree, attr, row[attr])
        a_root_tree.id = a_root_tree_id

        on.common.log.debug(a_root_tree, on.common.log.DEBUG, on.common.log.MAX_VERBOSITY)


        a_cursor.execute("""select * from tree where parent_id = %s;""", (a_root_tree_id))

        num_rows = a_cursor.rowcount
        on.common.log.debug("found %s subtrees" % (num_rows), on.common.log.DEBUG, on.common.log.MAX_VERBOSITY)

        rows = a_cursor.fetchall()


        if(num_rows == 0):
            a_subtree = tree(row["tag"], row["word"])
            a_subtree.start = row["start"]
            a_subtree.end = row["end"]
            a_subtree.phrase_type = row["phrase_type"]
            a_subtree.part_of_speech = row["part_of_speech"]

            return a_subtree


        for row in rows:
            a_child_id = row["id"]
            a_child = cls.from_db(a_child_id, a_cursor)
            a_child.parent = a_root_tree
            a_root_tree.children.append(a_child)

        return a_root_tree


    def load_lemmas_from_db(self, a_cursor):
        for a_leaf in self:
            a_leaf.lemma_object = lemma.from_db(a_leaf.id, a_cursor)
            if a_leaf.lemma_object:
                a_leaf.lemma = a_leaf.lemma_object.lemma

    @staticmethod
    def from_db_fast(a_root_tree_id, a_cursor):
        a_cursor.execute("""select parse,coref_section from tree where id = '%s';""" % (a_root_tree_id))
        row = a_cursor.fetchone()

        a_parse = row["parse"]

        a_tree = tree.from_string(a_parse, id=a_root_tree_id)

        a_tree.coref_section = row["coref_section"]

        return a_tree



    def check_subtrees_fix_quotes(self):
        """ check tags are legal, fix double quotes to be the appropriate directional single quote pair. """

        for a_subtree in self.subtrees():
            if a_subtree.is_leaf():
                a_subtree.part_of_speech = a_subtree.tag
                if a_subtree.tag == "-NONE-":
                    if a_subtree.word == "*0*":
                        a_subtree.word = "0"

                    if a_subtree.word != "0" and "*" not in a_subtree.word:
                        self.bad_data("non-trace tagged -NONE-", ["subtree", a_subtree.pretty_print()])

                if a_subtree.tag == '"':
                    """fix quotes"""
                    num_open_quotes = 0
                    num_close_quotes = 0
                    for a_leaf in self.leaves():
                        if a_leaf == a_subtree:
                            break
                        if a_leaf.tag == "``":
                            num_open_quotes += 1
                        elif a_leaf.tag == "''":
                            num_close_quotes += 1
                    a_subtree.tag = "``" if num_open_quotes == num_close_quotes else "''"

                if a_subtree.part_of_speech not in pos_type.allowed:
                    potential_replacement_pos = a_subtree.part_of_speech.replace("=","-").split("-")[0]
                    if potential_replacement_pos in pos_type.allowed:
                        a_subtree.part_of_speech = potential_replacement_pos
                    else:
                        self.bad_data("invalid pos type",
                                      ["pos type", a_subtree.part_of_speech],
                                      ["tag", a_subtree.tag])

            else:
                hyphenated_bits = a_subtree.tag.replace("=","-").split("-")
                a_subtree.phrase_type = hyphenated_bits[0]
                if a_subtree.phrase_type not in phrase_type.allowed:
                    self.bad_data("invalid phrase type", ["phrase type", a_subtree.phrase_type])

                a_compound_function_tag_string = "-".join(x for x in hyphenated_bits[1:] if not x.isdigit())
                if(a_compound_function_tag_string == ""):
                    a_subtree.compound_function_tag = []
                else:
                    a_subtree.compound_function_tag = compound_function_tag(a_compound_function_tag_string, a_subtree)


    def __repr__(self):
        s = self.pretty_print()

        while s.endswith("\n"):
            s = s[:-1]

        if self.is_root():
            return '<on.corpora.tree object id=%s value=<\n%s>' % (self.id, s)
        return s

    def to_string(self,
                  strip_traces=False,
                  strip_edits=False,
                  strip_codes=False,
                  strip_uh=False,
                  strip_disfluencies=False,
                  strip_function_tags=False,
                  buckwalter=False,
                  vocalized=True,
                  as_text=False):

        def helper(x):
            if ((strip_codes and x.tag == "CODE") or
                (strip_edits and x.tag == "EDITED") or
                (strip_uh and x.tag == "UH") or
                (strip_traces and x.is_trace()) or
                (strip_disfluencies and x.tag == "CODE" and x.word in ["<disfluency>", "</disfluency>"])):

                return ""

            if x.is_leaf():
                word = x.get_word(buckwalter, vocalized)
            else:
                child_strings = [y for y in [helper(c) for c in x.children] if y]
                if not child_strings:
                    return ""
                word = " ".join(child_strings)

            if as_text:
                return word
            else:
                tag = x.tag
                if strip_function_tags and not tag.startswith("-"):
                    tag = tag.split("-")[0]

                return "(%s %s)" % (tag, word)

        # we use a helper function so we don't need to keep passing the keyword arguments when we recurse
        return helper(self)


    def get_other_leaf(self, index):
        """

        Get leaves relative to this one.  An index of zero is this
        leaf, negative one would be the previous leaf, etc.  If the
        leaf does not exist, we return None

        """

        for i, a_leaf in enumerate(self.get_root()):
            if a_leaf == self:
                target_idex = i + index
                try:
                    return self.get_root()[target_idex]
                except Exception:
                    return None
        assert False


    def parents(self, inclusive=False):
        """
        give a list of all the parents of a node.  Useful to test things like::

            if any(a_parent.tag == 'NML' for a_parent in a_leaf.parents()):
               pass

        order is immidate parent, ... , root

        if inclusive, include self

        """

        parents = []
        x = self if inclusive else self.parent
        while x:
            parents.append(x)
            x = x.parent
        return parents




    def onf(self, COLS=80,
            skip_plain_sentence=False,
            skip_treebanked_sentence=False,
            wrap_tree = True):
        """ return a human readable representation of this tree and everything it's been enriched with """

        tr = []

        IND=4

        def tidy(title, val, wrapme=True):
            title = title + ":"
            wrapcols = COLS
            if not wrapme:
                wrapcols = 1000
            text = wrap(val, cols=wrapcols, ind=IND)
            tr.append("%s\n%s\n%s\n" % (title , "-"*len(title), text))

        def simplify_node(s_tree):
            """ if a node has only one leaf and that leaf is a trace, return the leaf """
            if len(s_tree) == 1 and s_tree[0].is_trace():
                return s_tree[0]
            return s_tree

        def leaf_info():
            adjusts = {"chain"    : max(len(s) for s in on.corpora.coreference.coreference_chain_type.allowed),
                       "link"     : max(len(s) for s in on.corpora.coreference.coreference_link_type.allowed),
                       "name"     : max(len(s) for s in on.corpora.name.name_entity_type.allowed),
                       "analogue" : max(len(a_type) for a_type in
                                        on.corpora.proposition.predicate_type.allowed +
                                        on.corpora.proposition.argument_type.allowed +
                                        on.corpora.proposition.link_type.allowed),
                       "num"      : 5,
                       "span"     : 7}
            adjusts["prespan"] = adjusts["chain"] + adjusts["link"] + adjusts["num"] + 16

            r = []

            for a_leaf in self:
                r.append("%s %s" % (("%s"%(a_leaf.get_token_index())).ljust(3), a_leaf.get_word()))
                if a_leaf.on_sense:
                    r.append("     %s sense: %s-%s.%s" %(
                        "*" if not a_leaf.on_sense.valid else " ",
                        a_leaf.on_sense.lemma, a_leaf.on_sense.pos, a_leaf.on_sense.sense))
                if a_leaf.proposition:
                    r.append("     %s prop:  %s.%s" % (
                        "*" if not a_leaf.proposition.valid else " ",
                        a_leaf.proposition.lemma, a_leaf.proposition.pb_sense_num))
                    for a_analogue in a_leaf.proposition:
                        indent = "        "
                        a_str = "%s%s" % (indent, a_analogue.type.ljust(adjusts["analogue"]))
                        for a_node_holder in a_analogue:
                            if not a_str:
                                a_str = indent + " "*adjusts["analogue"]
                            a_str += " *"

                            for a_node in a_node_holder:
                                if not a_str:
                                    a_str = indent + "  " + " "*adjusts["analogue"]

                                if not a_node.subtree:
                                    a_str += " -> %s*None*" % ("%s,"%a_node.node_id).ljust(6)
                                    r.append(a_str)
                                    a_str = ""
                                    continue


                                a_subtree = simplify_node(a_node.subtree)
                                is_first = True
                                while True:
                                    a_str += " -> "

                                    nodeid = "%s:%s, " % (a_subtree.get_token_index(), a_subtree.get_height())
                                    if is_first:
                                        nodeid = nodeid.ljust(6)
                                        is_first = False

                                    a_str += nodeid
                                    ind_len = len(a_str)

                                    if a_subtree.is_trace() and a_subtree.identity_subtree:
                                        a_str += '%s' %a_subtree.get_word_string()
                                        a_subtree = simplify_node(a_subtree.identity_subtree)
                                    else:
                                        break

                                a_str += wrap('%s' % a_subtree.get_word_string(), cols=(COLS-IND-ind_len),
                                              ind=len(a_str), indent_first_line=False)
                                r.append(a_str)
                                a_str = ""

                for a_coref_link in a_leaf.start_coreference_link_list:

                    chain_type = a_coref_link.coreference_chain.type
                    link_type = a_coref_link.type

                    if chain_type == link_type:
                        link_type = ""

                    c_link_str = a_coref_link.string

                    a_str = "    %s%s coref: %s %s %s" % (
                        "!" if not a_coref_link.subtree else " ",
                        "*" if not a_coref_link.valid or not a_coref_link.coreference_chain.valid else " ",
                        chain_type.ljust(adjusts["chain"]),
                        link_type.ljust(adjusts["link"]),
                        a_coref_link.coreference_chain.identifier.ljust(adjusts["num"]))

                    a_str = "%s %s" % (a_str.ljust(adjusts["prespan"]),
                                       ("%s-%s" % (a_coref_link.start_token_index,
                                                   a_coref_link.end_token_index)).ljust(adjusts["span"]))

                    a_str += wrap(c_link_str, cols=(COLS-IND-len(a_str)-2),
                                  ind=len(a_str), indent_first_line=False)
                    r.append(a_str)

                for a_named_entity in a_leaf.start_named_entity_list:
                    ne_str = a_named_entity.string

                    a_str = "    %s%s name:  %s" % ("!" if not a_named_entity.subtree else " ",
                                                    "*" if not a_named_entity.valid else " ",
                                                    a_named_entity.type.ljust(adjusts["name"]))

                    a_str = "%s %s" % (a_str.ljust(adjusts["prespan"]),
                                       ("%s-%s" % (a_named_entity.start_token_index,
                                                   a_named_entity.end_token_index)).ljust(adjusts["span"]))

                    a_str += wrap(ne_str, cols=(COLS-IND-len(a_str)-2),
                                  ind=len(a_str), indent_first_line=False)
                    r.append(a_str)

            return "\n".join(r)

        if not skip_plain_sentence:
            tidy("Plain sentence", self.get_plain_sentence())

        if not skip_treebanked_sentence:
            tidy("Treebanked sentence", self.get_word_string())

        if self.speaker_sentence:
            tidy("Speaker information",
                 "\n".join(["name: %s" % self.speaker_sentence.name,
                            "start time: %s" % self.speaker_sentence.start_time,
                            "stop time: %s" % self.speaker_sentence.stop_time]))

        for a_list, a_name in [[self.translations, "Translation"],
                               [self.originals, "Original"]]:
            if a_list:
                tidy(a_name, "\n".join(a_tree.get_word_string() for a_tree in a_list))

        tidy("Tree", self.pretty_print(), wrapme=wrap_tree)

        tidy("Leaves", leaf_info())

        return "\n".join(tr)


    def delete_edits_and_code(self):
        import copy
        a_tree = copy.deepcopy(self)

        for a_subtree in a_tree.subtrees():
            i=0
            while(i<len(a_subtree.children)):
                if(a_subtree.children[i].tag == "EDITED"
                   or a_subtree.children[i].tag == "CODE"):
                    del a_subtree.children[i]
                else:
                    i=i+1
        return a_tree


    def __iter__(self):
        return iter(self.leaves())

    def __getitem__(self, x):
        """ get a leaf, list of leaves, or subtree of this tree

        The semantics of this when used with a slice are tricky.  For
        many purposes you would do better to use one of the following
        instead:

         - :meth:`tree.leaves` -- if you want a list of leaves
         - :meth:`tree.subtrees` -- if you want a list of subtrees
         - :meth:`tree.get_subtree_by_span` -- if you want a subtree
           matching ``start`` and ``end`` leaves or indexes.

        This function is nice, though, especially for interactive use.
        The logic is:

         - if the argument is a single index, return that leaf, or
           index error if it does not exist
         - otherwise think of the tree as a list of leaves.  The
           argument is then interpreted just as list.__getitem__ does,
           with full slice support.
         - if such interpretation leads to a list of leaves that is a
           proper subtree of this one, return that subtree

           - note that if a subtree has a single child, two such
             subtrees can match.  If more than one matches, we take
             the *highest* one.

         - if all the slice can be interpreted to represent is an
           arbitrary list of leaves, return that list.

        For example, consider the following tree:

        .. code-block:: scheme

            (TOP (S (PP-MNR (IN Like)
                    (NP (JJ many)
                        (NNP Heartland)
                        (NNS states)))
                    (, ,)
                    (NP-SBJ (NNP Iowa))
                    (VP (VBZ has)
                        (VP (VBN had)
                            (NP (NP (NN trouble))
                                (S-NOM (NP-SBJ (-NONE- *PRO*))
                                       (VP (VBG keeping)
                                           (NP (JJ young)
                                               (NNS people))
                                           (ADVP-LOC (ADVP (RB down)
                                                           (PP (IN on)
                                                               (NP (DT the)
                                                                   (NN farm))))
                                                     (CC or)
                                                     (ADVP (RB anywhere)
                                                           (PP (IN within)
                                                               (NP (NN state)
                                                                   (NNS lines))))))))))
                    (. .)))

        The simplest thing we can do is look at individual leaves, such as ``tree[2]``:

        .. code-block:: scheme

            (NNP Heartland)

        Note that leaves act as subtrees, so even if we index a leaf,
        ``tree[2][0]`` we get it back again:

        .. code-block:: scheme

            (NNP Heartland)

        If we look at a valid subtree, like with ``tree[0:4]``, we see:

        .. code-block:: scheme

            (PP-MNR (IN Like)
                    (NP (JJ many)
                        (NNP Heartland)
                        (NNS states)))

        If our indexes do not fall exactly on subtree bounds, we instead get a list of leaves:

        .. code-block:: scheme

            [(IN Like),
             (JJ many),
             (NNP Heartland),
             (NNS states),
             (, ,)]

        Extended slices are supported, though they're probably not
        very useful.  For example, we can make a list of the even
        leaves of the tree in reverse order with ``tree[::-2]``:

        .. code-block:: scheme

           [(. .),
            (NN state),
            (RB anywhere),
            (NN farm),
            (IN on),
            (NNS people),
            (VBG keeping),
            (NN trouble),
            (VBZ has),
            (, ,),
            (NNP Heartland),
            (IN Like)]


        """

        leaves = self.leaves()[x]

        if not leaves:
            return []

        if type(leaves) == type(self):
            return leaves

        a_subtree = self.get_subtree_by_span(leaves[0], leaves[-1])

        if a_subtree and list(a_subtree.leaves()) == leaves:
            return a_subtree

        return leaves

    def __nonzero__(self):
        return True

    def __len__(self):
        return len(self.leaves())

    def get_proposition(self):
        return self.proposition

    def set_proposition(self, prop):
        raise Exception("This function is no longer to be used -- use proposition.enrich_tree instead")


    def pointer(self, indexing="token"):
        """ (document_id, tree_index, sentence_index)

        Return a triple in the format of the pointers in a sense or prop file.

        """


        if not self.is_leaf():
            raise Exception("Can generate pointers only to leaves")

        if indexing == "token":
            s_idx = self.get_token_index()
        elif indexing == "word":
            s_idx = self.get_word_index()
        else:
            raise Exception("tree.pointer: The only allowable indexing values are word and token")

        t_idx = self.get_sentence_index()

        return self.get_document_id(), t_idx, s_idx

    def get_document_id(self):
        return self.get_root().document_id

    def get_sentence_index(self):
        """ the index of the sentence (zero-indexed) that this tree represents """

        return int(self.get_root().id.split("@")[0])

    def get_word_index(self, sloppy=False):
        """ the index of the word in the sentence not counting traces

        sloppy is one of:
          False: be strict
          'next' : if self is a trace, take the next non-trace leaf
          'prev' : if self is a trace, take the prev non-trace leaf

        """

        assert sloppy in [False, 'next', 'prev']

        w_index = 0
        for a_leaf in self.get_root():
            if a_leaf.is_trace():
                if a_leaf == self:
                    if sloppy == "next":
                        return w_index
                    elif sloppy == "prev":
                        return w_index-1
                continue

            if a_leaf == self:
                return w_index

            w_index += 1

        assert self.is_trace()
        return None

    def get_token_index(self):
        """ the index of the word in the sentence including traces """
        return int(self.start)

    def get_height(self):
        """ how many times removed this node is from its initial leaf.

        Examples:

         - height of ``(NNS cabbages)`` is 0
         - height of ``(NP (NNS cabbages))`` is 1
         - height of ``(PP (IN of) (NP (NNS cabbages)))`` is also 1 because ``(IN of)`` is its initial leaf

        Used by propositions

        """

        if not self.children:
            return 0
        return self.children[0].get_height() + 1

    def get_on_sense(self):
        return self.on_sense

    def set_on_sense(self, sense):
        self.on_sense = sense

    def get_lemma(self, word_to_morph={}):
        if self.language == "ar":
            return on.common.util.buckwalter2fsbuckwalter(self.lemma)

        w = self.get_word().lower()

        # from bert 2009-07-29
        lemma_map = {
            u'\u4e0d\u8981': u'\u8981',
            u'\u4e25\u4e25\u5b9e\u5b9e': u'\u4e25\u5b9e',
            u'\u4f4e\u4f4e': u'\u4f4e',
            u'\u4f4f\u5728': u'\u4f4f',
            u'\u5149\u5149': u'\u5149',
            u'\u5199\u5199': u'\u5199',
            u'\u5229\u4ed6': u'\u4ed6',
            u'\u5520\u5520\u53e8\u53e8': u'\u5520\u53e8',
            u'\u559d\u5976': u'\u559d',
            u'\u56de\u5473\u56de\u5473': u'\u56de\u5473',
            u'\u5750\u5728': u'\u5750',
            u'\u5b58\u94b1': u'\u5b58',
            u'\u5df2\u77e5': u'\u77e5',
            u'\u5f97\u5b50': u'\u5f97',
            u'\u610f\u601d\u610f\u601d': u'\u610f\u601d',
            u'\u6258\u4eba': u'\u6258',
            u'\u6392\u6210\u957f\u9f99': u'\u6392\u6210',
            u'\u6392\u9664\u5728\u5916': u'\u6392\u9664',
            u'\u63d0\u7a0e': u'\u63d0',
            u'\u6709\u52a9\u76ca\u4e8e': u'\u6709',
            u'\u6709\u95e8\u574e': u'\u6709',
            u'\u671b\u671b': u'\u671b',
            u'\u70b8\u697c': u'\u70b8',
            u'\u72af\u9519\u8bef': u'\u72af',
            u'\u73a9\u73a9': u'\u73a9',
            u'\u7ad9\u5728': u'\u7ad9',
            u'\u7cbe\u7626\u7cbe\u7626': u'\u7cbe\u7626',
            u'\u7eba\u7ebf': u'\u7eba',
            u'\u82b1\u5728': u'\u82b1',
            u'\u89e3\u4eba\u610f': u'\u89e3',
            u'\u8c08\u8c08': u'\u8c08',
            u'\u8eb2\u8eb2\u85cf\u85cf': u'\u8eb2\u85cf',
            u'\u8f6c\u8f7d\u81ea': u'\u8f6c\u8f7d',
            u'\u8fc7\u5c0f\u5e74': u'\u8fc7',
            u'\u957f\u5f97': u'\u957f',
            u'\u96c7\u4e86': u'\u96c7'}

        if w in lemma_map:
            return lemma_map[w]
        elif w in word_to_morph:
            return word_to_morph[w]
        return w

    def set_lemma(self, lemma):
        self.lemma = lemma

    def leaves(self, regen_cache=False):
        """ generate the leaves under this subtree """

        if regen_cache or not self._leaves:
            if not self.children:
                self._leaves = [self]
            else:
                self._leaves = [a_leaf for a_child in self.children
                                       for a_leaf in a_child.leaves(regen_cache=regen_cache)]
        return self._leaves

    def tokens(self, regen_cache=False):
        """ generate the tokens under this subtree """

        if regen_cache or not self._tokens:
            self._tokens = [l.get_word() for l in self.leaves(regen_cache=regen_cache)]

        return self._tokens

    def subtrees(self, regen_cache=False):
        """ generate the subtrees under this subtree, including this one

        order is always top to bottom; if A contains B then index(A) < index(B)

        """

        if regen_cache or not self._subtrees:
            self._subtrees = [self] + [a_subtree for a_child in self.children
                                                 for a_subtree in a_child.subtrees(regen_cache=regen_cache)]
        return self._subtrees

    def is_leaf(self):
        """ does this tree node represent a leaf? """

        if( self.children == [] ):
            return True
        else:
            return False

    def is_trace(self):
        """ does this tree node represent a trace? """

        if self.tag == "XX" and "coref" == self.get_root().document_tag:
            # when we have dummy trees all leaves are XX.  So when
            # copying from automatically generated coref trees we need
            # to be able to know what a trace is.
            return self._raw_trace_type() in syntactic_link_type.allowed

        return self.tag == "-NONE-"



    # get the word string between two token indices
    def get_se_word_string(self, start, end, buckwalter=False, vocalized=True):
        return " ".join(self.get_word_string(buckwalter, vocalized).split()[start:end])

    def get_word(self,
                 buckwalter=False,
                 vocalized=True,
                 clean_speaker_names=True,
                 interpret_html_escapes=True,
                 indexed_traces=True):

        assert self.is_leaf()

        if not self.lemma_object:
            a_word = self.word
        elif buckwalter and vocalized:
            a_word = self.lemma_object.vocalized_string.replace("+", "").replace("~", "")
        elif buckwalter and not vocalized:
            a_word = self.lemma_object.unvocalized_string
        elif not buckwalter and vocalized:
            a_word = self.lemma_object.vocalized_input
        else: # not buckwalter and not vocalized:
            a_word = self.lemma_object.input_string

        if a_word is None:
            a_word = ""

        a_word = a_word.strip()

        if not a_word:
            language = self.get_root().language
            if not language:
                language = self.id.split("@")[-2]
            self.bad_data("leaf has no word",
                          ["raw_word", self.word],
                          ["token number", self.get_token_index()],
                          ["leaf id", self.id],
                          pretty_print=False)
            a_word = "nullp"

        if not indexed_traces and self.is_trace():
            a_word = a_word.split("-")[0]

        if self.tag == "CODE" and clean_speaker_names:
            a_word = re.sub(tree.CODE_CLEANER, r"[\2]", a_word)

        if interpret_html_escapes:
            for f, r in [['gt', '>'],
                         ['lt', '<'],
                         ['amp', '&'],
                         ['quot', '"']]:
                a_word = a_word.replace("&%s;" % f, r)

        a_word = a_word.replace("(","-LRB-").replace(")", "-RRB-").strip().replace(" ", "_")

        return a_word


    # get the string of tokens in the tree
    def get_word_string(self, buckwalter=False, vocalized=True):
        """ return the words for this tree, separated by spaces. """

        return " ".join(a_leaf.get_word(buckwalter, vocalized) for a_leaf in self.leaves())


    # get the string of trace adjusted tokens in the tree
    def get_trace_adjusted_word_string(self, buckwalter=False, vocalized=True):
        """ The same as :meth:`get_word_string` but without including traces """

        return " ".join(a_leaf.get_word(buckwalter, vocalized) for a_leaf in self.leaves() if not a_leaf.is_trace())


    def get_plain_sentence(self):
        """ display this sentence with as close to normal typographical conventions as we can.

        Note that the return value of this function *does not* follow ontonotes tokenization.

        """

        def _helper(a_tree):
            if a_tree.tag in ["CODE", "EDITED"]:
                return ""
            elif a_tree.is_leaf():
                if a_tree.is_trace():
                    return ""
                return a_tree.get_word(buckwalter=False, vocalized=False)

            sep = "" if self.language == "ch" else " "
            return sep.join(_helper(a_child) for a_child in a_tree.children )

        s = _helper(self)

        s = " ".join(s.split())

        for remove_slash_before in ["-", ".", ",", "?"]:
            s = s.replace(" /" + remove_slash_before, remove_slash_before)

        for remove_space_before in ["'m", "'s", ".", ",", "-", "/"]:
            s = s.replace(" " + remove_space_before, remove_space_before)

        for remove_space_after in ["-", "/"]:
            s = s.replace(remove_space_after + " ", remove_space_after)

        return s

    def get_root(self):
        """ Return the root of this tree, which may be ourself. """

        a_tree = self
        while a_tree.parent is not None:
            a_tree = a_tree.parent
        return a_tree


    def is_root(self):
        """ check whether the tree is a root """
        return self.parent is None

    def get_subtree(self, a_id):
        """ return the subtree with the specified id """

        if "@" not in a_id:
            self_first_bit, self_rest = self.id.split("@", 1)
            use_rest = self_rest
            if ":" not in self_first_bit:
                use_rest = self.id
            a_id = "%s@%s" % (a_id, use_rest)

        for a_subtree in self.subtrees():
            if( a_subtree.id == a_id ):
                return a_subtree

        raise tree_exception("requested id: %s not found -- the code should not reach here for a valid id." % (a_id))

    def get_leaf_by_word_index(self, a_word_index):
        """ given a word index, return the leaf at that index """

        w_index = 0
        for a_leaf in self:
            if a_leaf.is_trace():
                continue

            if a_word_index == w_index:
                return a_leaf
            w_index += 1

        raise KeyError("No Such Leaf Index %s" % a_word_index)

    def get_leaf_by_token_index(self, a_token_index):
        """ given a token index, return the leaf at that index """

        if not self.is_root():
            raise Exception("get_leaf_by_token_index only makes sense on the root of a tree")

        a_subtree_id = "%s:0@%s" % (a_token_index, self.id)
        try:
            return self.get_subtree(a_subtree_id)
        except Exception:
            return None

    def get_subtree_by_span(self, start, end):
        """ given start and end of a span, return the highest subtree that represents it

        The arguments ``start`` and ``end`` may either be leaves or token indecies.

        Returns ``None`` if there is no matching subtree.

        """

        leaves = list(self.leaves())

        try:
            if start not in leaves:
                start = leaves[start]
            if end not in leaves:
                end = leaves[end - 1]
        except Exception:
            return None

        for a_subtree in self.subtrees():
            if a_subtree[0] is start and a_subtree[-1] is end:
                return a_subtree

        return None


    def get_subtree_id(self, start, end):
        """ given start and end of a span, return the id of the highest subtree that represents it

        Instead of token indecies, start and end may be leaves.

        """

        a_subtree = self.get_subtree_by_span(start, end)

        if not a_subtree:
            on.common.log.warning(u"could not find a matching subtree id for '%s'.   returning False" % (self.get_se_word_string(start, end)), on.common.log.MAX_VERBOSITY)
            return False
        return a_subtree.id


    # get an id for a subtree
    def get_lowest_subtree_id(self, start, end):
        """ given start and end of a span, return the id of the lowest subtree that represents it """

        a_subtree = self.get_subtree_by_span(start, end)
        if not a_subtree:
            on.common.log.warning(u"could not find a matching subtree id for '%s'.   returning False" % (self.get_se_word_string(start, end)), on.common.log.MAX_VERBOSITY)
            return False
        while len(a_subtree.children) == 1:
            a_subtree = a_subtree.children[0]
        return a_subtree.id

    def get_closest_aligning_span(self, a_start_leaf, a_end_leaf,
                                  ignore_traces=True,
                                  ignore_punct=True,
                                  ignore_determiners=True,
                                  ignore_possessives=True,
                                  strict_alignment=True):
        """ return a start and end leaf that correspond to a subtree, or None, None

        if strict alignment is false, instead of requiring a subtree,
        we just require that this span not give bracketing errors (see
        get_bracketing_mismatch)

        """

        def condition_func(s, e):
            if strict_alignment:
                return self.get_subtree_by_span(s,e)
            return not self.get_bracketing_mismatch(s,e)

        if condition_func(a_start_leaf, a_end_leaf):
            return a_start_leaf, a_end_leaf # if we're already good, just be done with it

        def may_ignore(alt_leaf):
            return ((ignore_traces and alt_leaf.is_trace()) or
                    (ignore_punct and alt_leaf.is_punct()) or
                    (ignore_determiners and alt_leaf.part_of_speech == "DT") or
                    (ignore_possessives and alt_leaf.part_of_speech == "POS"))

        all_leaves = list(self.leaves())

        # make sure start and end cannot be ignored
        start_leaf_index, end_leaf_index = all_leaves.index(a_start_leaf), all_leaves.index(a_end_leaf)

        while start_leaf_index <= end_leaf_index and may_ignore(all_leaves[start_leaf_index]):
            start_leaf_index += 1
        while end_leaf_index >= start_leaf_index and may_ignore(all_leaves[end_leaf_index]):
            end_leaf_index -= 1

        if start_leaf_index <= end_leaf_index:
            """ only change start and end if the span contains some non ignorable leaves """
            a_start_leaf, a_end_leaf = all_leaves[start_leaf_index], all_leaves[end_leaf_index]
            assert not may_ignore(a_start_leaf) and not may_ignore(a_end_leaf)

        def alternates(a_leaf, our_loc):
            """ Return alternate alignable leaves that are are semantically equivalent

            Imagine we have the following name document:

            .. code-block:: xml

                The <ENAMEX> Iraq war </ENAMEX>

            If the tree looks like:

            .. code-block:: scheme

                (NP (DT the)
                    (NNP Iraq)
                    (NN war))

            then we can't align with the tree.  But the determiner can
            be included or not.  So here, if we were given the start
            leaf 'Iraq', we would return the leaves for 'Iraq' and
            'the', in the hopes that one of them would let us align.

            """

            def helper(a_list):
                for alt_leaf in a_list:
                    if not may_ignore(alt_leaf):
                        return
                    yield alt_leaf

            return itertools.chain([a_leaf], helper(all_leaves[our_loc:]), helper(reversed(all_leaves[:our_loc])))

        if a_start_leaf not in all_leaves or a_end_leaf not in all_leaves:
            return None, None

        for alt_start_leaf in alternates(a_start_leaf, all_leaves.index(a_start_leaf)):
            for alt_end_leaf in alternates(a_end_leaf, all_leaves.index(a_end_leaf) + 1):
                if condition_func(alt_start_leaf, alt_end_leaf):
                    return alt_start_leaf, alt_end_leaf

        return None, None


    def get_bracketing_mismatch(self, span_start, span_end):
        """ would inserting a span from start to end be illegal bracketing?

        Example:

           tree: (a (b c) (d e) (f (g (h i) j) k))

           span  legality
            a-b  False
            a-c  True
            b-e  True
            b-h  False
            f-k  True

        Alternately, are there equal numbers of open and close parens between start and stop?

        We return None if it would be legal, or a counterexample subtree if illegal

        """

        span_leaves = [a_leaf for a_leaf in self
                       if a_leaf.start >= span_start.start and a_leaf.start <= span_end.start]

        alt_string = " ".join(a_leaf.get_word() for a_leaf in span_leaves)

        # it's a bracketing mismatch if a subtree contains both span
        # leaves and non-span leaves, unless it contains all the span
        # leaves
        #
        bracketing_mismatches = [] # (height, tree)
        for a_subtree in self.subtrees():
            has_any_span_leaves = any(a_leaf in span_leaves for a_leaf in a_subtree)
            has_only_span_leaves = all(a_leaf in span_leaves for a_leaf in a_subtree)
            has_all_the_span_leaves = all(a_leaf in a_subtree for a_leaf in span_leaves)

            if has_any_span_leaves and not has_only_span_leaves and not has_all_the_span_leaves:
                bracketing_mismatches.append((a_subtree.get_height(), a_subtree))

        if not bracketing_mismatches:
            return None

        bracketing_mismatches.sort()
        height, mismatch = bracketing_mismatches[0]

        return mismatch


    def initialize_ids(self, language):
        """ initialize the ids of all the subtrees in this tree """

        if(self.parent != None):
            raise Exception("this function can only be called on the root node in a tree")

        self.language = language

        for token_index, a_leaf in enumerate(self):
            a_parent = a_leaf
            height = 0
            while a_parent.parent:
                if not a_parent.id:
                    a_parent.id = "%s:%s@%s" % (token_index, height, self.id)
                    a_parent.language = language
                    self.subtree_ids.append(a_parent.id)

                a_parent = a_parent.parent
                height += 1


    # lets tag the child indices of each subtree

    #
    # this could be done in the main code, but the reason this is
    # being handled here is to prevent adding a bug to the main
    # routine, and start a way to annotate data on top of the
    # trees as a supplementary process. although it does decrease
    # efficiency, i think it is a cleaner way to do things
    #

    # label all the child indices
    def label_child_indices(self):
        # result is the top-most node of the whole tree therefore we start from there
        for i in range(0, len(self.children)):
            self.children[i].child_index = i
            self.children[i].label_child_indices()



    def is_punct(self):
        """ is this leaf punctuation? """

        return self.part_of_speech in on.common.util.PUNCT

    def is_conj(self):
        """ is this leaf a conjunction? """

        return pos_type.allowed.get(self.part_of_speech, "not found") == "CC"


    def is_trace_indexed(self):
        """ if we're a trace, do we have a defined reference index? """
        return bool(self.reference_index)


    def is_trace_origin(self):
        """ if we're a trace, do we have a defined identity index? """
        return bool(self.identity_index)



    def print_ancestors(self):
        if self.parent == None:
            return

        on.common.log.status(self.parent.id, ":", self.parent)
        self.parent.print_ancestors()



    # initialize the syntactic links in the tree
    def initialize_syntactic_links(self):
        for a_leaf in self:
            if not a_leaf.identity_subtree:
                continue

            self.syntactic_links.append(syntactic_link(a_leaf.trace_type,
                                                       a_leaf.get_word_string(),
                                                       a_leaf.id,
                                                       a_leaf.identity_subtree.id))


    # compares the leaves in an automatic tree with a treebank tree to check
    # for possible segmentation errors
    def check_segmentation(self, t_tree):
        a_tree = self

        a_leaves = list(a_tree.leaves())
        t_leaves = list(t_tree.leaves())

        a_string = ""
        for a_leaf in a_leaves:
            a_string = "%s %s" % (a_string, a_leaf.word)
        a_string = a_string.strip()


        t_string = ""
        for t_leaf in t_leaves:
            if(t_leaf.is_trace() == False):
                t_string = "%s %s" % (t_string, t_leaf.word)
            else:
                on.common.log.debug("found trace leaf", on.common.log.DEBUG, on.common.log.MAX_VERBOSITY)
        t_string = t_string.strip()


        if(a_string != t_string):
            return False

        return True



    def pretty_print(self, offset='', buckwalter=False, vocalized=True):
        """ return a string representing this tree in a human readable format """

        return self.pretty_print_tree_string(self.to_string(buckwalter=buckwalter, vocalized=vocalized), offset)

    @staticmethod
    def pretty_print_tree_string(a_tree_string, offset=''):

        if not a_tree_string.strip():
            return ""

        # Maximum depth we're prepared for in trees
        maxdepth=100
        maxindent=300

        # Table of indentation at tree depth
        depth_to_indent = [0 for i in xrange(maxdepth)]

        # Initialize indent_string[i] to be a string of i spaces
        indent_string = ['' for i in xrange(maxindent)]
        for i in xrange(maxindent-1):
            indent_string[i+1] = indent_string[i] + ' '

        # RE object for split that matches on a ')' followed by not a ')', but only consumes the ')'
        close_paren = re.compile(r'\)(?=\s*[^\s\)])')

        # RE object to pick up first on this line(should be only) POS tag and the word of each lexical leaf of the tree
        lexical_leaf = re.compile(r'\((?P<tag>[^\s\)\(]+)\s+(?P<word>[^\s\)\(]+)\)')

        # RE object to parse OntoNotes Normal Form tree lines:
        a_tree = a_tree_string

        pp_tree = ""

        def treeindent(depth):
            return indent_string[depth_to_indent[depth]]+offset  #Indent to appropriate point


        current_depth = 0
        for frag in  close_paren.split(a_tree):  #Split input into lines ending with a lexical item
            if frag[-1]!= '\n':
                frag=frag+')'
            else: frag=frag[0:-1]

            #Indent to appropriate point
            pp_tree += treeindent(current_depth)

            pfrag = ""
            for pfrag in (frag).split('(')[1:]:         # Split line into segments each beginning with an '('
                pfrag='('+pfrag                         # Restore deleted initial '('
                pp_tree += pfrag                      # Print each
                current_depth=current_depth+1           # Up the current depth count

                # Remember how far to indent following items at this depth
                depth_to_indent[current_depth]=depth_to_indent[current_depth-1]+len(pfrag)

            current_depth=current_depth-pfrag.count(')')    # Correct depth given closing parens
            if current_depth<=0:
                pp_tree += ''            # Separate toplevel trees with blank lines

            pp_tree += '\n'              # Print CRLF


        return re.sub("\)$", "", pp_tree)





    sql_table_name = "tree"

    # sql create statement for the tree table
    sql_create_statement = \
"""
create table tree
(
  id                  varchar(255) not null collate utf8_bin primary key,
  parent_id           varchar(255),
  document_id         varchar(255),
  word                varchar(255),
  child_index         int,
  start               int not null,
  end                 int not null,
  coref_section       int not null,
  syntactic_link_type varchar(255),
  tag                 varchar(255) not null,
  part_of_speech      varchar(255),
  phrase_type         varchar(255),
  function_tag_id     varchar(255),
  string              longtext,
  no_trace_string     longtext,
  parse               longtext,
  foreign key (parent_id)           references tree.id,
  foreign key (document_id)         references document.id,
  foreign key (syntactic_link_type) references syntactic_link_type.id,
  foreign key (part_of_speech)      references pos_type.id,
  foreign key (phrase_type)         references phrase_type.id,
  foreign key (function_tag_id)     references compound_function_tag.id
)
default character set utf8;
"""





    # sql inser statement for the tree table
    sql_insert_statement = \
"""insert into tree
(
  id,
  parent_id,
  document_id,
  word,
  child_index,
  start,
  end,
  coref_section,
  syntactic_link_type,
  tag,
  part_of_speech,
  phrase_type,
  function_tag_id,
  string,
  no_trace_string,
  parse
) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""





    def write_to_db(self, cursor):

        if(self.parent == None):
            a_parent_id = None
        else:
            a_parent_id = self.parent.id

        if not self.compound_function_tag:
            a_compound_function_tag_id = None
        else:
            a_compound_function_tag_id = self.compound_function_tag.id

        data = [(self.id,
                 a_parent_id,
                 self.document_id,
                 self.get_word() if self.is_leaf() else "",
                 self.child_index,
                 self.start,
                 self.end,
                 self.coref_section,
                 self.trace_type,
                 self.tag,
                 self.part_of_speech,
                 self.phrase_type,
                 a_compound_function_tag_id,
                 self.get_word_string(),
                 self.get_trace_adjusted_word_string(),
                 self.to_string())]

        cursor.executemany("%s" % (self.__class__.sql_insert_statement), data)

        for a_syntactic_link in self.syntactic_links:
            a_syntactic_link.write_to_db(cursor)


        if self.compound_function_tag:
            self.compound_function_tag.write_to_db(cursor)


        for a_leaf in self.leaves():
            if(a_leaf.lemma_object != None):
                a_leaf.lemma_object.write_to_db(cursor)



    #def convert_leaves_to_unicode(self):
    #    for leaf in self.leaves():
    #        if(not leaf.is_trace()):
    #            leaf.word = on.common.util.buckwalter2unicode(leaf.word)





def tree_string2words(a_tree_string):
    a_word_string = " ".join(re.findall("\([^\s]+\s+([^\s]+?)\)", a_tree_string))
    a_word_string = re.sub("~([A-Z]+)", "\g<1>", a_word_string)
    a_word_string = re.sub(r"/([\.\-\?])\s*$", "\g<1>", a_word_string)
    return a_word_string











class tree_exception(Exception):
    pass













class tree_document:
    """
    Contained by: :class:`treebank`

    Contains: :class:`tree` (roots)

    Attributes:

        The following two attributes are set during enrichment with
        parallel banks.  For sentence level annotation, see
        :attr:`tree.translations` and :attr:`tree.originals` .

        .. attribute:: translations

           A list of :class:`tree_document` instances in other subcorpora
           that represent translations of this document

        .. attribute:: original

           A single :class:`tree_document` instance representing the
           original document that this one was translated from.  It doesn't
           make sense to have more than one of these.

    Methods:

        .. automethod:: sentence_tokens_as_lists


    """


    V2_ARABIC=["gold-p3-v2-0", "p3v2"]

    def __init__(self, document_id, parse_list,
                 sentence_id_list, headline_flag_list, paragraph_id_list,
                 absolute_file_path, a_treebank, subcorpus_id, a_cursor=None,
                 extension="parse"):

        self.language = subcorpus_id.split("@")[-2]

        #if  self.language  == "ar" :
        #    parse_list = [on.common.util.unicode2buckwalter(x) for x in parse_list]

        self.document_id = document_id
        self.parse_list = parse_list
        self.tree_hash = {}
        self.tree_ids = []
        self.coreference_document = None
        self.subcorpus_id = subcorpus_id
        self.treebank_id = a_treebank.id
        self.a_treebank = a_treebank
        self.extension=extension
        self.absolute_file_path = absolute_file_path

        self.original = None   # these two hold references to other tree documents
        self.translations = []

        version = self.treebank_id.split("@")[0]

        if(a_cursor == None):


            HAVE_LEMMA_FILE=True

            if(self.language == "ar"):

                if(version == None):
                    raise Exception("please specify the version of the arabic treebank")
                #elif version not in ["gold-p3-v2-0", "gold-p3-v3-0", "gold"]:
                #    raise Exception("the specified arabic treebank version (%s) is not supported in this release" % version)

                if version in self.V2_ARABIC:

                    on.common.log.debug("document_id: %s" % (document_id), on.common.log.DEBUG, on.common.log.MIN_VERBOSITY)
                    on.common.log.debug("absolute_file_path: %s" % (absolute_file_path), on.common.log.DEBUG, on.common.log.MIN_VERBOSITY)


                    pos_filename = absolute_file_path.replace("parse", "lemma")
                    if(os.path.exists(pos_filename)):
                        pos_file = codecs.open(pos_filename, "r", "utf-8")

                        actual_word_list = []
                        buckwalter_word_list = []
                        lemma_list = []

                        input_string_regex = re.compile(r"^\s*INPUT STRING:(.*)", re.U)
                        buckwalter_regex = re.compile(r"^\s*LOOK-UP WORD:(.*)", re.U)
                        lemma_regex = re.compile(r"^\s*\*\s+SOLUTION\s+\d+:\s+\(.*?\)\s+\[(.*?)\].*$", re.U)

                        pos_file_lines = pos_file.readlines()
                        found_solution = False

                        for i in range(0, len(pos_file_lines)):

                            input_string_regex_match = input_string_regex.findall(pos_file_lines[i])

                            if(input_string_regex_match != []):
                                on.common.log.debug("found INPUT STRING", on.common.log.DEBUG, on.common.log.MAX_VERBOSITY)
                                on.common.log.debug("input_string_regex_match: %s" % (input_string_regex_match), on.common.log.DEBUG, on.common.log.MAX_VERBOSITY)
                                actual_word_list.append(input_string_regex_match[0].strip())

                                if(len(actual_word_list) != 1):
                                    if not found_solution:
                                        lemma_list.append("")

                                found_solution = False


                                buckwalter_regex_match = buckwalter_regex.findall(pos_file_lines[i+1])
                                if(buckwalter_regex_match != []):
                                    on.common.log.debug("found LOOK-UP WORD", on.common.log.DEBUG, on.common.log.MAX_VERBOSITY)
                                    on.common.log.debug("buckwalter_regex_match: %s" % (buckwalter_regex_match), on.common.log.DEBUG, on.common.log.MAX_VERBOSITY)
                                    buckwalter_word_list.append(buckwalter_regex_match[0].strip())

                                else:
                                    on.common.log.debug("did not find LOOK-UP WORD", on.common.log.DEBUG, on.common.log.MAX_VERBOSITY)
                                    buckwalter_word_list.append("")

                            lemma_regex_match = lemma_regex.findall(pos_file_lines[i])
                            if(lemma_regex_match != []):
                                on.common.log.debug("found SOLUTION", on.common.log.DEBUG, on.common.log.MAX_VERBOSITY)
                                lemma_list.append(re.sub("_.*?$", "", lemma_regex_match[0].strip()))
                                found_solution = True


                        if(len(actual_word_list) != len(buckwalter_word_list)
                           or
                           len(actual_word_list) != len(lemma_list)):
                            on.common.log.debug("len(actual_word_list): %s" % (len(actual_word_list)), on.common.log.DEBUG, on.common.log.MAX_VERBOSITY)
                            on.common.log.debug("len(buckwalter_word_list): %s" % (len(buckwalter_word_list)), on.common.log.DEBUG, on.common.log.MAX_VERBOSITY)
                            on.common.log.debug("len(lemma_list): %s" % (len(lemma_list)), on.common.log.DEBUG, on.common.log.MAX_VERBOSITY)
                            raise Exception("the three lists -- actual word, buckwalter word, and lemma should be the same length, or else some information might be missing from the .lemma file")

                        for i in range(0, len(actual_word_list)):
                            if(lemma_list[i] == "DEFAULT"
                               or
                               buckwalter_word_list[i] == ""):
                                on.common.log.debug("%s %s %s\n" % (actual_word_list[i].rjust(50), buckwalter_word_list[i].rjust(50), lemma_list[i].rjust(50)), on.common.log.DEBUG, on.common.log.MAX_VERBOSITY)
                    else:
                        raise Exception("could not find lemma file")

                elif(version == "gold-p3-v3-0" or version == "gold" or True):

                    on.common.log.debug("document_id: %s" % (document_id), on.common.log.DEBUG, on.common.log.MIN_VERBOSITY)
                    on.common.log.debug("absolute_file_path: %s" % (absolute_file_path), on.common.log.DEBUG, on.common.log.MIN_VERBOSITY)

                    pos_filename = absolute_file_path.replace("parse", "lemma")
                    if(os.path.exists(pos_filename)):
                        pos_file = codecs.open(pos_filename, "r", "utf-8")

                        actual_word_list = []
                        buckwalter_word_list = []
                        lemma_list = []


                        input_string_regex = re.compile(r"^\s*INPUT STRING:(.*)", re.U|re.MULTILINE)
                        buckwalter_regex = re.compile(r"^\s*IS_TRANS:(.*)", re.U|re.MULTILINE)
                        comment_regex = re.compile(r"^\s*COMMENT:(.*)", re.U|re.MULTILINE)
                        index_regex = re.compile(r"^\s*INDEX:(.*)", re.U|re.MULTILINE)
                        offsets_regex = re.compile(r"^\s*OFFSETS:(.*)", re.U|re.MULTILINE)
                        unvocalized_string_regex = re.compile(r"^\s*UNVOCALIZED:(.*)", re.U|re.MULTILINE)
                        vocalized_string_regex = re.compile(r"^\s*VOCALIZED:(.*)", re.U|re.MULTILINE)
                        vocalized_input_string_regex = re.compile(r"^\s*VOC_STRING:(.*)", re.U|re.MULTILINE)
                        pos_string_regex = re.compile(r"^\s*POS:(.*)", re.U|re.MULTILINE)
                        gloss_string_regex = re.compile(r"^\s*GLOSS:(.*)", re.U|re.MULTILINE)
                        lemma_regex = re.compile(r"LEMMA:\s+\[([^\]]*)\]", re.U|re.MULTILINE)


                        pos_file_lines = pos_file.readlines()

                        list_of_pos_blocks = []

                        i=0
                        pos_block = ""
                        list_of_pos_blocks = []
                        while(i<len(pos_file_lines)):
                            input_string_regex_match = input_string_regex.findall(pos_file_lines[i])

                            if(input_string_regex_match != []):
                                while(i<len(pos_file_lines) and pos_file_lines[i].strip() != ""):
                                    pos_block = "%s%s" % (pos_block, pos_file_lines[i])
                                    i=i+1

                            if(pos_block.strip() != ""):
                                list_of_pos_blocks.append(pos_block)

                            pos_block = ""
                            i=i+1


                        list_of_input_strings = []
                        list_of_b_transliterations = []
                        list_of_comments = []
                        list_of_indices = []
                        list_of_offsets = []
                        list_of_unvocalized_strings = []
                        list_of_vocalized_strings = []
                        list_of_vocalized_inputs = []
                        list_of_pos = []
                        list_of_glosses = []
                        list_of_lemmas = []

                        for pos_block in list_of_pos_blocks:
                            for a_list, a_regex, a_name in [[list_of_input_strings, input_string_regex, "input"],
                                                            [list_of_b_transliterations, buckwalter_regex, "transliterations"],
                                                            [list_of_comments, comment_regex, "comment"],
                                                            [list_of_indices, index_regex, "indecies"],
                                                            [list_of_offsets, offsets_regex, "offsets"],
                                                            [list_of_unvocalized_strings, unvocalized_string_regex, "unvocalized_strings"],
                                                            [list_of_vocalized_strings, vocalized_string_regex, "vocalized_strings"],
                                                            [list_of_vocalized_inputs, vocalized_input_string_regex, "vocalized_inputs"],
                                                            [list_of_pos, pos_string_regex, "pos_strings"],
                                                            [list_of_glosses, gloss_string_regex, "gloss_strings"],
                                                            [list_of_lemmas, lemma_regex, "lemmas"]]:
                                try:
                                    a_list.append(a_regex.findall(pos_block)[0])
                                except IndexError:
                                    if a_name == "lemmas":
                                        on.common.log.report("lemma", "failed to find lemma",
                                                             "Failed to find lemma for " + on.common.util.unicode2buckwalter(pos_block))
                                        list_of_lemmas.append("lemma_not_set")
                                    else:
                                        raise Exception("Didn't find any %s in %s (%s)" % (a_name, ("\n" + pos_block).replace("\n", "\n     "), pos_filename))

                        # temporarily copying the lists to another list used earlier
                        actual_word_list = [] + list_of_input_strings
                        buckwalter_word_list = [] + list_of_b_transliterations
                        lemma_list = [] + list_of_lemmas


                        on.common.log.debug("len(actual_word_list): %s" % (len(actual_word_list)), on.common.log.DEBUG, on.common.log.MAX_VERBOSITY)
                        on.common.log.debug("actual_word_list: %s" % (actual_word_list), on.common.log.DEBUG, on.common.log.MAX_VERBOSITY)
                        on.common.log.debug("len(buckwalter_word_list): %s" % (len(buckwalter_word_list)), on.common.log.DEBUG, on.common.log.MAX_VERBOSITY)
                        on.common.log.debug("buckwalter_word_list: %s" % (buckwalter_word_list), on.common.log.DEBUG, on.common.log.MAX_VERBOSITY)
                        on.common.log.debug("len(lemma_list): %s" % (len(lemma_list)), on.common.log.DEBUG, on.common.log.MAX_VERBOSITY)
                        on.common.log.debug("lemma_list: %s" % (lemma_list), on.common.log.DEBUG, on.common.log.MAX_VERBOSITY)



                        if(len(actual_word_list) != len(buckwalter_word_list)
                           or
                           len(actual_word_list) != len(lemma_list)):
                            on.common.log.debug("len(actual_word_list): %s" % (len(actual_word_list)), on.common.log.DEBUG, on.common.log.MAX_VERBOSITY)
                            on.common.log.debug("len(buckwalter_word_list): %s" % (len(buckwalter_word_list)), on.common.log.DEBUG, on.common.log.MAX_VERBOSITY)
                            on.common.log.debug("len(lemma_list): %s" % (len(lemma_list)), on.common.log.DEBUG, on.common.log.MAX_VERBOSITY)
                            raise Exception("the three lists -- actual word, buckwalter word, and lemma should be the same length, or else some information might be missing from the .lemma file")

                        for i in range(0, len(actual_word_list)):
                            if(lemma_list[i] == "DEFAULT"
                               or
                               buckwalter_word_list[i] == ""):
                                on.common.log.debug("%s %s %s\n" % (actual_word_list[i].rjust(50), buckwalter_word_list[i].rjust(50), lemma_list[i].rjust(50)), on.common.log.DEBUG, on.common.log.MAX_VERBOSITY)
                    else:
                        on.common.log.report("lemma", "could not find lemma file", document_id=document_id, pos_filename=pos_filename, absolute_file_path=absolute_file_path)
                        HAVE_LEMMA_FILE = False
                else:
                    on.common.log.warning(version)
                    raise Exception("the code should not ever reach here.  this means that the version check earlier has failed to raise an error")


            document_word_index = 0 # this should keep track of the index of the word in the document
            problem_flag = False




            for i in range(0, len(parse_list)):

                on.common.log.debug("tree index: %s" % (i), on.common.log.DEBUG, on.common.log.MAX_VERBOSITY)
                tree_id = "%s@%s" % (i, document_id)
                on.common.log.debug("tree id: %s" % (tree_id), on.common.log.DEBUG, on.common.log.MAX_VERBOSITY)
                on.common.log.debug("parse: %s" % (parse_list[i]), on.common.log.DEBUG, on.common.log.MAX_VERBOSITY)

                a_language = subcorpus_id.split("@")[-2]

                # sometimes there are multiple parses in on a line,
                # and so only the first parse is recognized by the
                # script, and it produced a silent error of ignoring
                # the other parse(s) therefore we encapsulate all
                # arabic parse lines in a top level parenthesis so
                # that all sentences come under one tree
                if a_language == "ar" and not parse_list[i].startswith("(TOP"):
                    parse_list[i] = "(TOP %s )" % (parse_list[i])


                try:

                    #print i, parse_list[i]
                    a_tree = tree.from_string(parse_list[i], id=tree_id, document_tag=self.tag)

                    def strip_traces(s):
                        x=re.sub("\*-\d+$", "*", s)
                        return re.sub("\*-\d+ ", "* ", x)

                    # we need to compare versions that don't have the
                    # trace numbers because an intentional change is
                    # to move the trace numbers.
                    try:
                        a_sentence_from_leaves = strip_traces(" ".join(a_leaf.word for a_leaf in a_tree))
                    except Exception:
                        print parse_list[i]
                        raise

                    a_sentence_from_flat_parse = strip_traces(on.common.util.parse2word(parse_list[i]))

                    if(a_sentence_from_leaves != a_sentence_from_flat_parse):

                        problem_flag = True
                        on.common.log.status("\n", parse_list[i], "\n",
                                             a_sentence_from_leaves,
                                             a_sentence_from_flat_parse, "\n")

                    a_tree.document_id = self.document_id
                    a_tree.id = tree_id

                    if(len(sentence_id_list) > 0):  # assuming that if it is > 0, it will be equal to parse_list length
                        a_tree.sentence_id = sentence_id_list[i]
                        a_tree.paragraph_id = paragraph_id_list[i]
                        a_tree.headline_flag = headline_flag_list[i]


                    # all these functions are here because i was
                    # trying to keep from passing the document id to
                    # tree object (or, maybe i should)
                    #

                    a_tree.initialize_ids(self.language)

                    a_tree.label_child_indices()

                    try:
                        a_tree.check_subtrees_fix_quotes()
                    except Exception:
                        print parse_list[i]
                        raise

                    # lets process the syntactic links
                    a_tree.initialize_syntactic_links()

                    # lets associate lemmas with words in the tree for arabic
                    for a_leaf in a_tree.leaves():
                        if not a_leaf.is_trace():
                            if self.language == "ar" and HAVE_LEMMA_FILE:
                                lemma_lemma = list_of_lemmas[document_word_index]

                                if version not in self.V2_ARABIC:

                                    coarse_sense = ""
                                    if "_" in lemma_lemma and lemma_lemma != "lemma_not_set":
                                        try:
                                            lemma_lemma, coarse_sense = lemma_lemma.split("_")
                                        except ValueError:
                                            print lemma_lemma
                                            raise

                                    a_leaf.lemma_object = lemma(list_of_input_strings[document_word_index],
                                                                list_of_b_transliterations[document_word_index],
                                                                list_of_comments[document_word_index],
                                                                list_of_indices[document_word_index],
                                                                list_of_offsets[document_word_index],
                                                                list_of_unvocalized_strings[document_word_index],
                                                                list_of_vocalized_strings[document_word_index],
                                                                list_of_vocalized_inputs[document_word_index],
                                                                list_of_pos[document_word_index],
                                                                list_of_glosses[document_word_index],
                                                                lemma_lemma, coarse_sense,
                                                                a_leaf.id)

                                    lemma_type(a_leaf.lemma_object.lemma)

                                a_leaf.lemma = lemma_lemma

                            a_leaf.document_word_index = document_word_index
                            document_word_index += 1


                    if(self.language == "ar"):
                        if(len(a_tree.children) != 1):
                            on.common.log.report("treebank", "FOUND A FRAGMENTED PARSE",
                                                 "tree id: %s\n" % (tree_id))

                    self.tree_hash[tree_id] = a_tree
                    self.tree_ids.append(tree_id)
                except Exception, e:
                    #on.common.log.report("treebank", "SOME PROBLEM WITH THE PARSE",
                    #                     "tree_id: %s\n" % (tree_id) + str(e))
                    raise

            if problem_flag:
                on.common.log.error("PROBLEM FLAG RAISED", False)


    def __getitem__(self, index):
        treeids_or_treeid = self.get_tree_ids()[index]
        if type(treeids_or_treeid) == type([]):
            return [self.get_tree(treeid) for treeid in treeids_or_treeid]
        return self.get_tree(treeids_or_treeid)

    def __len__(self):
        return len(self.get_tree_ids())

    def get_trees(self):
        return self.tree_hash

    def set_trees(self, tree_hash):
        self.tree_hash = tree_hash


    def get_tree_ids(self):
        return self.tree_ids

    def get_tree(self, tree_id):
        return self.tree_hash[tree_id]

    @property
    def tag(self):
        try:
            a_tag, a_layer = self.extension.rsplit("_", 1)
            if a_layer != "parse":
                raise Exception("%s != parse" % a_layer)
            return a_tag
        except Exception:
            return "gold"

    def get_coreference_document(self):
        return self.coreference_document

    def set_coreference_document(self, a_coreference_document):
        self.coreference = a_coreference_document

    def get_plain_text(self):
        a_text = ""
        count = 0
        for a_tree_id in self.tree_ids:
            a_tree = self.tree_hash[a_tree_id]
            a_text = "%s\n%s: %s" % (a_text, count, a_tree.get_word_string())
            count = count + 1
        return a_text


    def onf(self, a_cursor=None, COLS=120):

        tr = []

        #
        # lets print the proposition and sense objects attached to the
        # leaves of trees
        #
        i=0

        cur_coref_section = None
        seen_coref_chains = []

        def append_coref_info():
            if not seen_coref_chains:
                return

            tr.append("=" * COLS)

            a = "Coreference chains for section %s:" % cur_coref_section

            tr.append(a)
            tr.append("-"*(len(a)))
            tr.append("")

            for a_coref_chain in seen_coref_chains:
                tr.append("    Chain %s (%s)" % (a_coref_chain.identifier, a_coref_chain.type))
                for a_coref_link in a_coref_chain:
                    link_type = a_coref_link.type
                    if link_type == a_coref_chain.type:
                        link_type = ""

                    a = "        %s %s " % (
                        link_type.ljust(max(len(t) for t in on.corpora.coreference.coreference_link_type.allowed)),
                        ("%s.%s-%s" % (a_coref_link.sentence_index,
                                       a_coref_link.start_token_index,
                                       a_coref_link.end_token_index)).ljust(10))
                    tr.append(a + wrap(a_coref_link.string, COLS-len(a), len(a), indent_first_line=False))
                tr.append("")

        for a_tree in self:
            sys.stderr.write('.')

            if cur_coref_section != a_tree.coref_section:
                append_coref_info()
                cur_coref_section = a_tree.coref_section
                seen_coref_chains = []

            for a_leaf in a_tree:
                for a_coref_link in a_leaf.start_coreference_link_list:
                    if a_coref_link.coreference_chain not in seen_coref_chains:
                        seen_coref_chains.append(a_coref_link.coreference_chain)

            tr.append("-" * COLS)
            tr.append("")
            tr.append(a_tree.onf(COLS=COLS))
            tr.append("")

        append_coref_info()

        return "\n".join(tr)


    def __repr__(self):
        return "tree_document instance, id=%s, trees:\n%s" % (
            self.document_id, on.common.util.repr_helper(enumerate(a_tree.id for a_tree in self)))




    def get_words(self, sgml_safe=False, skip_traces=False, buckwalter=False, vocalized=True):
        """ return a list of the words in this document.  If sgml_safe
        is given, use -RAB- and -LAB- for < and > """

        def clean(s):
            if sgml_safe:
                return s.replace("<", "-LAB-").replace(">", "-RAB-")
            return s

        return [clean(a_leaf.get_word(buckwalter, vocalized))
                for a_tree in self
                for a_leaf in a_tree.leaves()
                if not (a_leaf.is_trace() and skip_traces)]



    def write_to_db(self, cursor):
        for a_tree in self:
            for a_subtree in a_tree.subtrees():
                a_subtree.write_to_db(cursor)


    def dump_view(self, a_cursor=None, out_dir="", buckwalter=False, vocalized=True):
        # write view file

        lemma_lines = []
        with codecs.open(on.common.util.output_file_name(self.document_id, self.extension, out_dir), "w", "utf-8") as f:
            for a_tree in self:
                f.write("%s\n" % (a_tree.pretty_print(buckwalter=buckwalter, vocalized=vocalized)))
                for a_leaf in a_tree:
                    if a_leaf.lemma_object:
                        lemma_lines.append("\n%s\n" % (a_leaf.lemma_object))

        if lemma_lines:
            with codecs.open(on.common.util.output_file_name(self.document_id, self.extension.replace("parse", "lemma"), out_dir), "w", "utf-8") as f:
                f.writelines(lemma_lines)


    def sentence_tokens_as_lists(self, make_sgml_safe=False, strip_traces=False):
        """ all the words in this document broken into lists by sentence

        So 'Good morning.  My name is John.' becomes:

        .. code-block:: python

           [['Good', 'morning', '.'],
            ['My', 'name', 'is', 'john', '.']]

        This doesn't actually return a list, but instead a generator.
        To get this as a (very large) list of lists, just call it as
        ``list(a_tree_document.sentence_tokens_as_lists())``

        If 'make_sgml_safe' is True, :func:`on.common.util.make_sgml_safe`
        is called for each word.

        If 'strip_traces' is True, trace leaves are not included in the output.

        """

        for a_tree in self:
            a_list = [a_leaf.get_word() for a_leaf in a_tree.leaves() if not strip_traces or not a_leaf.is_trace()]
            if make_sgml_safe:
                a_list = [on.common.util.make_sgml_safe(a_word) for a_word in a_list]
            yield a_list



class treebank(abstract_bank):
    """

    The treebank class represents a collection of
    :class:`tree_document` classes and provides methods for
    manipulating trees.  Further, because annotation in other banks
    was generally done relative to these parse trees, much of the code
    works relative to the trees.  For example, the
    :class:`on.corpora.document` data, their
    :class:`on.corpora.sentence` data, and their
    :class:`on.corpora.token` data are all derived from the trees.

    Attributes:

      .. attribute:: banks

         A hash from standard extensions (coref, name, ...) to bank instances

    """

    def __init__(self, a_subcorpus, tag, cursor=None, extension="parse", file_input_extension=None):
        abstract_bank.__init__(self, a_subcorpus, tag, extension)

        if not file_input_extension:
            file_input_extension = self.extension

        self.tree_ids = []        # list of tree ids in this treebank
        self.tree_hash = {}       # tree_id2parse_hash
        self.num_trees = 0        # total number of trees in treebank
        self.tree_start_end_tuples_hash = {}
        self.banks = {} # standard extension -> bank instance

        if(cursor == None):
            sys.stderr.write("reading the treebank [%s] ..." % file_input_extension)
            input_files = self.subcorpus.get_files(file_input_extension)

            if not input_files:
                on.common.log.status("\n *** warning -- treebank %s: no files with extension %s" % (self.extension, file_input_extension))
                return

            for a_file in input_files:

                document_id = "%s@%s" % (a_file.document_id, a_subcorpus.id)

                filename = a_file.physical_filename

                sys.stderr.write(".")
                on.common.log.debug("doc id: %s" % (document_id), on.common.log.DEBUG, on.common.log.MAX_VERBOSITY)

                file = codecs.open(filename, "r", "utf-8")

                # join lines
                try:
                    one_parse_string = file.read()
                except UnicodeDecodeError:
                    on.common.log.report("treebank", "unicode decode error in file SERIOUS", fname=filename)
                    one_parse_string = ""
                finally:
                    file.close()


                if not one_parse_string:
                    file = codecs.open(filename, "r", "gb18030")
                    try:
                        one_parse_string = file.read()
                    finally:
                        file.close()

                if one_parse_string.startswith("file;unicode\t"):
                    on.common.log.status("\n *** warning -- file %s is a tdf not a parse file" % filename)
                    continue

                non_doubleparened_parse_string =  one_parse_string.replace("((","").replace("))","").replace("\n", " ")

                if not re.match(r".*\([A-Z]", non_doubleparened_parse_string) or ")" not in non_doubleparened_parse_string:
                     on.common.log.status("\n *** warning -- file %s does not contain S-expressions" % filename)
                     continue

                # strip any leading, following spaces
                one_parse_string = one_parse_string.strip()

                if(a_subcorpus.language_id == "ch"):

                    def strip_empties(s):
                        """ remove trees that are empty in that they have the double wide EMPTY """

                        lines = s.split("\n")

                        n_lines = []
                        unsure = []

                        def contains_EMPTY(s):
                            for oc in [[ord(c) for c in t] for t in zip(s[0:], s[1:], s[2:], s[3:], s[4:])]:
                                if oc == [65317, 65325, 65328, 65332, 65337]:
                                    return True
                            return False

                        for line in lines:
                            if line.startswith("<segment "):
                                if not unsure:
                                    unsure.append(line)
                                else:
                                    raise Exception("bad parse file (segment) " + filename + "\n"
                                                    + "unsure=" + str(unsure) + "\n"
                                                    + "line=" + line)
                            elif not unsure:
                                n_lines.append(line)
                            else:
                                unsure.append(line)

                            if len(unsure) == 3:
                                if not unsure[2].startswith("</segment"):
                                    raise Exception("bad parse file (/segment) " + filename)

                                if not contains_EMPTY(unsure[1]):
                                    for u in unsure:
                                        n_lines.append(u)
                                unsure = []

                        if unsure:
                            raise Exception("bad parse file (/unsure) " + filename)

                        return "\n".join(n_lines)


                    if False:
                        one_parse_string = strip_empties(one_parse_string)

                    s_e_re_a = re.compile("<segment\s+id=\".*?\"\s+start=\"(.*?)\"\s+end=\"(.*?)\">", re.M|re.S)
                    s_e_re_b = re.compile("<segment\s+id=''.*?''\s+start=''(.*?)''\s+end=''(.*?)''>", re.M|re.S)

                    self.tree_start_end_tuples_hash[a_file.document_id] = []
                    self.tree_start_end_tuples_hash[a_file.document_id] += s_e_re_a.findall(one_parse_string)
                    self.tree_start_end_tuples_hash[a_file.document_id] += s_e_re_b.findall(one_parse_string)

                dup_parse_string = "" + one_parse_string

                # get headline information
                headlines = on.common.util.headline_re.findall(dup_parse_string)

                headline_string = ""
                if(len(headlines) > 0):
                    # assuming that there can only be one headline
                    if(len(headlines) != 1):
                        raise Exception("we are assuming that there is only one headline in the document")

                    headline_string = headlines[0]


                headline_sentence_id_hash = {}
                for item in on.common.util.sentence_id_para_re.findall(headline_string):
                    headline_sentence_id_hash[item[2]] = 0

                on.common.log.debug(headline_sentence_id_hash, on.common.log.DEBUG, on.common.log.MAX_VERBOSITY)

                # initialize the date
                a_date = None
                a_list = on.common.util.date_re.findall(dup_parse_string)
                if(len(a_list) > 0):
                    a_date = a_list[0]
                    on.common.log.debug(a_date, on.common.log.DEBUG, on.common.log.MAX_VERBOSITY)

                # initialize the doc_no
                a_doc_no = None
                a_list = on.common.util.doc_id_re.findall(dup_parse_string)
                if(len(a_list) > 0):
                    a_doc_no = a_list[0]
                    on.common.log.debug(a_doc_no, on.common.log.DEBUG, on.common.log.MAX_VERBOSITY)


                sentence_id_list = []
                paragraph_id_list = []
                headline_flag_list = [] # value of 1 if a headline else 0
                paragraph_index = -1  # outside paragraphs
                for item in on.common.util.sentence_id_para_re.findall(dup_parse_string):
                    if(item[0] == "S"):
                        on.common.log.debug("adding S", on.common.log.DEBUG, on.common.log.MAX_VERBOSITY)
                        sentence_id_list.append(item[2])
                        paragraph_id_list.append(paragraph_index)  # sentence is outside a paragraph
                        if(headline_sentence_id_hash.has_key(item[2])):
                            headline_flag_list.append(1)
                        else:
                            headline_flag_list.append(0)

                    if(item[0] == "P"):
                        on.common.log.debug("adding P", on.common.log.DEBUG, on.common.log.MAX_VERBOSITY)
                        paragraph_index = paragraph_index + 1


                on.common.log.debug("%d: %s" % (len(sentence_id_list), str(sentence_id_list)), on.common.log.DEBUG, on.common.log.MAX_VERBOSITY)
                on.common.log.debug("%d: %s" % (len(paragraph_id_list), str(paragraph_id_list)), on.common.log.DEBUG, on.common.log.MAX_VERBOSITY)
                on.common.log.debug("%d: %s" % (len(headline_flag_list), str(headline_flag_list)), on.common.log.DEBUG, on.common.log.MAX_VERBOSITY)


                one_parse_substrings = []
                for a_line in one_parse_string.split("\n"):
                    if not a_line.startswith(";"):
                        one_parse_substrings.append(a_line)
                one_parse_string = "\n".join(one_parse_substrings)


                #----- ONLY THE CHINESE FILES HAVE SGML TAGS IN THEM. DOING THIS FOR THE ENGLISH TEXT CAN DELETE LEGITIMATE TEXT -----#
                one_parse_string_with_sgml = None
                if(a_subcorpus.language_id == "ch"):
                    # now strip the parse_string of any sgml tags
                    one_parse_string = on.common.util.doc_id_re.sub("", one_parse_string)
                    one_parse_string = on.common.util.date_re.sub("", one_parse_string)

                    one_parse_string_with_sgml = one_parse_string
                    one_parse_string = on.common.util.sgml_tag_re.sub("", one_parse_string)


                one_parse_string = re.sub("\n+", "\n", one_parse_string)
                one_parse_string = one_parse_string.strip()
                on.common.log.debug("'%s'" % (one_parse_string), on.common.log.DEBUG, on.common.log.MAX_VERBOSITY)

                # list of parses
                parse_list = one_parse_string.split("\n(")

                # reintroduce the ( in the 1 to nth parses (excluding the 0th)
                k=0
                for k in range(1, len(parse_list)):
                    parse_list[k] = "(" + parse_list[k]

                l=0
                for l in range(0, len(parse_list)):
                    parse_list[l] = on.common.util.compress_space(parse_list[l]).strip()
                    parse_list[l] = on.common.util.tighten_curly_braces(parse_list[l]).strip()
                    parse_list[l] = re.sub(r"^\( ", "(TOP ", parse_list[l]).strip()
                    parse_list[l] = re.sub(r"^\(\(", "(TOP (", parse_list[l]).strip()


                # now fill the hash
                m=0
                for m in range(0, len(parse_list)):
                    key = "%s@%s" % (m, document_id)
                    self.tree_ids.append(key)

                    self.tree_hash[key] = parse_list[m]

                    self.num_trees = self.num_trees + 1


                if (a_subcorpus.language_id == "ch" and
                    self.tree_start_end_tuples_hash.has_key(a_file.document_id) and
                    self.tree_start_end_tuples_hash[a_file.document_id] and
                    len(self.tree_start_end_tuples_hash[a_file.document_id]) != len(parse_list)):

                    raise Exception("""
there is something wrong in the assumptions of timings for chinese parses, please check
whether there are more than one parses per timing range, and correct accordingly:

    number of time tuples: %s
         number of parses: %s
    """ % (len(self.tree_start_end_tuples_hash[a_file.document_id]), len(parse_list)))

                # create a tree_document object out of the list of parses for this tree_document
                a_tree_document = tree_document(document_id, parse_list, sentence_id_list, headline_flag_list,
                                                paragraph_id_list, filename, self, self.subcorpus.id, extension=self.extension)
                self.append(a_tree_document)

            sys.stderr.write(" %s trees in the treebank\n" % self.num_trees)
        else:
            pass


    def write_timing_file(self):
        for a_document_id in self.tree_start_end_tuples_hash:
            a_timings_file = open("%s/%s.timing" % (self.subcorpus.base_dir, a_document_id), "w")
            for a_tuple in self.tree_start_end_tuples_hash[a_document_id]:
                a_timings_file.write("%s %s\n" % (a_tuple[0], a_tuple[1]))
            a_timings_file.close()


    def inform_enriched(self, a_bank):
        """ record that we've been enriched with this bank
        """

        potential_bank = a_bank.extension.split("_")[-1]
        if potential_bank in self.banks:
            raise Exception("Treebank %s already has attribute %s" % (self.id, potential_bank))
        self.banks[potential_bank] = a_bank

    def enrich_treebank(self, a_treebank):
        """ this doesn't make sense for treebanks

        You ask other banks to enrich a treebank, not the other way
        around.  For example:

        .. code-block:: python

           a_sense_bank = ...
           a_treebank = ...
           a_sense_bank.enrich_treebank(a_treebank)

        """
        raise Exception("treebank.enrich_treebank not supported")


    sql_table_name = "treebank"

    sql_create_statement = \
"""
create table treebank
(
  id varchar(255) not null collate utf8_bin primary key,
  subcorpus_id varchar(255) not null,
  tag varchar (255) not null,
  foreign key (subcorpus_id) references subcorpus.id
)
default character set utf8;
"""



    # sql insert statement for the syntactic_link table
    sql_insert_statement = \
"""
insert into treebank
(
  id,
  subcorpus_id,
  tag
) values(%s, %s, %s)
"""



    @classmethod
    def from_db(cls, a_subcorpus, a_tag, a_cursor, affixes=None):
        sys.stderr.write("reading the treebank ...")
        a_cursor.execute("""select * from treebank where subcorpus_id = '%s';""" % (a_subcorpus.id))

        a_treebank = treebank(a_subcorpus, a_tag, a_cursor)

        # now get document ids for this treebank
        a_cursor.execute("""select document.id from document where subcorpus_id = '%s';""" % (a_subcorpus.id))
        document_rows = a_cursor.fetchall()

        # and process each document
        for document_row in document_rows:
            a_document_id = document_row["id"]

            if not on.common.util.matches_an_affix(a_document_id, affixes):
                continue

            sys.stderr.write(".")

            # create an empty tree_document
            a_tree_document = tree_document(a_document_id, [], [], [], [], [], a_treebank, a_subcorpus.id,
                                            a_cursor, extension=a_treebank.extension)



            # now get the sentences in this document
            a_cursor.execute("""select tree.id from tree where document_id = '%s'""" % (a_document_id))
            tree_rows = a_cursor.fetchall()

            a_parse_list = []
            for tree_row in tree_rows:
                sentence_id = tree_row["id"]
                a_tree = on.corpora.tree.tree.from_db_fast(sentence_id, a_cursor)
                a_tree.document_id = a_document_id

                # process the tree object.  this has to be here fore legacy reasons
                # let's set the ids of all sub trees
                a_tree.initialize_ids(a_document_id.split('@')[-2])

                # now lets label the child indices
                a_tree.label_child_indices()

                # lets process the traces
                a_tree.check_subtrees_fix_quotes()

                # lets process the syntactic links
                a_tree.initialize_syntactic_links()

                a_parse = a_tree.to_string()
                a_tree_document.parse_list.append(a_parse)


                a_tree_id = a_tree.id

                # some treebank wide variables that the treebank cares about
                a_treebank.tree_ids.append(a_tree.id)           # list of tree ids in this treebank
                a_treebank.tree_hash[a_tree_id] = a_parse       # tree_id2parse_hash
                a_treebank.num_trees = a_treebank.num_trees + 1 # total number of trees in treebank

                # fill in the tree_document values
                a_tree_document.tree_ids.append(a_tree_id)
                a_tree_document.tree_hash[a_tree_id] = a_tree

                a_tree.load_lemmas_from_db(a_cursor)


            # now add the tree document to the treebank
            a_treebank.append(a_tree_document)

        sys.stderr.write("\n")
        return a_treebank



    def dump_onf(self, a_cursor=None, out_dir=""):
        for a_tree_document in self:
            try:
                with codecs.open(on.common.util.output_file_name(a_tree_document.document_id, "onf", out_dir), "w", "utf-8") as f:
                    sys.stderr.write("writing ONF for %s ..." % (a_tree_document.document_id))
                    f.write(a_tree_document.onf(a_cursor=a_cursor))
                    sys.stderr.write("\n")
            except Exception:
                on.common.log.status("error writing ONF for", a_tree_document.document_id)
                raise


