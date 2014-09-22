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

:mod:`util` -- Utility functions
------------------------------------------

See:

 - Dealing with config file, command line options, etc:

   - :func:`load_options`
   - :func:`load_config`
   - :func:`parse_cfg_args`
   - :class:`FancyConfigParser`
   - :func:`register_config`

 - Buckwalter Arabic encoding:

   - :func:`buckwalter2unicode`
   - :func:`unicode2buckwalter`
   - :func:`devocalize_buckwalter`

 - DB:

   - :func:`esc`
   - :func:`insert_ignoring_dups`
   - :func:`is_db_ref`
   - :func:`make_db_ref`
   - :func:`is_not_loaded`
   - :func:`make_not_loaded`

 - SGML (``.name`` and ``.coref`` files):

   - :func:`make_sgml_safe`
   - :func:`make_sgml_unsafe`

 - File System:

   - :func:`matches_an_affix`
   - :func:`mkdirs`
   - :func:`output_file_name`
   - :func:`sopen`
   - :func:`listdir`
   - :func:`listdir_full`
   - :func:`listdir_both`

 - Other:

   - :func:`bunch`
   - :func:`get_lemma`


Functions:

  .. autofunction:: buckwalter2unicode
  .. autofunction:: unicode2buckwalter
  .. autofunction:: register_config
  .. autofunction:: insert_ignoring_dups
  .. autofunction:: matches_an_affix
  .. autofunction:: output_file_name
  .. autofunction:: get_lemma
  .. autofunction:: load_config
  .. autofunction:: mkdirs
  .. autofunction:: load_options
  .. autofunction:: parse_cfg_args
  .. autofunction:: listdir
  .. autofunction:: listdir_full
  .. autofunction:: listdir_both
  .. autofunction:: sopen

  .. exception:: NotInConfigError

     Because people might want to use a dictionary in place of a
     :mod:`ConfigParser` object, use a :exc:`NotInConfigError` as
     the error to catch for ``config[section, value]`` call.
     For example:

     .. code-block:: python

        try:
           load_data(config['Data', 'data_location'])
        except on.common.util.NotInConfigError:
           print 'Loading data failed.  Sorry.'

  .. autoclass:: bunch
  .. autofunction:: is_db_ref
  .. autofunction:: make_db_ref
  .. autofunction:: is_not_loaded
  .. autofunction:: make_not_loaded
  .. autofunction:: esc
  .. autofunction:: make_sgml_safe
  .. autofunction:: make_sgml_unsafe
  .. autoclass:: FancyConfigParser

"""

#---- standard library imports ----#
from __future__ import with_statement
import string
import sys
import re
import math
import os
import time
import getopt
import zlib
import gzip
import bz2
import base64
import codecs
import xml.dom.minidom
import ConfigParser
from optparse import OptionParser
from collections import defaultdict
import tempfile
import commands
import xml.etree.ElementTree as ElementTree
import difflib

#---- specific imports ----#
import on.common.log

#---- pre-compiled regular expressions ----#
multi_space_re = re.compile(r"\s+")
start_space_re = re.compile(r"^\s+")
end_space_re = re.compile(r"\s+$")

parse2pos_re = re.compile(r"\(([^ ][^ ]*) [^)(][^)(]*\)")
parse2word_re = re.compile(r"\([^ ][^ ]* ([^)(][^)(]*)\)")
parse2word_pos_re = re.compile(r"\(([^ ][^ ]*) ([^)(][^)(]*)\)")


date_re = re.compile(r"<DATE>\s*(.*?)\s*</DATE>", re.MULTILINE|re.DOTALL|re.UNICODE)
doc_re = re.compile(r"<DOC>.*?</DOC>", re.MULTILINE|re.DOTALL|re.UNICODE)
doc_id_re = re.compile(r'<DOC DOC(?:ID|NO)="([^"]+)">')
sgml_tag_re = re.compile(r"<.*?>", re.UNICODE)
coref_tag_re = re.compile(r"<(/)?COREF", re.UNICODE)
begin_coref_tag_re = re.compile(r"<COREF", re.UNICODE)
doc_tag_re = re.compile(r"<(/)?DOC>", re.UNICODE)
doc_full_re = re.compile(r'<DOC[^>]*>', re.MULTILINE|re.DOTALL|re.UNICODE)

headline_re = re.compile("<HEADLINE>(.*?)</HEADLINE>", re.DOTALL|re.MULTILINE|re.UNICODE)
headline_tag_re = re.compile("<(/)?HEADLINE>", re.DOTALL|re.MULTILINE|re.UNICODE)
sentence_id_para_re = re.compile("<(S|P)(\sID=(.*?))?>", re.DOTALL|re.MULTILINE|re.UNICODE)
header_re = re.compile("<(/)?HEADER>", re.UNICODE)
body_re = re.compile("<(/)?BODY>", re.UNICODE)
text_re = re.compile("<(/)?TEXT>", re.UNICODE)
sentence_re = re.compile("<(/)?S.*?>", re.UNICODE)
paragraph_re = re.compile("<(/)?P>", re.UNICODE)
ne_coref_tag_re = re.compile("\s+COREFID=.*?>", re.UNICODE)
start_pronoun_tag_re = re.compile("<PRONOUN.*?>", re.UNICODE)
end_pronoun_tag_re = re.compile("</PRONOUN>", re.UNICODE)

lcb_space_re = re.compile("\(\s+", re.UNICODE)
rcb_space_re = re.compile("\s+\)", re.UNICODE)

pool_id_specification_re = re.compile("[PE]\d+") # might also want to specify 4 digits instead of just one or more

buck2fsbuck = {">": "O",
               "<": "I",
               "&": "W",
               "'": "L",
               "|": "M",
               "*": "X"}

buck2uni = {"'": u"\u0621", # hamza-on-the-line
            "|": u"\u0622", # madda
            ">": u"\u0623", # hamza-on-'alif
            "&": u"\u0624", # hamza-on-waaw
            "<": u"\u0625", # hamza-under-'alif
            "}": u"\u0626", # hamza-on-yaa'
            "A": u"\u0627", # bare 'alif
            "b": u"\u0628", # baa'
            "p": u"\u0629", # taa' marbuuTa
            "t": u"\u062A", # taa'
            "v": u"\u062B", # thaa'
            "j": u"\u062C", # jiim
            "H": u"\u062D", # Haa'
            "x": u"\u062E", # khaa'
            "d": u"\u062F", # daal
            "*": u"\u0630", # dhaal
            "r": u"\u0631", # raa'
            "z": u"\u0632", # zaay
            "s": u"\u0633", # siin
            "$": u"\u0634", # shiin
            "S": u"\u0635", # Saad
            "D": u"\u0636", # Daad
            "T": u"\u0637", # Taa'
            "Z": u"\u0638", # Zaa' DHaa'
            "E": u"\u0639", # cayn
            "g": u"\u063A", # ghayn
            "_": u"\u0640", # taTwiil
            "f": u"\u0641", # faa'
            "q": u"\u0642", # qaaf
            "k": u"\u0643", # kaaf
            "l": u"\u0644", # laam
            "m": u"\u0645", # miim
            "n": u"\u0646", # nuun
            "h": u"\u0647", # haa'
            "w": u"\u0648", # waaw
            "Y": u"\u0649", # 'alif maqSuura
            "y": u"\u064A", # yaa'
            "F": u"\u064B", # fatHatayn
            "N": u"\u064C", # Dammatayn
            "K": u"\u064D", # kasratayn
            "a": u"\u064E", # fatHa
            "u": u"\u064F", # Damma
            "i": u"\u0650", # kasra
            "~": u"\u0651", # shaddah
            "o": u"\u0652", # sukuun
            "`": u"\u0670", # dagger 'alif
            "{": u"\u0671", # waSla
             }
"""Buckwalter to Unicode conversion table for decoding ASCII-encoded Arabic"""

uni2buck = {}
"""Unicode to Buckwalter conversion table for encoding Arabic as ASCII."""


# Iterate through all the items in the buck2uni dict.
for (key, value) in buck2uni.iteritems():
    # The value from buck2uni becomes a key in uni2buck, and vice
    # versa for the keys.
    uni2buck[value] = key

PUNCT=["#", "$", '"', "-LSB-", "-RSB-", "-LRB-", "-RRB-", "-LCB-", "-RCB-",
       "[", "]", "(", ")", "{", "}", "'", ",", ".", ":", "``", "''", "PUNC",
       "NUMERIC_COMMA"]

def usage():
    print >>sys.stderr, USAGE_TEXT
    sys.exit(1)




def tighten_curly_braces(a_string):
    a_string = lcb_space_re.sub("(", a_string)
    a_string = rcb_space_re.sub(")", a_string)
    return a_string


def compress_space( a_string ):
    a_string = multi_space_re.sub(" ", a_string)
    a_string = start_space_re.sub("", a_string)
    a_string = end_space_re.sub("", a_string)
    return a_string




def parse2word( a_parse ):
    word_list = parse2word_re.findall(a_parse)
    return string.join(word_list)




def remove_doc_tags(a_string):
    a_string = doc_tag_re.sub("", a_string)
    a_string = doc_full_re.sub("", a_string)
    #return a_string.strip()
    return a_string




def remove_sgml_tags(a_string, strip=True):
    a_string = sgml_tag_re.sub("", a_string)
    if(strip == True):
        return a_string.strip()
    else:
        return a_string



def remove_all_non_ne_sgml_tags(a_string):
    #a_string = date_re.sub("", a_string)
    a_string = a_string.replace("<DATE>", "").replace("</DATE>", "")
    a_string = headline_tag_re.sub("", a_string)
    a_string = sentence_re.sub("", a_string)
    a_string = paragraph_re.sub("", a_string)
    a_string = header_re.sub("", a_string)
    a_string = body_re.sub("", a_string)
    a_string = text_re.sub("", a_string)
    a_string = ne_coref_tag_re.sub(">", a_string)
    a_string = start_pronoun_tag_re.sub("", a_string)
    a_string = end_pronoun_tag_re.sub("", a_string)
    a_string = re.sub("\n+", "\n", a_string)

    #return a_string.strip()
    return a_string





def get_attribute(a_element_tree, attribute_name):
    if(a_element_tree.attrib.has_key(attribute_name)):
        return a_element_tree.attrib[attribute_name]
    else:
        on.common.log.warning("attribute \"%s\" not defined for %s" % (attribute_name, str(a_element_tree)))





# formats the string to remove dot-unfriendly characters
def format_for_dot(a_string):
    a_string = re.sub("\.a\.n", ".n", a_string)
    a_string = re.sub("\.aN", ".N", a_string)
    a_string = re.sub("\.", "\.", a_string)
    a_string = re.sub("\*", "\*", a_string)
    a_string = re.sub("\>", "\>", a_string)
    a_string = re.sub("#", "", a_string)
    a_string = re.sub("\"", "<quote>", a_string)
    return a_string


def matches_pool_id_specification(a_id):
    if(len(pool_id_specification_re.findall(a_id)) > 0):
        return True
    else:
        return False

def buckwalter2fsbuckwalter(b_word):
    """ convert traditional buckwalter to filename-safe buckwalter """
    if b_word is None:
        return b_word

    return "".join(buck2fsbuck.get(b_chr, b_chr) for b_chr in b_word)

def buckwalter2unicode(b_word, sgml_safety=True):
    """Given a string in Buckwalter ASCII encoded Arabic, return the Unicode version. """

    if sgml_safety:
        b_word = make_sgml_unsafe(b_word)


    return "".join(buck2uni.get(b_char, b_char) for b_char in b_word)

def unicode2buckwalter(u_word, sgml_safe=False, devocalize=False):
    """Given a Unicode word, return the Buckwalter ASCII encoded version.

    If ``sgml_safe`` is set, run the output through :func:`make_sgml_safe` before returning.

    If ``devocalize`` is set delete a,u,i,o before returning.

    """

    def to_buck(c):

        if not sgml_safe:
            return uni2buck.get(c,c)

        if c in '<>':
            return c
        return make_sgml_safe(uni2buck.get(c,c))

    s = "".join([to_buck(u_char) for u_char in u_word])
    if devocalize:
        s = devocalize_buckwalter(s)
    return s

def devocalize_buckwalter(buckwalter_s):
    return buckwalter_s.replace("a", "").replace("u", "").replace("i", "").replace("o", "")





def desubtokenize_annotations(a_string, add_offset_notations=False, delete_annotations=False):
    '''  returns new_string, num_annotations_changed

    where

      a_string:   something like pre-<COREF...>Tuesday</COREF>
      new_string: something like <COREF...>pre-Tuesday</COREF>

    or

      new_token: something like <COREF...S_OFF="4">pre-Tuesday</COREF>

    depending on add_offset_notations

    num_annotations_changed: in this case, 1

    '''

    a_string = a_string.replace("<TURN>", "-TURN-")
    a_string = re.sub(r"<(\+[^>]*\+)>", r"[\1]", a_string)

    def is_tag(x): return x.startswith("<")
    def is_close(x): return x.startswith("</")
    def is_open(x): return is_tag(x) and not is_close(x)

    a_string = re.sub(r" (</[^>]*>)", r"\1 ", a_string)
    a_string = re.sub(r"(<[^/>][^>]*>) ", r" \1", a_string)

    def spaceseparate(s):
        cur = []
        for c in s:
            if c.isspace():
                yield "".join(cur)
                cur = []
                yield c
            else:
                cur.append(c)
        if cur:
            yield "".join(cur)

    def tokenize(s):
        while s:
            if "<" not in s:
                for ss in spaceseparate(s):
                    yield ss
                return
            if s[0] != "<":
                nontag, s = s.split("<", 1)
                s = "<" + s
                for ss in spaceseparate(nontag):
                    yield ss

            if len(s.split(">")) == 1:
                yield s
                s = ""
            else:
                tag, s = s.split(">", 1)
                tag += ">"
                yield tag
        yield "\n"

    changes = 0

    new_tokens = []
    token_stack = [] # active tags -- form is (tag, token index, character offset)
    token_table = [] # all tags -- form is (open, close) where open and close are (tag, token index, character offset)

    for a_token in tokenize(a_string):
        def middle_of_token():
            if not new_tokens:
                return False
            if a_token.isspace():
                return False
            return not new_tokens[-1].isspace()

        def current_info():
            assert is_tag(a_token)
            if not middle_of_token():
                s_off = 0
                token_index = len(new_tokens)
            else:
                s_off = len(new_tokens[-1])
                token_index = len(new_tokens) - 1
            return a_token, token_index, s_off

        if is_close(a_token):
            token_table.append((token_stack.pop(), current_info()))
        elif is_open(a_token):
            token_stack.append(current_info())
        else:
            if middle_of_token():
                new_tokens[-1] = "%s%s" % (new_tokens[-1], a_token)
            else:
                new_tokens.append(a_token)

    if token_stack:
        for y in [x for x in new_tokens if x.strip()][-10:]:
            print y
    assert not token_stack, token_stack

    def table_sorter(a, b):
        (a_o_tag, a_o_t_idx, a_o_s_off), (a_c_tag, a_c_t_idx, a_c_s_off) = a
        (b_o_tag, b_o_t_idx, b_o_s_off), (b_c_tag, b_c_t_idx, b_c_s_off) = b

        if a_o_t_idx != b_o_t_idx:
            return cmp(b_o_t_idx, a_o_t_idx) # if one start on a later token, do it first
        if a_c_t_idx != b_c_t_idx:
            return cmp(a_c_t_idx, b_c_t_idx) # if one ends on an earlier token, do it first
        return cmp(a_c_s_off, b_c_s_off)     # if one ends on an eralier character, do it first

    token_table.sort(cmp=table_sorter)

    def add_s_off(tag, s_off):
        if s_off == 0:
            return tag

        return '%s-S_OFF="%s">' % (tag[:-1], s_off)

    def add_e_off(tag, e_off):
        if e_off == 0:
            return tag

        return '%s-E_OFF="%s">' % (tag[:-1], e_off)

    converted_token_table = []
    for (o_tag, o_t_idx, o_s_off), (c_tag, c_t_idx, c_s_off) in token_table:
        try:
            c_e_off = len(new_tokens[c_t_idx]) - c_s_off
        except IndexError:
            print list(enumerate(new_tokens))
            print c_tag, c_t_idx, c_s_off
            raise
        converted_token_table.append(((o_tag, o_t_idx, o_s_off), (c_tag, c_t_idx, c_e_off)))

    deletions = 0
    for (o_tag, o_t_idx, o_s_off), (c_tag, c_t_idx, c_e_off) in converted_token_table:
        if (o_s_off or c_e_off) and delete_annotations:
            deletions += 1
            continue

        if add_offset_notations:
            o_tag = add_e_off(add_s_off(o_tag, o_s_off), c_e_off)

        new_tokens[o_t_idx] = "%s%s" % (o_tag, new_tokens[o_t_idx])
        new_tokens[c_t_idx] = "%s%s" % (new_tokens[c_t_idx], c_tag)


    new_string = "".join(new_tokens)
    changes = new_string.count('-S_OFF="') + new_string.count('-E_OFF="') + deletions
    return "".join(new_tokens), changes




# section -> value -> (allowed_values, doc, required, section_required, allow_multiple)
__registered_config_options = defaultdict( dict )

def is_config_section_registered(section):
    return section in __registered_config_options

def is_config_registered(section, value, strict=False):
    if section not in __registered_config_options:
        return False
    return value in __registered_config_options[section] or (
        not strict and "__dynamic" in __registered_config_options[section])

def required_config_options(section):
    if not is_config_section_registered(section):
        return []
    return [value for value in __registered_config_options[section]
            if __registered_config_options[section][value][2]] # required

def required_config_sections():
    return [section for section in __registered_config_options if
            [True for value in __registered_config_options[section]
             if __registered_config_options[section][value][3]]] # section_required

def allowed_config_values(section, option):
    if not is_config_registered(section, option, strict=True):
        return []
    return __registered_config_options[section][option][0]

def allow_multiple_config_values(section, option):
    if not is_config_registered(section, option, strict=True):
        return []
    return __registered_config_options[section][option][4]

def print_config_docs(to_string=False):
    p = []
    p.append("")
    p.append("Allowed configuration arguments:")
    for section in sorted(__registered_config_options.iterkeys()):
        p.append("   Section " + section + ":")

        if section in required_config_sections():
            p[-1] += " (required)"

        for value, (allowed_values, doc, required, section_required, allow_multiple) in sorted(__registered_config_options[section].iteritems()):
            if value == "__dynamic":
                value = "note: other dynamically generated config options may be used"

            p.append("      " + value)
            if required:
                p[-1] += " (required)"

            if doc:
                p.append("         " + doc)
            if allowed_values:
                if allow_multiple:
                    p.append("         may be one or more of:")
                else:
                    p.append("         may be one of:")

                for allowed_value in allowed_values:
                    p.append("            " + allowed_value)
        p.append("")
    s = "\n".join(p)
    if to_string:
        return s
    else:
        on.common.log.status(s)

def register_config(section, value, allowed_values=[], doc=None, required=False, section_required=False, allow_multiple=False):
    """ make decorator so funcs can specify which config options they take.

    usage is:

    .. code-block:: python

      @register_config('corpus', 'load', 'specify which data to load to the db \
                                          in the format lang-genre-source')
      def load_banks(config):
          ...

    The special value '__dynamic' means that some config values are
    created dynamically and we can't verify if a config argument is
    correct simply by seeing if it's on the list.  Documentation is
    also generated to this effect.

    If ``allowed_values`` is non-empty, then check to see that the
    setting the user chose is on the list.

    If ``allow_multiple`` is True, then when checking whether only
    allowed values are being given the key is first split on
    whitespace and then each component is tested.

    If ``required`` is True, then if the section exists it must
    specify this value.  If the section does not exist, it is free to
    ignore this value.  See ``section_required`` .

    If ``section_required`` is True, then issue an error if
    ``section`` is not defined by the user.  Often wanted in
    combination with ``required`` .

    """

    __registered_config_options[section][value] = (allowed_values, doc, required, section_required, allow_multiple)
    return lambda f: f

def load_options(parser=None, argv=[], positional_args=True):
    """ parses sys.argv, possibly exiting if there are mistakes

    If you set parser to a ConfigParser object, then you have control
    over the usage string and you can prepopulate it with options you
    intend to use.  But don't set a ``--config`` / ``-c`` option;
    load_options uses that to find a configuration file to load

    If a parser was passed in, we return ``(config, parser, [args])``.
    Otherwise we return ``(config, [args])``.  Args is only included
    if ``positional_args`` is True and there are positional arguments

    See :func:`load_config` for details on the ``--config`` option.

    """

    def is_config_appender(arg):
        return "." in arg and "=" in arg and arg.find(".") < arg.find("=")

    parser_passed_in=parser
    if not parser:
        parser = OptionParser()

    parser.add_option("-c", "--config", help="the path to a config file to read options from")

    if argv:
        options, args = parser.parse_args(argv)
    else:
        options, args = parser.parse_args()

    config = load_config(options.config, [a for a in args if is_config_appender(a)])

    other_args = [a for a in args if not is_config_appender(a)]

    return_list = [config]
    if parser_passed_in:
        return_list.append(options)
    if other_args:
        if positional_args:
            return_list.append(other_args)
        else:
            raise Exception("Arguments %s not understood" % other_args)
    else:
        if positional_args:
            raise Exception("This program expects one or more positional arguments that are missing")

    if len(return_list) == 1:
        return return_list[0]
    else:
        return tuple(return_list)


class FancyConfigParserError(Exception):
    """ raised by :class:`FancyConfigParser` when used improperly """

    def __init__(self, vals):
        Exception.__init__(self, 'Config usage must be in the form "config[\'section\', \'item\']". '
                           'Given something more like "config[%s]".' % (", ".join("%r"%v for v in vals)))


class FancyConfigParser(ConfigParser.SafeConfigParser):
    """ make a config parser with support for config[section, value]

    raises :class:`FancyConfigParserError` on improper usage.

    """

    def __getitem__(self, vals):
        try:
            section, item = vals
        except (ValueError, TypeError):
            raise FancyConfigParserError(vals)
        return self.get(section, item)


    def __setitem__(self, vals, value):
        try:
            section, item = vals
        except (ValueError, TypeError):
            raise FancyConfigParserError(vals)
        return self.set(section, item, value)

    def __delitem__(self, vals):
        try:
            section, item = vals
        except (ValueError, TypeError):
            raise FancyConfigParserError(vals)

        self.remove_option(section, item)

def load_config(cfg_name=None, config_append=[]):
    """ Load a configuration file to memory.

    The given configuration file name can be a full path, in which
    case we simply read that configuration file.  Otherwise, if you
    give 'myconfig' or something similar, we look in the current
    directory and the home directory.  We also look to see if files
    with this name and extension '.conf' exist.  So for 'myconfig' we
    would look in the following places:

     * ./myconfig
     * ./myconfig.conf
     * [home]/.myconfig
     * [home]/.myconfig.conf

    Once we find the configuration, we load it.  We also extend
    ConfigParser to support ``[]`` notation.  So you could look up key
    ``k`` in section ``s`` with ``config[s,k]``.  See
    :func:`FancyConfigParser` .

    If config_append is set we use :func:`parse_cfg_args` and add any
    values it creates to the config object.  These values override any
    previous ones.

    """

    config = FancyConfigParser()

    if cfg_name:
        config_locs = [cfg_name + '.conf',
                       os.path.expanduser('~/.' + cfg_name + '.conf'),
                       cfg_name,
                       os.path.expanduser('~/.' + cfg_name)]
        l = config.read(config_locs)
        if not l:
            raise Exception("Couldn't find config file.  Looked in:" +
                            "".join(["\n - " + c for c in config_locs]) +
                            "\nto no avail.")


    for (section, key_name), value in parse_cfg_args(config_append).iteritems():
        if not config.has_section(section):
            config.add_section(section)
        config.set(section, key_name, value)

    problems = []
    for section in config.sections():
        if not is_config_section_registered(section):
            on.common.log.status("Ignoring unknown configuration section", section)
            continue
        for option in config.options(section):
            if not is_config_registered(section, option):
                problems.append("Unknown configuration variable %s.%s" % (section, option))
                continue

            value = config.get(section, option)
            allowed = allowed_config_values(section, option)
            multiple = allow_multiple_config_values(section, option)

            values = value.split() if multiple else [value]
            for value in values:
                if allowed and not value in allowed:
                    problems.append("Illegal value '%s' for configuration variable %s.%s.  Permitted values are: %s" %
                                    (value, section, option, ", ".join(["'%s'" % x for x in allowed])))

        for option in required_config_options(section):
            if not config.has_option(section, option):
                problems.append("Required configuration variable %s.%s is absent" % (section, option))

    for section in required_config_sections():
        if not config.has_section(section):
            problems.append("Required configuration section %s is absent" % section)

    if problems:
        print_config_docs()

        on.common.log.status("Configuration Problems:")
        for problem in problems:
            on.common.log.status("  " + problem)

        sys.exit(-1)

    return config

def mkdirs(long_path):
    """ Make the given path exist.  If the path already exists, raise an exception. """

    p_dir = os.path.split(long_path)[0]

    if p_dir and not os.path.exists(p_dir):
        mkdirs(p_dir)

    os.mkdir(long_path)

def division(numerator, denominator):
    """ if d = 0 return 0, otherwise return n/d """

    return numerator/denominator if denominator else 0

def output_file_name(doc_id, doc_type, out_dir=""):
    """Determine what file to write an X_document to

    doc_id: a document id
    doc_type: the type of the document, like a suffix (parse, prop, name, ...)
    out_dir: if set, make the output as a child of out_dir

    """

    pathname = doc_id.split("@")[0]

    language = {"en": "english",
                "ar": "arabic",
                "ch": "chinese"}[doc_id.split("@")[-2]]

    original_file_name = "%s/%s/%s/%s.%s" % ("data", language, "annotations",
                                             pathname, doc_type)

    if out_dir:
        original_file_name = os.path.join(out_dir, original_file_name)

    view_parent, view_base = os.path.split(original_file_name)

    if not os.path.exists(view_parent):
        try:
            on.common.util.mkdirs(view_parent)
        except OSError:
            pass # race condition

    return original_file_name


def get_lemma(a_leaf, verb2morph, noun2morph, fail_on_not_found=False):
    """ return the lemma for a_leaf's word

    if we have appropriate word2morph hashes, look the work up
    there.  Otherwise just return the word.  Functionally, for
    chinese we use the word itself and for english we have the
    hashes.  When we get to doing arabic we'll need to add a
    case.

    if fail_on_not_found is set, return "" instead of a_leaf.word if
    we don't have a mapping for this lemma.
    """

    if a_leaf.on_sense:
        pos = a_leaf.on_sense.pos
    elif a_leaf.tag.startswith("V"):
        pos = 'v'
    elif a_leaf.tag.startswith('N'):
        pos = 'n'
    else:
        pos = 'other'

    w = a_leaf.word.lower()

    return word2lemma(w, pos, verb2morph, noun2morph, fail_on_not_found)

def word2lemma(a_word, pos, verb2morph, noun2morph, fail_on_not_found=False):
    """ return the lemma for a word given its pos

    if we have appropriate word2morph hashes, look the work up
    there.  Otherwise just return the word.  Functionally, for
    chinese we use the word itself and for english we have the
    hashes.  When we get to doing arabic we'll need to add a
    case.

    if fail_on_not_found is set, return "" instead of a_leaf.word if
    we don't have a mapping for this lemma.
    """

    w = a_word.lower()
    for p, h in [['n', noun2morph],
                 ['v', verb2morph]]:


        if pos == p and h and w in h:
            if(h[w] == "sayyid"):
                print pos, w, h[w]
            return  h[w]

    return "" if fail_on_not_found else w


def parse_cfg_args(arg_list):
    """Parse command-line style config settings to a dictionary.

    If you want to override configuration file values on the command
    line or set ones that were not set, this should make it simpler.
    Given a list in format [section.key=value, ...] return a
    dictionary in form { (section, key): value, ...}.

    So we might have:

    .. code-block:: python

      ['corpus.load=english-mz',
       'corpus.data_in=/home/user/corpora/ontonotes/data/']

    we would then return the dictionary:

    .. code-block:: python

      { ('corpus', 'load') : 'english-mz',
        ('corpus', 'data_in') : '/home/user/corpora/ontonotes/data/' }

    See also :func:`load_config` and :func:`load_options`

    """

    if not arg_list:
        return {}

    config_append = {}

    for arg in arg_list:
        if len(arg.split("=")) != 2 or len(arg.split("=")[0].split('.')) != 2:
            raise Exception("Invalid argument; not in form section.key=value : " + arg)

        key, value = arg.split("=")
        config_append[tuple(key.split("."))] = value

    return config_append


def listdir(dirname):
    """List a dir's child dirs, sorted and without hidden files.

    Basically :func:`os.listdir`, sorted and without hidden (in the
    Unix sense: starting with a '.') files.

    """

    dl = [x for x in os.listdir(dirname)
          if x[0] != "." and x not in ["report", "bad_data"]]

    dl.sort()
    return dl

def listdir_full(dirname):
    """ A full path to file version of :func:`on.common.util.listdir`. """

    return [os.path.join(dirname, d) for d in listdir(dirname)]

def listdir_both(dirname):
    """ return a list of short_path, full_path tuples

    identical to ``zip(listdir(dirname), listdir_full(dirname))``

    """

    return [(d, os.path.join(dirname, d)) for d in listdir(dirname)]

# documentation for this is in common/__init__.rst
NotInConfigError = ConfigParser.NoOptionError



class bunch():
    """
    a simple class for short term holding related variables

    change code like:

    .. code-block:: python

      def foo_some(a_ontonotes, b_ontonotes):
        a_sense_bank = ...
        a_ontonotes.foo(a_sense_bank)
        a_...
        a_...

        b_sense_bank = ...
        b_ontonotes.foo(b_sense_bank)
        b_...
        b_...

        big_func(a_bar, b_bar)

    To:

    .. code-block:: python

      def foo_some():
        a = bunch(ontonotes=a_ontonotes)
        b = bunch(ontonotes=b_ontonotes)

        for v in [a,b]:
          v.sense_bank = ...
          v.ontonotes.foo(v.sense_bank)
          v. ...
          v. ...

        big_func(a.bar, b.bar)

    Or:

    .. code-block:: python

      def  foo_some():
        def foo_one(v):
          v.sense_bank = ...
          v.ontonotes.foo(v.sense_bank)
          v. ...
          v. ...
          return v

        big_func(foo_one(bunch(ontonotes=a_ontonotes)).bar,
                 foo_one(bunch(ontonotes=b_ontonotes)).bar)

    Basically it lets you group similar things.  It's adding hierarchy
    to the local variables.  It's a hash table with more convenient
    syntax.

    """

    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)


def is_db_ref(a_hash):
    """ Is this hash a reference to the database?

    If a hash (sense inventories, frames, etc) is equal to
    ``{'DB' : a_cursor}`` that means instead of using the hash as
    information we should go look for our information in the database
    instead.

    """

    return a_hash and a_hash.keys() == ['DB']

def make_db_ref(a_cursor):
    """ Create a hash substitute that means 'go look in the db instead'.

    See :func:`is_db_ref`

    """

    return {'DB': a_cursor}

def is_not_loaded(a_hash):
    """ Do we have no intention of loading the data a_hash is supposed to contain?

    If a hash has a single key 'NotLoaded' that means we don't
    intend to load that hash and we shouldn't complain about data
    inconsistency involving the hash.  So if we're loading senses and
    the sense_inventory_hash :func:`is_not_loaded` then we shouldn't
    drop senses for being references against lemmas that don't exist.
    """

    return a_hash and a_hash.keys() == ['NotLoaded']

def make_not_loaded():
    """ Create a hash substitute that means 'act as if you had this information'

    See :func:`is_not_loaded`

    """

    return {'NotLoaded' : 'NotLoaded'}


def esc(*varargs):
    """ given a number of arguments, return escaped (for mysql) versions of each of them """

    try:
        import MySQLdb
    except ImportError:
        raise Exception("Can't import MySQLdb -- why are you trying to call this function if you're not working with mysql?")

    return tuple([MySQLdb.escape_string(str(s)) for s in varargs])

def make_sgml_safe(s, reverse=False, keep_turn=True):
    """ return a version of the string that can be put in an sgml document

    This means changing angle brackets and ampersands to '-LAB-',
    '-RAB-', and '-AMP-'.  Needed for creating ``.name`` and
    ``.coref`` files.

    If keep_turn is set, <TURN> in the input is turned into [TURN], not turned into -LAB-TURN-RAB-

    """

    if not reverse and keep_turn:
        s = s.replace("<TURN>", "[TURN]")

    for f, r in [("<", "-LAB-"),
                 (">", "-RAB-"),
                 ("&", "-AMP-")]:
        if reverse:
            r, f = f, r
        s = s.replace(f, r)

    return s

def make_sgml_unsafe(s):
    """ return a version of the string that has real <, >, and &.

    Convert the 'escaped' versions of dangerous characters back to
    their normal ascii form.  Needed for reading .name and .coref
    files, as well as any other sgml files like the frames and the
    sense inventories and pools.

    See :func:`make_sgml_safe`

    """

    return make_sgml_safe(s, reverse=True)

def insert_ignoring_dups(inserter, a_cursor, *values):
    """ insert values to db ignoring duplicates

    The caller can be a string, another class instance or a class:

      string  : take to be an sql insert statement
      class   : use it's sql_insert_statement field, then proceed as with string
      instance: get it's __class__ and proceed as with class

    So any of the following are good:

    .. code-block:: python

      insert_ignoring_dups(self, a_cursor, id, tag)
      insert_ignoring_dups(cls,  a_cursor, id, tag)
      insert_ignoring_dups(self.__class__.weirdly_named_sql_insert_statement, a_cursor, id, tag)

    """

    import MySQLdb


    if type(inserter) == type(""):
        insert_statement = inserter
    else:
        if hasattr(inserter, "__class__"):
            inserter = inserter.__class__
        insert_statement = inserter.sql_insert_statement

    try:
        a_cursor.executemany("%s" % insert_statement, [esc(*values)])
    except MySQLdb.Error, e:
        if(str(e.args[0]) != "1062"):
            on.common.log.error("{%s, %s} %s %s" % (insert_statement, values, str(e.args[0]), str(e.args[1])))



def matches_an_affix(s, affixes):
    """ Does the given id match the affixes?

    Affixes = prefixes, suffixes

    Given either a four digit string or a document id, return whether
    at least one of the prefixes and at least one of the suffixes
    matches it

    """

    def helper(id_bit):

        if not affixes:
            return True

        prefixes, suffixes = affixes

        ok_for_prefixes, ok_for_suffixes = not prefixes, not suffixes

        for prefix in prefixes:
            if id_bit.startswith(prefix):
                ok_for_prefixes = True

        for suffix in suffixes:
            if id_bit.endswith(suffix):
                ok_for_suffixes = True

        return ok_for_prefixes and ok_for_suffixes

    if len(s) == 4:
        return helper(s)

    if "@" in s:
        s, rest = s.split("@", 1)
    elif "." in s:
        s, rest = s.rsplit(".", 1)

    id_bit_start = s.rfind("_")

    return id_bit_start == -1 or helper(s[id_bit_start+1 : id_bit_start + 5])


def wrap(val, cols, ind=0, indent_first_line=True):
    """ wrap the string in 'val' to use a maximum of 'cols' columns.  Lines are indented by 'ind'. """

    if val is None:
        return ""

    wrapped = []

    for s in val.split("\n"):
        while len(s) > cols:
            last_good_wrap_point = -1
            for i, c in enumerate(s):
                if c in ' ':
                    last_good_wrap_point = i
                if i >= cols and last_good_wrap_point != -1:
                    break
            if last_good_wrap_point != -1:
                wrapped.append(s[:last_good_wrap_point])
                s = s[last_good_wrap_point+1:]
            else:
                break
        if s:
            wrapped.append(s)

    a_str = ("\n" + " "*ind).join(w for w in wrapped)
    if indent_first_line:
        return " "*ind + a_str
    return a_str

def lpad(s, l):
    """ add spaces to the beginning of s until it is length l """
    s = str(s)
    return " "*max(0, (l - len(s))) + s

def rpad(s, l):
    """ add spaces to the end of s until it is length l """
    s = str(s)
    return  s + " "*max(0, (l - len(s)))


def repr_helper(tuple_gen_exp, ind=2):
    """ given a sequence of 2-tuples, return a nice string like:

    .. code_block:: python

       (1, 'hi'), (2, 'there'), (40, 'you') ->

    .. code_block:: python

       [ 1] : hi
       [ 2] : there
       [40] : you

    """


    lines = []

    k_v = list(tuple_gen_exp)

    if not k_v:
        return " "*ind + "(empty)"

    max_len_k = max(len(str(k_vp[0])) for k_vp in k_v)
    for k, v in sorted(k_v):
        lines.append(" "*ind + "[%s] : %s" % (lpad(k, max_len_k), v))
    return "\n".join(lines)



def clean_up_name_string(s):
    """ """

    s = s.replace("-TURN-", "<TURN>")

    for x in ["GPE", "ORG", "Cardinal", "NORP", "LOC",
              "Date", "Money", "Time", "Event", "FAC",
              "Language", "Law", "Ordinal", "Percent",
              "Product", "Quantity", "Work-of-art",
              "Work-of-Art"]:
        for follower in "->":
            s = s.replace('<'+x+follower, '<ENAMEX TYPE="'+x.upper()+'"' + follower)

        s = s.replace('</'+x+'>', '</ENAMEX>')


    s = s.replace('<PER>', '<ENAMEX TYPE="PERSON">')
    s = s.replace('</PER>', '</ENAMEX>')

    for x in ["NUMEX", "TIMEX"]:
        s = s.replace('<%s TYPE="' % x, '<ENAMEX TYPE="')
        s = s.replace('</%s>' % x, '</ENAMEX>')

    for x in ["CARDINAL", "MONEY", "ORDINAL", "PERCENT", "QUANTITY"]:
        s = re.sub('<ENA(MEX TYPE="%s">[^<]*</)ENAMEX>' % x, "<NU\\1NUMEX>", s)
    for x in ["DATE", "TIME"]:
        s = re.sub('<ENA(MEX TYPE="%s">[^<]*</)ENAMEX>' % x, "<TI\\1TIMEX>", s)

    return s


def cannonical_arabic_word(a_word):
    word = on.common.util.unicode2buckwalter(a_word, devocalize=True)

    for f, r in ((">", "A"),
                 ("<", "A"),
                 ("~", ""),
                 ("-", "")):
        word = word.replace(f, r)

    return word


def same_except_for_tokenization_and_hyphenization(s_1, s_2):
    return s_1.replace("- -", " ").replace(" ", "") == s_2.replace("- -", " ").replace(" ", "")



