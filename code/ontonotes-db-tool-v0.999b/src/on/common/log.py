
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


# author sameer pradhan

"""
:mod:`log` -- Logging and reporting functions
-------------------------------------------------------

See:

 - :func:`error`
 - :func:`warning`

Functions:

  .. autofunction:: error
  .. autofunction:: warning
  .. autofunction:: info
  .. autofunction:: debug
  .. autofunction:: status

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
import pprint
import traceback
import StringIO
import codecs
from on.common.util import mkdirs, output_file_name, mkdirs, unicode2buckwalter, listdir_both

import base64

DEBUG = False
VERBOSITY = 0;

MIN_VERBOSITY = 0
MED_VERBOSITY = 5
MAX_VERBOSITY = 10
SUPER_VERBOSITY = 15
WARNING_COUNT = 0

#---- specific imports ----#

#output_stream = e_unicode_to_utf8
output_stream = sys.stderr


def error(error_string, terminate_program=True, current_frame=False):
    """Print error messages to stderr, optionally sys.exit. """

    if(current_frame == False):
        pretty_error_string = """

--------------------------------------------------------------------------------
                                       ERROR
--------------------------------------------------------------------------------
%s
--------------------------------------------------------------------------------

""" % (error_string)
    else:
        pretty_error_string = """

--------------------------------------------------------------------------------
                                       ERROR
--------------------------------------------------------------------------------
FILE: %s
LINE: %s
--------------------------------------------------------------------------------
%s
--------------------------------------------------------------------------------

""" % (current_frame.f_code.co_filename, current_frame.f_lineno, error_string)

    sys.stderr.write(pretty_error_string)

    if(terminate_program == True):
        sys.exit(1)




def warning(warning_string, verbosity=0):
    """ print warning string depending on the value of on.common.log.VERBOSITY """

    if(verbosity <= VERBOSITY):
        output_stream.write(u"""

--------------------------------------------------------------------------------
                                      WARNING
--------------------------------------------------------------------------------
%s
--------------------------------------------------------------------------------

""" % (warning_string))



def debug(debug_object, debug_flag, verbosity=0):
    if((debug_flag == True) and (verbosity <= VERBOSITY)):
        output_stream.write(str(debug_object) + "\n")



def show_locals(local_hash):
    print "\n"
    for key in local_hash.keys():
        print key.rjust(50), ": ", str(local_hash[key])[0:100]
        print

def bad_data(complaint_target, complaint_name, data_pointer, *r_msgs,  **kw_msgs):
    """ Record that the data in 'data_pointer' is invalid

    Bad data can sometimes be complained about to someone.  If so,
    create a file for this purpose under the ``bad_data`` directory.
    Also file a report with the ``report`` function.

    The format of additional positional and keyword arguments is the
    same as expected by :func:`report` .

    """

    if False:
        kw_msgs["data_pointer"] = data_pointer
        report(complaint_target + "-" + "complaints", complaint_name, *r_msgs, **kw_msgs)

        out_parent = "bad_data"
        out_dir = os.path.join(out_parent, complaint_target)
        for d in out_parent, out_dir:
            try:
                os.mkdir(d)
            except OSError:
                pass # already exists

        def clean(c):
            if c.isalnum():
                return c
            return "-"

        with codecs.open(os.path.join(out_dir, "".join([clean(c) for c in complaint_name]) + ".txt"), "a", "utf-8") as outf:
            outf.write("%s\n" % data_pointer)


def report(report_name, report_msg_title, *report_msgs, **kw_msgs):
    """ Add a report to the named report with the given title and contents.

    The report is created in the ``report`` subdir of the working
    directory with the name ``${report_name}_report.txt``.  So the code::

          report('coreference',
                 'corrupt input file',
                 'input file cnn_0022.coref failed to load')

    Will append the following pair of lines to the file
    ``./report/coreference_report.txt``::

          09847                          REPORT: corrupt input file
          09847  input file cnn_0022.coref failed to load

    The initial number (09847) is derived from the hash of the message
    title and so will (with high probability) be unique for each error
    message.

    Additional options include multiple complaints and keyword complaints::

          report('coreference',
                 'corrupt input file',
                 'input file cnn_0022.coref failed to load',
                 'check the output')

          report('coreference',
                 'corrupt input file',
                 'input file cnn_0022.coref failed to load',
                 fname=fn, doc_id=doc_id)

    If there is an active traceback, report appends it.  Much of the
    time this will be irrelevant, as the traceback could be from an
    exception that was caught and handled appropriately.  The philosophy
    is that it's better to have irrelevant information sometimes than
    miss out on useful information when we want it.

    """

    if False:
        trb = traceback.format_exc()

        try:
            os.mkdir("report")
        except OSError:
            pass # already exists

        lines = []

        lines.append("-"*70)
        lines.append(" "*15 + "REPORT: " + report_msg_title)
        lines.append("-"*70)

        for report_msg in report_msgs:
            lines += report_msg.split('\n')

        for report_key, report_msg in kw_msgs.iteritems():
            for report_msg in str(report_msg).split("\n"):
                lines.append("%s: %s" % (report_key, report_msg))

        if trb != 'None\n':
            lines.append("Stack Trace")
            lines += trb.split("\n")

        #lines.append("-"*70)

        prepend = ("%x" % hash(report_name + report_msg_title))[-5:].rjust(5) + "  "
        with codecs.open("report/" + report_name + "-report.txt", "a", "utf-8") as outf:
            outf.writelines([prepend + line + "\n" for line in lines])


def interpret_errcode(errcode):
    """ turn 13502 into ('split', 'ann_ref', 'Dropped for invalid format in the annotation reference path') """

    assert len(errcode) == 5
    errcat_given, errnum_given = errcode[:2], errcode[2:]

    for errcat_name in ERRS:
        errcat_found, errnames_found = ERRS[errcat_name]
        if errcat_found == errcat_given:
            for errname_found in errnames_found:
                errnum_found, err_str, err_pri = errnames_found[errname_found]
                if errnum_found == errnum_given:
                    return errcat_name, errname_found, err_str, err_pri
    raise Exception("Errcode %s not found" % errcode)


# note that if the first of three digits is '5', the program interprets this to mean drop the data
ERRS = {"universal":     ("00",
                          {"ok":               ("000", "Universal ok", 0.0)}),
        "split" :        ("13",
                          {"fields":           ("501", "Dropped for invalid number of fields", 10.0),
                           "ann_ref":          ("502", "Dropped for invalid format in the annotation reference path", 1.0),
                           "ann_ref_literals": ("503", "Dropped for invalid values for the first or third component of the annotation reference path", 1.1),
                           "genre":            ("504", "Dropped because genre specified in filename differs from genre on line", 4.0),
                           "ext_format":       ("505", "Dropped for bad extension format", 2.0),
                           "baselayer":        ("506", "Dropped because annotation is against the wrong sort of document (parse when sense, word when prop, etc)", 8.0),
                           "baseref":          ("507", "Dropped because annotation is against a non existent document or document version", 12.0)}),

        "prop" :         ("14",

                          # initial prop loading
                          {"invrel":           ("501", "Dropping for invalid REL index", 2.0),
                           "invpred":          ("502", "Dropping for invalid predicate type", 2.1),
                           #"invsense":         ("503", "Dropping for non-numeric frame sense", 3.2),
                           "invframe":         ("504", "Dropping for reference to nonexistent frame file", 3.3),
                           "invfsid":          ("505", "Dropping for reference to invalid frame set id within frame file", 10.2),
                           "nosense":          ("506", "Dropping for lack of frame sense", 10.3),
                           "notype":           ("507", "Dropping because an argument part is missing type information", 11.0),
                           "badtype":          ("508", "Dropping because an argument part has an illegal type", 11.1),
                           "wrongnpred":       ("509", "Dropping for wrong number of predicates", 2.2),
                           "nopripred":        ("510", "Dropping because we have no primary predicate", 2.3),
                           "badargtype":       ("511", "Dropping for bad argument type", 1.0),
                           "singlink":         ("512", "Dropping because a link is a singleton", 3.0),
                           "mlinka":           ("513", "Dropping because a link has no anchor", 3.1),
                           "invltype":         ("514", "Dropping because a link has an invalid type", 1.1),
                           #"auxrel":          ("515", "Dropping REL-only annotation on auxilliary verb", 0.5),
                           #"aux":             ("516", "Dropping annotation on auxilliary verb", 0.6),
                           "notinc":           ("115", "Warning that proposition not included in coverage calculations", .0001),
                           "nnotn":            ("517", "Dropping -n proposition on non-noun target", 0.2),
                           "vnotv":            ("518", "Dropping -v proposition on non-verb target", 0.3),
                           "hnotzero":         ("519", "Dropping because primary predicate is not at height zero", 1.5),
                           "ovargnt":          ("520", "Dropping because proposition has overlapping arguments even ignoring traces", 5.0),
                           "invrgp":           ("522", "Dropping because an argument pointer is invalid", 5.9),
                           "cornocoi-en":      ("523", "Dropping because nodes marked as coreferential are not coreferential in the tree", 4.5),
                           "cornocoi-nonen":   ("124", "Warning because nodes marked as coreferential are not coreferential in the tree", .0045),
                           "badlemma":         ("123", "Warning that the lemma we have for the leaf disagrees with lemma field", 0.0012),
                           "ovarg":            ("121", "Warning that proposition has overlapping trace arguments", 0.0009),
                           "syntaxerr":        ("524", "Dropping because prop had a syntax error", 8.0), # raised an exception in the prop handling code
                           "linksemi":         ("525", "Dropping because prop had an ICH-indicating semicolon in a link", 3.3),
                           "tracepred":        ("526", "Dropping because the predicate is coindexed in the tree with some other node", 3.4),
                           "invargno":         ("527", "Dropping for numbered argument that is not allowed for this role", 10.25),
                           "invsense":         ("531", "Dropping for non-numeric non-XX frame sense", 7.23),
                           "invsenseXX":       ("532", "Dropping for having frame sense XX", 3.2),
                           "modalXX":          ("533", "Dropping proposition on chinese VV (modal) verb with no arguments and XX frame sense", 0.1345),

                          # duplicates
                           "identdup":         ("541", "Dropping identical duplicate", 0.4),
                           "nonidentdup":      ("542", "Dropping non-identical duplicate", 12.0),

                          # copy to new trees
                           "notarget":         ("550", "No target found for node", 10.4),
                           "notracetarget":    ("551", "No target found for trace node", 10.5),
                           "leafdiff":         ("152", "Warning that the subtree node maps to is not an exact match", 0.0013),
                           "nosubtree":        ("553", "Node is not a subtree in revised tree", 6.0),
                           "spanstrees":       ("554", "Node spans multiple revised trees", 7.0),
                           "modtok":           ("155", "Warning that the node has different tokenization in new tree", 0.0007),
                           "modinstrace":      ("556", "Node had a trace inserted or modified", 8.1), # we can't reliably tell these apart
                           "deltrace":         ("557", "Node had a trace deleted", 8.2),
                           "prop_modinstrace": ("558", "Tree had a trace inserted or modified, not inside any argument span", 8.01), # we can't reliably tell these apart
                           "prop_deltrace":    ("559", "Tree had a trace delete, not inside any argument span", 8.02),

                           "copyok":           ("051", "Copied to new trees with no issues", 0.1) }),

        "sense" :        ("15",

                          # initial sense loading
                          {"oob":              ("501", "Dropping for index out of bounds", 5.0),
                           "badlemma":         ("502", "Dropping because the lemma we have for the leaf disagrees with lemma field", 2.0),
                           "posmm":            ("503", "Dropping because the POS of the leaf disagrees with the pos field or the leaf is an aux verb", 0.5),
                           "invsense":         ("504", "Dropping for invalid non-XXX sense", 6.0),
                           "invsenseXXX":      ("505", "Dropping for invalid XXX sense", 6.1),
                           "badtree":          ("506", "Dropping because annotation is against an unknown tree", 4.0),
                           "invfields":        ("507", "Dropping because annotation has the wrong number of fields", 3.0),
                           "blankfield":       ("508", "Dropping because annotation has a blank entry for some field", 2.5),

                           "notinc":           ("115", "Warning that sense is not included in coverage calculations", .00001),

                           # copy to new trees
                           "unaligned":        ("558", "Dropping because leaf in old tree has no counterpart in new tree", 0.2),

                           "copyok":           ("051", "Successfully copied to new trees", 0.1)})}


# sanity check ERRS
for category, (ignore, errdict) in ERRS.items():
    warnings = [] # (errval, short_errno, errname, errexpl)
    errors = [] # (errval, short_errno, errname, errexpl)
    for errname, (short_errno, errexpl, errval) in errdict.items():
        x = (errval, short_errno, errname, errexpl)
        if short_errno[0] == "5":
            errors.append(x)
        else:
            warnings.append(x)

    for (errval, short_errno, errname, errexpl) in warnings:
        assert "Drop" not in errexpl, errexpl

    for (errval, short_errno, errname, errexpl) in errors:
        assert "Warning" not in errexpl, errexpl

    if warnings and errors:
        warnings.sort()
        errors.sort()

        highest_warning_value, w_short_errno, w_errname, w_errexpl = warnings[-1]
        lowest_error_value, e_short_errno, e_errname, e_errexpl = errors[0]

        if highest_warning_value >= lowest_error_value:
            raise Exception("Misordered warnings and errors: (%s, %s, %s, %s) >= (%s, %s, %s, %s) in %s" % (
                highest_warning_value, w_short_errno, w_errname, w_errexpl,
                lowest_error_value, e_short_errno, e_errname, e_errexpl,
                category))



def _write_reject(where, dropped_from, errcomms, opcode, rejection):
    """
    where is either:
      ['fname', fname]
    or
      ['docid', document_id, data_sort]

    errcoms is a list in the form:
      [[errcode, comments], e... ]

    where 'errcode' and 'comments' are defined as:

      comments -- list of comment lines.  If there are literal
                  newlines in a comment line, that's fine and new line
                  will be indented.  Comments may be the empty list
                  for one or all errorcodes.

      errcode  -- a short string to be looked up in the ERRS table.

    """

    if False:
        if where[0] == "fname":
            fname_base = where[1]
        else:
            assert where[0] == "docid"
            document_id, data_sort = where[1:]
            try:
                fname_base = output_file_name(document_id, data_sort)
            except IndexError:
                print document_id, data_sort
                raise
            try:
                mkdirs(os.path.dirname(fname_base))
            except Exception:
                pass # already exists

        fname = fname_base + ".rejects"

        with codecs.open(fname, "a", "utf8") as outf:
            errnums = []

            try:
                for errcode, comments in errcomms:
                    errnum = "%s%s" % (ERRS[dropped_from][0],
                                       ERRS[dropped_from][1][errcode][0])
                    errmsg = ERRS[dropped_from][1][errcode][1]
                    errnums.append(errnum)
                    outf.write("; %s %s\n" % (errnum, errmsg))
                    for line in comments:
                        line = unicode2buckwalter(line)

                        indent=" "*6
                        try:
                            line = indent + line.replace("\n", "\n" + indent + "   ")
                        except Exception:
                            print "%r" % line
                            raise
                        for subline in line.split("\n"):
                            outf.write("; %s\n" % subline)
            except ValueError:
                pprint.pprint(errcomms)
                raise

            outf.write("%s %s %s\n;\n;\n" % (opcode, ",".join(errnums), rejection))

def adjust(where, dropped_from, errcomms, line_find, line_replace):
    """ where, dropped_from, and errcomms are interpreted by _write_reject """

    _write_reject(where, dropped_from, errcomms, "COPY", "%s %% %s" % (line_find.strip(), line_replace.strip()))

def reject(where, dropped_from, errcomms, line_reject):
    """ where, dropped_from, and errcomms are interpreted by _write_reject """
    """ where, dropped_from, errcodes, and comments are interpreted by _write_reject """
    _write_reject(where, dropped_from, errcomms, "REJECT", line_reject.strip())


def info(text, newline=True):
    """ write the text to standard error followed by a newline """

    sys.stderr.write("%s%s" % (text, "\n" if newline else ""))

def print_trace():
    """ print the traceback to stderr """

    sbuf = StringIO.StringIO()
    traceback.print_exc(file = sbuf)
    excval = sbuf.getvalue()
    sys.stderr.write(excval)
    return excval

def status(*args):
    """ write each argument to stderr, space separated, with a trailing newline """
    sys.stderr.write(" ".join([str(a) for a in args]) + "\n")
