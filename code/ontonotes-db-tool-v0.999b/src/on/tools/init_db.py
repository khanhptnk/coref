"""
:mod:`init_db` -- initialize the db and load inventories
-----------------------------------------------------------------

This code does two things: initializes the database (creating the
tables) and optionally loads the sense inventories and frame files.
This needs to happen before data can be loaded to the database with
the :mod:`on.tools.load_to_db` command.

For usage information, run this command with no arguments:

.. code-block:: bash

  $ python init_db.py

"""

import sys
import os.path
import on
import on.common
import on.common.util
from optparse import OptionParser

def load_sense_inventories(a_cursor, frame_set_hash, lang, top_dir):

    sb = on.corpora.sense.sense_bank

    if not os.path.exists(os.path.join(top_dir, lang, "metadata", "sense-inventories")):
        on.common.log.status("Not loading %s sense-inventories because they don't exist")
        return
    
    sb.write_sense_inventory_hash_to_db(
        sb.build_sense_inventory_hash(lang, top_dir + lang,
                                      lemma_pos_hash=None,
                                      a_frame_set_hash=frame_set_hash), a_cursor)

def load_frames(a_cursor, lang, top_dir):
    pb = on.corpora.proposition.proposition_bank

    if not os.path.exists(os.path.join(top_dir, lang, "metadata", "frames")):
        on.common.log.status("Not loading %s frames because they don't exist")
        return
    
    pb.write_frame_set_hash_to_db(
        pb.build_frame_set_hash(top_dir + lang, lang[:2]), a_cursor)

if __name__ == "__main__":

    positional_args = "db_name db_host db_user top_dir".split()
    
    parser = OptionParser(usage="usage: %prog [options] " + " ".join(positional_args))
    for long_option in "sense-inventories frames".split():
        parser.add_option("--" + long_option,
                          help="a comma separated list of languages to load %s for" % long_option)
    parser.add_option("-i", "--init",
                      action="store_true", dest="init", default=False,
                      help="initialize the db before doing anything else")
    
        
    options, args = parser.parse_args()

    if len(args) != len(positional_args):
        parser.error("expected %d positional arguments: %s" % ( len(positional_args), ", ".join(positional_args)))

    a_cursor = on.ontonotes.get_db_cursor(
        a_db   = args[0],
        a_host = args[1],
        a_user = args[2])

    top_dir = args[3] + "/data/"

    if options.init:
        on.ontonotes.initialize_db(a_cursor)

    a_frame_set_hash = {}
    if options.frames:
        for lang in options.frames.split(","):
            load_frames(a_cursor, lang, top_dir)
            a_frame_set_hash = {'DB': a_cursor}

    on.ontonotes.write_type_tables_to_db(a_cursor, write_closed_type_tables=True)

    if options.sense_inventories:
        for lang in options.sense_inventories.split(","):
            load_sense_inventories(a_cursor, a_frame_set_hash, lang, top_dir)

    on.ontonotes.write_type_tables_to_db(a_cursor, write_closed_type_tables=True)


