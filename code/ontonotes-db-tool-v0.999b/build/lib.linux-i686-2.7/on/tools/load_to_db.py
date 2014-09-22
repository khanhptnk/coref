"""
:mod:`load_to_db` -- load data from files to database
--------------------------------------------------------------


This code shows how to load ontonotes data to the database.  The
database needs to have already been initialized and the sense
inventories and frames need to have already been loaded.  See
:mod:`on.tools.init_db` to see how to do that.

"""

import on
import on.common
import on.common.util

def load_to_db():

    config = on.common.util.load_options(positional_args=False)
    a_ontonotes = on.ontonotes(config)
    a_cursor = a_ontonotes.db_cursor(config)

    for a_subcorpus in a_ontonotes:
        a_subcorpus.write_to_db(a_cursor)
    a_ontonotes.write_type_tables_to_db(a_cursor)

if __name__ == "__main__":
    load_to_db()
