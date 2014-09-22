"""
Usage: python files_from_db.py -c config_file
"""

import on
import on.common
import on.common.util

@on.common.util.register_config("FilesFromDb", "out_dir", required=True, section_required=True)
def start():
    config = on.common.util.load_options(positional_args=False)
    a_ontonotes = on.ontonotes(config)
    
    a_cursor = on.ontonotes.db_cursor(config)
    out_dir = config["FilesFromDb", "out_dir"]

    if not a_ontonotes:
        raise Exception("Failed to load anything")

    for a_subcorpus in a_ontonotes:
        for a_bank in a_subcorpus.itervalues():
            a_bank.dump_view(a_cursor, out_dir)

if __name__ == "__main__":
    start()
