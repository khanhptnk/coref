import on
import on.common
import on.common.util
from on.common.util import bunch
import codecs

def process_subcorpus_a(a_subcorpus):
    td_originals = 0
    td_translations = 0

    for a_treebank in a_subcorpus.all_banks("parse"):
        for a_tree_document in a_treebank:
            b = bunch(td=a_tree_document)

            b.leaves = 0
            b.senses = 0
            b.props = 0
            b.originals = 0
            b.translations = 0

            if a_tree_document.original:
                td_originals += 1
            td_translations += len(a_tree_document.translations)

            for a_tree in b.td:
                for a_leaf in a_tree:
                    b.leaves += 1
                    if a_leaf.on_sense:
                        b.senses += 1
                    if a_leaf.proposition:
                        b.props += 1
                b.originals += len(a_tree.originals)
                b.translations += len(a_tree.translations)

            print b.td.document_id, "(%s) has:" % b.td.tag

            print " -", b.leaves, "leaves"
            print " -", b.senses, "senses"
            print " -", b.props, "propositions"
            print " -", b.originals, "originals"
            print " -", b.translations, "translations"

    print "All documents combined have:"
    print " -", td_originals, "originals"
    print " -", td_translations, "translations"



def start():
    config = on.common.util.load_options(positional_args=False)
    a_ontonotes = on.ontonotes(config)

    for a_subcorpus in a_ontonotes:
        print "  >> processing subcorpus %s" % a_subcorpus.id
        process_subcorpus_a(a_subcorpus)

if __name__ == "__main__":
    start()
