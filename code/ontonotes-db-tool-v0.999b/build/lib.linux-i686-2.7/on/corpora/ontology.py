
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
:mod:`ontology` -- Ontology Annotation
-------------------------------------------------

.. autoclass:: ontology
.. autoclass:: upper_model
.. autoclass:: sense_pool
.. autoclass:: sense_pool_collection
.. autoclass:: concept
.. autoclass:: feature

.. autoexception:: no_such_parent_concept_error
.. autoexception:: no_such_parent_sense_pool_error

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
from on.common.log import status

import on.common.util

import on.corpora
import on.corpora.tree
import on.corpora.proposition
import on.corpora.coreference
import on.corpora.name
import on.corpora.sense

from collections import defaultdict

from on.common.util import insert_ignoring_dups


class ontology:
    def __init__(self, a_id, a_upper_model, a_sense_pool_collection, a_cursor=None):
        self.id = a_id
        self.upper_model = a_upper_model
        self.sense_pool_collection = a_sense_pool_collection


    def to_dot(self):

        a_dot_string = self.upper_model.to_dot() + "\n\t" + \
                       self.sense_pool_collection.to_dot()

        v_e_hash = {}
        v_e_list = a_dot_string.split("\n\t")

        for v_e in v_e_list:
            if(not v_e_hash.has_key(v_e)):
                v_e_hash[v_e] = v_e
            else:
                on.common.log.debug("ignoring duplicate vertex/edge", on.common.log.DEBUG, on.common.log.MAX_VERBOSITY)

        a_dot_string = ""
        for v_e in v_e_hash.keys():
            a_dot_string = a_dot_string + "\n\t" + v_e

        a_dot_string = a_dot_string.strip()
        a_dot_string = "digraph UM {\n\t" + a_dot_string + "\n}"
        return a_dot_string


    def write_to_db(self, a_cursor):
        self.upper_model.write_to_db(a_cursor)
        self.sense_pool_collection.write_to_db(a_cursor)

        # write the feature type table
        on.corpora.ontology.feature_type.write_to_db(a_cursor)


    def from_db(self, a_id, a_cursor):
        a_ontology = on.corpora.ontology.ontology(a_id, None, None, a_cursor)

        # might want to fetch the id from the database
        a_upper_model = on.corpora.ontology.upper_model.from_db("upper_model@%s" % (a_id), a_cursor)
        a_sense_pool_collection = on.corpora.ontology.sense_pool_collection.from_db("sense_pool_collection@%s" % (a_id), a_cursor)

        a_ontology.upper_model = a_upper_model
        a_ontology.sense_pool_collection = a_sense_pool_collection

        return a_ontology

    @staticmethod
    @on.common.util.register_config("corpus", "data_in", required=False)
    def from_files(config_or_ontology_dir):
        """ Given: either a string representing a the path to the ontology
        directory or a configuration file that defines the key
        (corpus, data_in) representing the parent directory of the
        ontology dir.

        Return: an instance of the ontology loaded from the filesystem"""

        def make_upper_model(um_fname):
            status("Loading upper model ...")
            with codecs.open(um_fname, "r", "utf8") as um_inf:
                return on.corpora.ontology.upper_model(
                    "upper_model@ontology@on",  um_inf.read())

        def make_sense_pools(sp_dir):
            status("Loading sense pools ...")
            return on.corpora.ontology.sense_pool_collection(
                "sense_pool_collection@ontology@on", sp_dir)

        try:
            ontology_dir = os.path.join(config_or_ontology_dir[
                "corpus", "data_in"], "ontology")
        except TypeError:
            ontology_dir = config_or_ontology_dir


        return ontology(
            "ontology@on",
            make_upper_model(os.path.join(ontology_dir, "upper-model.xml")),
            make_sense_pools(os.path.join(ontology_dir, "sense-pools")))



class sense_pool_type(on.corpora.abstract_open_type_table):
    type_hash = defaultdict(int)

    @classmethod
    def write_to_db(cls, a_cursor):
        pass


class sense_pool_collection:
    def __init__(self, a_id, root_dir, a_cursor=None):
        self.sense_pools = []
        self.id = a_id

        if(a_cursor == None):
            filenames = [ x for x in os.listdir(root_dir) if x[-4:] == ".xml" ]

            # make one pass to fill the sense_pool_type hash so we can
            # check for missing parent, related pools, etc.
            for filename in filenames:
                sense_pool_type(re.sub("\.xml$", "", filename))


            for filename in filenames:
                file_string = open("%s/%s" % (root_dir, filename)).read()

                try:
                    a_sense_pool = sense_pool(re.sub("\.xml$", "", filename), file_string)
                    self.sense_pools.append(a_sense_pool)
                except Exception:
                    on.common.log.report("ontology", "failed to initialize sense pool", fname=filename)

                #except no_such_parent_concept_error:
                #    on.common.log.error("""
                #found reference to a undefined concept, please correct the upper model
                #definition file and reload the data.  the reason for this action is
                #that this concept is not created, and therefore any successor concepts
                #would have a missing path to the root concept.  just deleting this
                #concept from the list of concepts won't help either because no
                #particular sequence in which the concepts are loaded is assumed, and
                #therefore there might have been a descendant that got added earlier
                #which had this one as the parent, and since our assumption that the
                #presense of this concept in the hash means that it would be created
                #successfully does not hold, we will have to rectify the error and load
                #the concepts once again.
                #""")
                #except no_such_parent_sense_pool_error:
                #    on.common.log.error("""
                #found reference to a undefined sense pool, please correct the sense pool
                #definition files and reload the data.  the reason for this action is
                #that this concept is not created, and therefore any successor concepts
                #would have a missing path to the root sense pool/concept.  just deleting this
                #concept from the list of concepts won't help either because no
                #particular sequence in which the concepts are loaded is assumed, and
                #therefore there might have been a descendant that got added earlier
                #which had this one as the parent, and since our assumption that the
                #presense of this concept in the hash means that it would be created
                #successfully does not hold, we will have to rectify the error and load
                #the sense pools once again.
                #""")
                #print "e"
        else:
            pass

    def to_dot(self, complete=False):
        dot_string = ""
        for a_sense_pool in self.sense_pools:
            dot_string = dot_string + a_sense_pool.to_dot()

        if(complete == True):
            dot_string = "digraph UM {\n\t" + dot_string.strip() + "\n}"

        return dot_string.strip()


    def write_to_db(self, a_cursor):
        for a_sense_pool in self.sense_pools:
            a_sense_pool.write_to_db(a_cursor)


    def from_db(self, a_id, a_cursor):
        # create the object
        a_sense_pool_collection = on.corpora.ontology.sense_pool_collection(a_id, None, a_cursor)

        # add sense pools to
        a_cursor.execute("""select concept_pool_type.id from concept_pool_type where concept_pool_type.type = 'pool'""")
        pool_rows = a_cursor.fetchall()

        for a_pool_row in pool_rows:
            a_pool_id = a_pool_row["id"]

            a_pool = on.corpora.ontology.sense_pool.from_db(a_pool_id, a_cursor)
            a_sense_pool_collection.sense_pools.append(a_pool)

        return a_sense_pool_collection


    from_db = classmethod(from_db)








class sense_pool:
    def __init__(self, a_sense_pool_id, a_sense_pool_string, a_cursor=None):
        self.id = a_sense_pool_id       # the file name of the .xml pool file
        self.commentary = ""            # the commentary tag in the .xml file
        self.description = ""           # the SPID field in the .xml file
        self.spid = ""
        self.fid = ""
        self.name = ""
        self.sense_list = []
        self.parent_concepts_list = []
        self.parent_pools_list = []
        self.related_concepts_list = []
        self.related_pools_list = []


        if(a_cursor == None):
            try:
                a_sense_pool_tree = ElementTree.fromstring(a_sense_pool_string)
            except Exception:
                on.common.log.warning("there was some problem reading the XML file." + "\n" + a_sense_pool_string)
                raise

            self.description = on.common.util.get_attribute(a_sense_pool_tree, "SPID")

            self.spid = self.description
            self.fid = on.common.util.get_attribute(a_sense_pool_tree, "FID")
            self.name = on.common.util.get_attribute(a_sense_pool_tree, "NAME")



            for a_sense_tree in a_sense_pool_tree.findall(".//SENSE"):
                for a_sense_id_tree in a_sense_tree.findall(".//SENSEID"):
                    a_sense_string = a_sense_id_tree.text

                    sense_contents = a_sense_string.split(".")
                    if len(sense_contents) not in [4,5]:
                        raise Exception("invalid senseid " + a_sense_string)
                    else:
                        a_lemma = sense_contents[0]

                        a_lang = sense_contents[1] if len(sense_contents) == 5 else None
                        if a_lang != "e":
                            continue


                        a_type = sense_contents[-3]
                        a_pos = sense_contents[-2]
                        a_num = sense_contents[-1]

                        if(a_type not in "cbayoj"):
                            raise Exception("invalid senseid annotator" + a_sense_string)
                        elif(a_type == "y"):
                            a_on_sense_string = "%s@%s@%s@omega" % (a_lemma, a_num, a_pos)
                        else:
                            a_on_sense_string = "%s@%s@%s" % (a_lemma, a_num, a_pos)

                    self.sense_list.append(a_on_sense_string)



            for a_sub_to_tree in a_sense_pool_tree.findall(".//SUBTO"):
                for a_sub_tag_tree in a_sub_to_tree.findall(".//SUBTAG"):

                    a_id = a_sub_tag_tree.text.split("=")[0]

                    # check if it is a concept or pool and add it to the appropriate list
                    if(on.common.util.matches_pool_id_specification(a_id)):
                        if(sense_pool_type.type_hash.has_key(a_id)):
                            self.parent_pools_list.append(a_id)
                        else:
                            on.common.log.warning("found an undefined sense pool '%s' as being a parent" % (a_id))
                            raise no_such_parent_sense_pool_error
                    # else assume it to be a concept (as there is no specific definition for it)
                    else:
                        if( concept_type.type_hash.has_key(a_id) ):
                            self.parent_concepts_list.append(a_id)
                        else:
                            on.common.log.warning("found an undefined concept '%s' as being a parent" % (a_id))
                            raise no_such_parent_concept_error



            for a_relation_tree in a_sense_pool_tree.findall(".//RELATION"):
                for a_relation_tag_tree in a_relation_tree.findall(".//RELATIONTAG"):

                    a_id = a_relation_tag_tree.text.split("=")[0]

                    # check if it is a concept or pool and add it to the appropriate list
                    if(on.common.util.matches_pool_id_specification(a_id)):
                        if(sense_pool_type.type_hash.has_key(a_id)):
                            self.related_pools_list.append(a_id)
                        else:
                            on.common.log.warning("found an undefined sense pool '%s' as being related" % (a_id))
                            raise no_such_parent_sense_pool_error
                    # else assume it to be a concept (as there is no specific definition for it)
                    else:
                        if( concept_type.type_hash.has_key(a_id) ):
                            self.related_concepts_list.append(a_id)
                        else:
                            on.common.log.warning("found an undefined concept '%s' as being related" % (a_id))
                            raise no_such_parent_concept_error



            for a_commentary_tree in a_sense_pool_tree.findall(".//COMMENTARY"):
                self.commentary = a_commentary_tree.text


            on.common.log.debug("""
--------------------------------------------------------------------------------------------------------
sense pool
--------------------------------------------------------------------------------------------------------
description     : %s
commentary      : %s
senses          : %s
parent concepts : %s
parent pools    : %s
related concepts: %s
related pools   : %s
--------------------------------------------------------------------------------------------------------
""" % (self.description, self.commentary, str(self.sense_list), str(self.parent_concepts_list), str(self.parent_pools_list), str(self.related_concepts_list), str(self.related_pools_list)), on.common.log.DEBUG, on.common.log.MAX_VERBOSITY)
        else:
            pass





    def __repr__(self):
        return """
--------------------------------------------------------------------------------------------------------
sense pool
--------------------------------------------------------------------------------------------------------
description     : %s
commentary      : %s
senses          : %s
parent concepts : %s
parent pools    : %s
related concepts: %s
related pools   : %s
--------------------------------------------------------------------------------------------------------
""" % (self.description, self.commentary, str(self.sense_list), str(self.parent_concepts_list), str(self.parent_pools_list), str(self.related_concepts_list), str(self.related_pools_list))







    def to_dot(self):

        dot_string = ""
        a_sense_pool_id = self.id
        a_sense_pool_id = on.common.util.format_for_dot(a_sense_pool_id)

        for a_parent_concept in self.parent_concepts_list:
            a_parent_concept = on.common.util.format_for_dot(a_parent_concept)

            dot_string = dot_string + "\t\"" + a_parent_concept + "\" -> \"" + a_sense_pool_id + "\" [label=\"sub-concept\"];\n"

        for a_parent_pool in self.parent_pools_list:
            a_parent_pool = on.common.util.format_for_dot(a_parent_pool)

            dot_string = dot_string + "\t\"" + a_parent_pool + "\" -> \"" + a_sense_pool_id + "\" [label=\"sub-pool\"];\n"

        for a_related_concept in self.related_concepts_list:
            a_related_concept = on.common.util.format_for_dot(a_related_concept)

            dot_string = dot_string + "\t\"" + a_related_concept + "\" -> \"" + a_sense_pool_id + "\" [label=\"related-concept\"];\n"

        for a_related_pool in self.related_pools_list:
            a_related_pool = on.common.util.format_for_dot(a_related_pool)

            dot_string = dot_string + "\t\"" + a_related_pool + "\" -> \"" + a_sense_pool_id + "\" [label=\"related-pool\"];\n"

        for a_sense in self.sense_list:
            a_sense = on.common.util.format_for_dot(a_sense)

            dot_string = dot_string + "\t\"" + a_sense_pool_id + "\" -> \"" + a_sense + "\" [label=\"sense\"];\n"

        return dot_string


    sense_sql_table_name = "pool_sense"

    sense_sql_create_statement = \
"""
create table pool_sense
(
  id varchar(255),
  sense_id varchar (255)
)
default character set utf8;
"""

    sense_sql_insert_statement = \
"""insert into pool_sense
(
  id,
  sense_id
) values (%s, %s)
"""




    def write_senses_to_db(self, cursor):
        cursor.executemany("%s" % (self.sense_sql_insert_statement),
                           [ (self.id, a_sense_id) for a_sense_id in self.sense_list])


    def write_parents_to_db(self, cursor):
        data = []
        for thinglist, thing in [[self.parent_concepts_list, "concept"],
                                [self.parent_pools_list, "pool"]]:
            for a_parent_thing_id in thinglist:
                # making an assumption (looking at the current snapshot of
                # the data) that relations to concepts are always concepts
                data.append((self.id, a_parent_thing_id, "pool", thing))

        cursor.executemany("%s" % (on.corpora.ontology.concept.parent_sql_insert_statement), data)




    def write_relations_to_db(self, cursor):
        data = []

        for a_related_concept_id in self.related_concepts_list:

            a_tuple = (self.id, a_related_concept_id, "pool", "concept")
            data.append(a_tuple)

        for a_related_pool_id in self.related_pools_list:

            a_tuple = (self.id, a_related_pool_id, "pool", "pool")
            data.append(a_tuple)

        cursor.executemany("%s" % (on.corpora.ontology.concept.relation_sql_insert_statement), data)






    # this is the method that writes the concept to the database
    def write_to_db(self, a_cursor):

        insert_ignoring_dups(on.corpora.ontology.concept.sql_insert_statement, a_cursor,
                             self.id, self.spid, self.fid, self.name, self.commentary, "pool")

        # write other features, relations and parents to the db
        self.write_parents_to_db(a_cursor)
        self.write_relations_to_db(a_cursor)
        self.write_senses_to_db(a_cursor)



    @staticmethod
    def from_db(a_sense_pool_id, a_cursor):
        a_sense_pool = on.corpora.ontology.sense_pool(a_sense_pool_id, None, a_cursor)
        a_sense_pool.id = a_sense_pool_id

        # lets fill the concept attributes first
        a_cursor.execute("""select * from concept_pool_type where id = '%s'""" % (a_sense_pool_id))
        sense_pool_type_rows = a_cursor.fetchall()


        for a_sense_pool_type_row in sense_pool_type_rows:
            a_sense_pool.spid = a_sense_pool_type_row["spid"]
            a_sense_pool.fid = a_sense_pool_type_row["spid"]
            a_sense_pool.name = a_sense_pool_type_row["name"]
            a_sense_pool.commentary = a_sense_pool_type_row["commentary"]

            a_cursor.execute("""select * from concept_pool_parent where id = '%s' and type = 'concept'""" % (a_sense_pool_id))
            parent_concept_id_rows = a_cursor.fetchall()

            for a_parent_concept_id_row in parent_concept_id_rows:
                status("adding %s as parent concept" % (a_parent_concept_id_row["parent_id"]))
                a_sense_pool.parent_concepts_list.append(a_parent_concept_id_row["parent_id"])


            a_cursor.execute("""select * from concept_pool_parent where id = '%s' and type = 'pool'""" % (a_sense_pool_id))
            parent_pool_id_rows = a_cursor.fetchall()

            for a_parent_pool_id_row in parent_pool_id_rows:
                status("adding %s as parent pool" % (a_parent_pool_id_row["parent_id"]))
                a_sense_pool.parent_pools_list.append(a_parent_pool_id_row["parent_id"])





            a_cursor.execute("""select * from concept_pool_relation where id = '%s' and relation_type='concept'""" % (a_sense_pool_id))
            relation_concept_id_rows = a_cursor.fetchall()

            for a_relation_concept_id_row in relation_concept_id_rows:
                status("adding %s as being related concept" % (a_relation_pool_id_row["relation_id"]))
                a_sense_pool.related_concepts_list.append(a_relation_pool_id_row["relation_id"])

            a_cursor.execute("""select * from concept_pool_relation where id = '%s' and relation_type='pool'""" % (a_sense_pool_id))
            relation_pool_id_rows = a_cursor.fetchall()

            for a_relation_pool_id_row in relation_pool_id_rows:
                status("adding %s as being related pool" % (a_relation_pool_id_row["relation_id"]))
                a_sense_pool.related_pools_list.append(a_relation_pool_id_row["relation_id"])





            a_cursor.execute("""select * from pool_sense where id = '%s'""" % (a_sense_pool_id))
            sense_id_rows = a_cursor.fetchall()

            for a_sense_id_row in sense_id_rows:
                a_sense_id = a_sense_id_row["sense_id"]
                status("adding %s as a sense in this pool" % (a_sense_id))
                a_sense_pool.sense_list.append(a_sense_id)

        return a_sense_pool





class no_such_parent_concept_error(exceptions.Exception):
    pass









class no_such_parent_sense_pool_error(exceptions.Exception):
    pass








class upper_model:
    def __init__(self, a_id, a_um_string, a_cursor=None):
        self.id = a_id
        self.concepts = []

        if(a_cursor == None):
            try:
                a_um_tree = ElementTree.fromstring(a_um_string)        # lower case all the data in the upper model
            except Exception:
                on.common.log.warning("there was some problem reading the XML file." + "\n" +
                                      a_um_string)
                raise


            # it is important to note that in the upper model, each sensepool is a concept definition

            # make one pass over the concepts to fill the concept hash
            for a_sensepool_tree in a_um_tree.findall(".//SENSEPOOL"):
                a_concept_id = on.common.util.get_attribute(a_sensepool_tree, "SPID")
                concept_type(a_concept_id)


            # now let's create the actual concepts, and verify the validity using the aforefilled hash
            k=0
            for a_sensepool_tree in a_um_tree.findall(".//SENSEPOOL"):

                try:
                    a_concept = concept(ElementTree.tostring(a_sensepool_tree))
                    self.concepts.append(a_concept)
                except no_such_parent_concept_error:
                    on.common.log.error("""
    found reference to a undefined concept, please correct the upper model
    definition file and reload the data.  the reason for this action is
    that this concept is not created, and therefore any successor concepts
    would have a missing path to the root concept.  just deleting this
    concept from the list of concepts won't help either because no
    particular sequence in which the concepts are loaded is assumed, and
    therefore there might have been a descendant that got added earlier
    which had this one as the parent, and since our assumption that the
    presense of this concept in the hash means that it would be created
    successfully does not hold, we will have to rectify the error and load
    the concepts once again.
    """)
        else:
            pass





    def to_dot(self, complete=False):

        dot_string = ""
        for a_concept in self.concepts:
            dot_string = dot_string + "\n\t" + a_concept.to_dot()

        if(complete == True):
            dot_string = "digraph UM {\n\t" + dot_string.strip() + "\n}"

        return dot_string.strip()



    def write_to_db(self, a_cursor):
        for a_concept in self.concepts:
            a_concept.write_to_db(a_cursor)




    def from_db(self, a_id, a_cursor):
        a_upper_model = on.corpora.ontology.upper_model(a_id, None, a_cursor)

        a_cursor.execute("""select concept_pool_type.id from concept_pool_type where concept_pool_type.type = 'concept'""")
        concept_rows = a_cursor.fetchall()

        for a_concept_row in concept_rows:
            a_concept_id = a_concept_row["id"]
            a_concept = on.corpora.ontology.concept.from_db(a_concept_id, a_cursor)
            a_upper_model.concepts.append(a_concept)

        return a_upper_model



    from_db = classmethod(from_db)



class feature_type(on.corpora.abstract_open_type_table):
    type_hash = defaultdict(int)

    sql_table_name = "ontology_feature_type"

    sql_create_statement = \
"""
create table ontology_feature_type
(
  id varchar(255) not null collate utf8_bin primary key
)
default character set utf8;
"""


    sql_insert_statement = \
"""insert into ontology_feature_type
(
  id
) values (%s)
"""




class feature:

    def __init__(self, a_feature):

        if(a_feature[0] != "+"
           and
           a_feature[0] != "-"):
            on.common.log.warning("the feature string should have a + or - modifier along with it")

        (a_modifier, a_type) = re.findall("^(\+|\-)?(.+)$", a_feature)[0]

        self.modifier = a_modifier
        self.type = feature_type(a_type)


    def __repr__(self):
        return "%s%s" % (self.modifier, self.type.id)







class concept_type(on.corpora.abstract_open_type_table):
    type_hash = defaultdict(int)

    @classmethod
    def write_to_db(cls, a_cursor):
        pass


class concept:

    # global hash for consistency check
    concept_hash = {}

    def __init__(self, a_concept_string, a_cursor=None):
        self.spid = ""       #---- the SPID attribute
        self.name = ""       #---- the NAME attribute
        self.fid  = ""       #---- the FID attribute
        self.id = ""
        self.features = []
        self.parent_ids = []
        self.relation_ids = []
        self.commentaries = []

        #self.sub_concept_names = []

        if(a_cursor == None):
            try:
                on.common.log.debug("""
------------------------------ the concept string representation ---------------------------------------
%s
--------------------------------------------------------------------------------------------------------
""" % (a_concept_string), on.common.log.DEBUG, on.common.log.MAX_VERBOSITY)
                a_concept_tree = ElementTree.fromstring(a_concept_string)
            except Exception:
                on.common.log.warning("there was some problem reading the XML file." + "\n" + a_concept_string)
                raise


            self.spid = on.common.util.get_attribute(a_concept_tree, "SPID")
            self.name = on.common.util.get_attribute(a_concept_tree, "NAME")
            self.fid =  on.common.util.get_attribute(a_concept_tree, "FID")
            self.id = self.spid

            on.common.log.debug("came to create concept: %s" % (self.spid), on.common.log.DEBUG, on.common.log.MAX_VERBOSITY)

            a_commentary_index = 0   # there can be multiple commentaries, so let's just tag them with an index
            for a_commentary_tree in a_concept_tree.findall(".//COMMENTARY"):
                a_commentary = a_commentary_tree.text

                if(a_commentary == None):
                    a_commentary = ""

                self.commentaries.append(a_commentary)


            for a_feature_tree in a_concept_tree.findall(".//FEATURE"):
                for a_featuretag_tree in a_feature_tree.findall(".//FEATURETAG"):
                    a_feature = feature(a_featuretag_tree.text.lower())
                    self.features.append(a_feature)


            for a_relation_tree in a_concept_tree.findall(".//RELATION"):
                for a_relationtag_tree in a_relation_tree.findall(".//RELATIONTAG"):
                    a_relation_id = a_relationtag_tree.text

                    # since relation is just another concept, we won't create a new relation class
                    if(not concept_type.type_hash.has_key(a_relation_id)):
                        on.common.log.warning("found an undefined concept '%s' as being related" % (a_relation_id))
                        raise no_such_parent_concept_error
                    self.relation_ids.append(a_relation_id)


            for a_subto_tree in a_concept_tree.findall(".//SUBTO"):
                for a_subtag_tree in a_subto_tree.findall(".//SUBTAG"):
                    a_parent_id = a_subtag_tree.text

                    # since parent is just another concept, we won't create a new class
                    if(not concept_type.type_hash.has_key(a_parent_id)):
                        on.common.log.warning("found an undefined concept '%s' as being a parent" % (a_parent_id))
                        raise no_such_parent_concept_error

                    self.parent_ids.append(a_parent_id)

                on.common.log.debug("""
-------------------------------- the concept object contents  ------------------------------------------
%s
--------------------------------------------------------------------------------------------------------
""" % (self), on.common.log.DEBUG, on.common.log.MAX_VERBOSITY)

        else:
            pass


    def __repr__(self):
            return """
--------------------------------------------------------------------------------------------------------
concept
--------------------------------------------------------------------------------------------------------
               spid: %s
                fid: %s
               name: %s

           features: %s
 parent concept ids: %s
related concept ids: %s
       commantaries: %s
--------------------------------------------------------------------------------------------------------
""" % (self.spid, self.fid, self.name, str(self.features), str(self.parent_ids), str(self.relation_ids), " ".join(self.commentaries))





    def to_dot(self):
        dot_string = ""

        a_concept_id = self.id
        a_concept_id = on.common.util.format_for_dot(a_concept_id)
        dot_string = dot_string + "\t\"" + a_concept_id + "\" [id=\"" + a_concept_id + "\", commentary=\"" + on.common.util.format_for_dot(" ".join(self.commentaries)) + "\"];\n"

        for a_parent_concept_id in self.parent_ids:
            a_parent_concept_id = on.common.util.format_for_dot(a_parent_concept_id)
            dot_string = dot_string + "\t\"" + a_parent_concept_id + "\" -> \"" + a_concept_id + "\" [label=\"sub-concept\"];\n"

        for a_relation_id in self.relation_ids:
            a_relation_id = on.common.util.format_for_dot(a_relation_id)
            dot_string = dot_string + "\t\"" + a_concept_id + "\" -> \"" + a_relation_id + "\" [label=\"related\"];\n"

        return dot_string.strip()

    sql_table_name = "concept_pool_type"

    sql_create_statement = \
"""
create table concept_pool_type
(
  id varchar(255) not null collate utf8_bin primary key,
  spid varchar(255) not null,
  fid varchar(255) not null,
  name varchar(255) not null,
  commentary varchar(5000),
  type varchar(255)
)
default character set utf8;
"""


    sql_insert_statement = \
"""insert into concept_pool_type
(
  id,
  spid,
  fid,
  name,
  commentary,
  type
) values (%s, %s, %s, %s, %s, %s)
"""


    parent_sql_table_name = "concept_pool_parent"
    parent_sql_create_statement = \
"""
create table concept_pool_parent
(
  id varchar(255),
  parent_id varchar(255),
  type varchar(255),
  parent_type varchar(255)
)
default character set utf8;
"""

    parent_sql_insert_statement = \
"""insert into concept_pool_parent
(
  id,
  parent_id,
  type,
  parent_type
) values (%s, %s, %s, %s)
"""



    relation_sql_table_name = "concept_pool_relation"

    relation_sql_create_statement = \
"""
create table concept_pool_relation
(
  id varchar(255),
  relation_id varchar(255),
  type varchar(255),
  relation_type varchar(255)
)
default character set utf8;
"""

    relation_sql_insert_statement = \
"""insert into concept_pool_relation
(
  id,
  relation_id,
  type,
  relation_type
) values (%s, %s, %s, %s)
"""



    feature_sql_table_name = "concept_pool_feature"
    feature_sql_create_statement = \
"""
create table concept_pool_feature
(
  id varchar(255),
  feature_type varchar (255),
  feature_modifier varchar (255)

)
default character set utf8;
"""

    feature_sql_insert_statement = \
"""insert into concept_pool_feature
(
  id,
  feature_type,
  feature_modifier
) values (%s, %s, %s)
"""





    def write_parents_to_db(self, cursor):
        data = []

        for a_parent_id in self.parent_ids:

            # making an assumption (looking at the current snapshot of
            # the data) that relations to concepts are always concepts
            a_tuple = (self.id, a_parent_id, "concept", "concept")
            data.append(a_tuple)

        cursor.executemany("%s" % (self.parent_sql_insert_statement), data)






    def write_relations_to_db(self, cursor):
        data = []

        for a_relation_id in self.relation_ids:

            # making an assumption (looking at the current snapshot of
            # the data) that relations to concepts are always concepts
            a_tuple = (self.id, a_relation_id, "concept", "concept")
            data.append(a_tuple)

        cursor.executemany("%s" % (self.relation_sql_insert_statement), data)






    def write_features_to_db(self, cursor):
        data = []

        for a_feature_id in self.features:

            # making an assumption (looking at the current snapshot of
            # the data) that relations to concepts are always concepts
            a_tuple = (self.id, a_feature_id.type.id, a_feature_id.modifier)
            data.append(a_tuple)

        cursor.executemany("%s" % (self.feature_sql_insert_statement), data)






    # this is the method that writes the concept to the database
    def write_to_db(self, a_cursor):

        insert_ignoring_dups(self, a_cursor, self.id, self.spid, self.fid, self.name, " ".join(self.commentaries), "concept")


        # write other features, relations and parents to the db
        self.write_parents_to_db(a_cursor)
        self.write_relations_to_db(a_cursor)
        self.write_features_to_db(a_cursor)



    @staticmethod
    def from_db(a_concept_id, a_cursor=None):
        a_concept = on.corpora.ontology.concept(None, a_cursor)
        a_concept.id = a_concept_id

        # lets fill the concept attributes first
        a_cursor.execute("""select * from concept_pool_type where id = '%s'""" % (a_concept_id))
        concept_pool_type_rows = a_cursor.fetchall()

        for a_concept_pool_type_row in concept_pool_type_rows:
            a_concept.spid = a_concept_pool_type_row["spid"]
            a_concept.fid = a_concept_pool_type_row["spid"]
            a_concept.name = a_concept_pool_type_row["name"]
            a_concept.commentaries.append(a_concept_pool_type_row["commentary"])

            a_cursor.execute("""select * from concept_pool_parent where id = '%s'""" % (a_concept_id))
            parent_id_rows = a_cursor.fetchall()

            for a_parent_id_row in parent_id_rows:
                status("adding %s as parent" % (a_parent_id_row["parent_id"]))
                a_concept.parent_ids.append(a_parent_id_row["parent_id"])

            a_cursor.execute("""select * from concept_pool_relation where id = '%s'""" % (a_concept_id))
            relation_id_rows = a_cursor.fetchall()

            for a_relation_id_row in relation_id_rows:
                status("adding %s as being related" % (a_relation_id_row["relation_id"]))
                a_concept.relation_ids.append(a_relation_id_row["relation_id"])

            a_cursor.execute("""select * from concept_pool_feature where id = '%s'""" % (a_concept_id))
            feature_id_rows = a_cursor.fetchall()

            for a_feature_id_row in feature_id_rows:
                status("adding %s as a feature with %s modifier" % (a_feature_id_row["feature_type"], a_feature_id_row["feature_modifier"]))
                a_feature = on.corpora.ontology.feature("%s%s" % (a_feature_id_row["feature_modifier"], a_feature_id_row["feature_type"]))
                a_concept.features.append(a_feature)

        return a_concept



