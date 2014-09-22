import nltk
import re
import hobbs
from nltk.tree import *
from nltk.tokenize import *

def read_trees(filename):
  f = open(filename)
  raw = f.read()
  trees_raw = BlanklineTokenizer().tokenize(raw)
  trees = [ParentedTree.fromstring(s) for s in trees_raw]
  return trees 

def read_sentences(filename):
  f = open(filename)
  raw = f.read()
  sentences_raw = LineTokenizer().tokenize(raw)[2:-2]
  tokenizer = RegexpTokenizer('<\s*COREF[^>]*>|<\s*/\s*COREF>|\w+')
  sentences = [tokenizer.tokenize(s) for s in sentences_raw]
  return sentences

def traverse_tree(tree):
  if not(isinstance(tree, Tree)):
    if (tree in ['We', 'You', 'They', 'She', 'He', 'It']):
      return [tree.lower()]
    return [tree]
  tree.str = []
  for child in tree:
    tree.str.extend(traverse_tree(child))
  return tree.str

def get_gender(tree):
  tree.gender = 0
  # Not an 'NP' 
  if not(tree.label() in ['NP', 'NN', 'NNS', 'NNP', 'NNPS', 'PRP']):
    for child in tree:
      if (isinstance(child, str)):
        break
      get_gender(child)
    return
  # Is an 'NP' 
  # Trival cases
  # Proper noun
  if (tree.label() == 'NNP'):
    if (tree.str[0] in male_names):
      tree.gender = 1
    else:
      if (tree.str[0] in female_names):
        tree.gender = 2
      else:
        tree.gender = 3
  if (tree.label() == 'NNPS'):
    tree.gender = 3
  # Personal pronoun
  if (tree.label() == 'PRP'):
    if (tree.str[0] in ['he', 'him']):
      tree.gender = 1
    else:
      if (tree.str[0] == ['she', 'her']):
        tree.gender = 2
      else:
        tree.gender = 3
  # Noun
  if (tree.label() == 'NN'):
    if (tree.str[0] in male_nouns):
      tree.gender = 1
    else:
      if (tree.str[0] == female_nouns):
        tree.gender = 2
      else:
        tree.gender = 3
  if (tree.label() == 'NNS'):
    tree.gender = 3
  # Recursive case
  if (tree.gender == 0):
    count_NP = 0
    for child in tree:
      if (isinstance(child, str)):
        break
      get_gender(child)
      if (child.label() == 'CC' and child.str[0] == 'and'):
        tree.gender = 3
      if (tree.gender == 0):
        if (child.label() in ['NNP', 'NN', 'PRP'] and child.gender != 3):
          tree.gender = child.gender
        if (child.label() == 'NP'):
          count_NP = count_NP + 1
          if (count_NP == 1):
            tree.gender = child.gender
          if (count_NP > 1):
            tree.gender = 3
  # Other cases are unknown
  if (tree.gender == 0):
    tree.gender = 3
  # print tree.str, tree.gender

def get_number(tree):
  tree.number = 0
  # Not an 'NP' 
  if not(tree.label() in ['NP', 'NN', 'NNS', 'NNP', 'NNPS', 'PRP']):
    for child in tree:
      if (isinstance(child, str)):
        break
      get_number(child)
    return
  # Is an 'NP' 
  # Trivial cases
  # Proper noun (singular)
  if (tree.label() == 'NNP'):
    tree.number = 1
  if (tree.label() == 'NNPS'):
    tree.number = 2
  # Pronoun
  if (tree.label() == 'PRP'):
    if (tree.str[0] in ['I', 'he', 'she', 'it', 'me', 'him', 'her']):
      tree.number = 1
    else:
      tree.number = 2
  # Noun
  if (tree.label() == 'NN'):
    tree.number = 1
  if (tree.label() == 'NNS'):
    tree.number = 2
  if (tree.number == 0):
    # Recursive case
    for child in tree:
      if (isinstance(child, str)):
        break
      get_number(child)
      if (child.label() == 'CC' and child.str[0] == 'and'):
        tree.number = 2
      if (child.label() in ['NP', 'NN', 'NNS', 'NNP', 'NNPS', 'PRP']):
        tree.number = max(tree.number, child.number)
  # Other cases are singular
  if (tree.number == 0):
    tree.number = 1
  # print tree.str, tree.number
 
def get_person(tree):
  tree.person = 0
  # Not an 'NP' 
  if not(tree.label() in ['NP', 'NN', 'NNS', 'NNP', 'NNPS', 'PRP']):
    for child in tree:
      if (isinstance(child, str)):
        break
      get_person(child)
    return
  # Is an 'NP'   
  # Trival cases
  # Pronoun
  if (tree.label() == 'PRP'):
    if (tree.str[0] in ['I', 'we', 'me', 'us']):
      tree.person = 1
    if (tree.str[0] in ['you']):
      tree.person = 2
  if (tree.person == 0):
    # Recursive case
    for child in tree:
      if (isinstance(child, str)):
        break
      get_person(child)
      if (child.label() == 'PRP'):
        tree.person = child.person
  # Other cases are third person
  if (tree.person == 0):
    tree.person = 3
  # print tree.str, tree.person

def find_node(tree, phrase):
  if (len(tree.str) <= len(phrase)):
    if (phrase == tree.str and tree.label() == 'NP'):
      return tree
    return None
  for child in tree:
    child_res = find_node(child, phrase)
    if (child_res != None):
      return child_res
  return None 

def find_coref(trees, id, phrase):
  tree = find_node(trees[id], phrase)
  return hobbs.hobbs(trees, id, tree)
  
def find_coref_sentence(trees, sentences, id):
  print sentences[id]
  at = -1
  stack = []
  for token in sentences[id]:
    if (re.compile('<\s*/\s*COREF>').match(token)):
      phrase = []
      while not(len(stack[-1]) == 1 and re.compile('<\s*COREF[^>]*>').match(stack[-1][0])):
        phrase.extend(stack.pop())
      stack.pop()
      phrase.reverse()
      print phrase, find_coref(trees, id, phrase)
      stack.append(phrase)
    else:
      stack.append([token])


global male_names
global female_names
names = nltk.corpus.names
male_names = names.words('male.txt')
female_names = names.words('female.txt')

global male_nouns
global female_nouns
nouns = LineTokenizer().tokenize(open("../data/gender/gender_nouns.txt", "r").read())
nouns = [WhitespaceTokenizer().tokenize(line) for line in nouns]
male_nouns = [word[0] for word in nouns]
female_nouns = [word[1] for word in nouns]

# trees = read_trees("../data/conll.trial/data/english/annotations/bc/cnn/00/cnn_0000.parse")
# sentences = read_sentences("../data/conll.trial/data/english/annotations/bc/cnn/00/cnn_0000.coref")
# trees = trees[0:2]
# sentences = sentences[0:2]

trees = []
sentences = []
# trees.append(ParentedTree.fromstring("""
# (ROOT
#   (S
#     (NP
#       (NP (DT The) (NN castle))
#       (PP (IN in)
#         (NP (NNP Camelot))))
#     (VP (VBD remained)
#       (NP
#         (NP (DT the) (NN residence))
#         (PP (IN of)
#           (NP (DT the) (NN king))))
#       (PP (IN until)
#         (NP (CD 536)))
#       (SBAR
#         (WHADVP (WRB when))
#         (S
#           (NP (PRP he))
#           (VP (VBD moved)
#             (NP (PRP it))
#             (PP (TO to)
#               (NP (NNP London)))))))
#     (. .)))"""))
# sentences.append(WhitespaceTokenizer().tokenize("The castle in Camelot remained the residence of the king until 536 when he moved it to London."))


# trees.append(ParentedTree.fromstring("""
# (ROOT
#   (S
#     (S
#       (NP (NNP Jane))
#       (VP (VBZ has)
#         (NP (DT a) (NN cat))))
#     (CC and)
#     (S
#       (NP (PRP she))
#       (VP (VBZ loves)
#         (NP (PRP it))))
#     (. .)))"""))

# sentences.append(WhitespaceTokenizer().tokenize("Jane has a cat and she loves it."))

# trees.append(ParentedTree.fromstring("""
# (ROOT
#   (S
#     (NP
#       (NP (PRP you))
#       (CC and)
#       (NP (PRP I)))
#     (VP (VBP are)
#       (VP (VBG catching)
#         (NP (JJ many) (NNS mosquitoes))))
#     (. .)))"""))
# sentences.append(WhitespaceTokenizer().tokenize("you and I are catching many mosquitoes.."))

trees.append(ParentedTree.fromstring("""
(ROOT
  (S
    (NP
      (NP
        (NP
          (NP (NNP John) (POS 's))
          (NN father) (VBZ 's))
        (NN portrait))
      (PP (IN of)
        (NP (PRP him))))
    (VP (VBZ is)
      (VP (VBN lost)))))"""))
sentences.append(WhitespaceTokenizer().tokenize("John's father's portrait of him is lost"))

for tree in trees:
  traverse_tree(tree)
  get_gender(tree)
  get_number(tree)
  get_person(tree)

print find_coref(trees, 0, ['him'])

# for i in range(0, len(trees)):
#   find_coref_sentence(trees, sentences, i)















