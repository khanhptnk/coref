import nltk
import re
from nltk.tree import *
from nltk.tokenize import *

def find_first_NP_or_S(node, path):
  while (node.parent() != None):
    tmp = node
    node = node.parent()
    path.append(node)
    if (node.label() == 'NP' or node.label() == 'S'):
      return node

def is_match(node1, node2):
  return (node1.gender == node2.gender) & (node1.number == node2.number) & (node1.person == node2.person)

def bfs1(orig, node, path):
  ans = []
  queue = [node]
  while (len(queue) > 0):
    cur = queue[0]
    del queue[0]
    # See an NP, check if there is an NP in between 'node' and 'cur'
    if (not(cur in path) and cur.label() == 'NP'):
      tmp = cur
      while (True):
        tmp = tmp.parent()
        if (tmp == node):
          break
        if (tmp != node and tmp.label() == 'NP'):
          # Found an antecedent!!!
          if (is_match(orig, cur)):
            return cur
    for child in cur:
      if (isinstance(child, Tree)):
        if (child != path[0]):
          queue.append(child)
        if (child in path):
          break
  return None

def bfs2(orig, node, path):
  ans = []
  queue = [node]
  while (len(queue) > 0):
    cur = queue[0]
    del queue[0]
    # Found an antecedent!!!
    if (not(cur in path) and cur.label() == 'NP'):
      if (is_match(orig, cur)):
        return cur
    for child in cur:
      if (isinstance(child, Tree)):
        if (child != path[0]):
          queue.append(child)
        if (child in path):
          break
  return ans

def bfs3(orig, node, path):
  ans = []
  queue = [node]
  while (len(queue) > 0):
    cur = queue[0]
    del queue[0]
    # Found an antecedent!!!
    if (not (cur in path) and cur.label() == 'NP'):
      if (is_match(orig, cur)):
        return cur
      continue
    if (cur.label() == 'S'):
      continue
    can_add = False
    for child in cur:
      if (isinstance(child, Tree)):
        if (child in path):
          can_add = True
        if (can_add and child != path[0]):
          queue.append(child)
  return ans

def hobbs(trees, id, node):
  orig = node
  path = [node]
  # Step 2
  node = find_first_NP_or_S(node, path)
  # Step 3
  ans = bfs1(orig, node, path)
  if (ans != None): 
    return ans

  while not(node.label() == 'S' and node.parent().parent() == None):
    # Step 5
    node = find_first_NP_or_S(node, path)
    # Step 6
    if (node.label() == 'NP'):
      for child in node:
        if (child.label() in ['NN', 'NNP', 'PRP'] and not(child in path)):
          if (is_match(orig, node)):
            return node
    # Step 7
    ans = bfs2(orig, node, path)
    if (ans != None):
      return ans
    # Step 8
    if (node.label() == 'S'):
      ans = bfs3(orig, node, path)
      if (ans != None):
        return ans
  
  # Step 4
  while (id > 0):
    id = id - 1
    ans = bfs2(orig, trees[id], path)
    if (ans != None):
      return ans
  return None