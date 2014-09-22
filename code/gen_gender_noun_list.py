from nltk.tokenize import *
from sets import Set

f = open("../data/gender/word_gender_people.txt", "r")
raw = f.read()

lines = LineTokenizer(blanklines='keep').tokenize(raw)
print len(lines)

gender_nouns = Set()
for i in range(0, 76):
	words = WhitespaceTokenizer().tokenize(lines[i])
	gender_nouns.add((words[0], words[1]))

male = []
for i in range(77, 124):
	words = WhitespaceTokenizer().tokenize(lines[i])
	male.append(words[0])

female = []
for i in range(125, 172):
	words = WhitespaceTokenizer().tokenize(lines[i])
	female.append(words[0])

for i in range(0, len(male)):
	gender_nouns.add((male[i], female[i]))

gender_nouns = sorted(gender_nouns)
with open("../data/gender/gender_nouns.txt", "w") as f:
	print >> f, "\n".join([' '.join(map(str, pair)) for pair in gender_nouns])
