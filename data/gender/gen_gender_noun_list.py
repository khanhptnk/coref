from nltk.tokenize import *

f = open("../data/gender/word_gender_creatures.txt", "r")
raw = f.read()

lines = LineTokenizer().tokenize(raw)

male = []
female = []
for i in range(0, 77):
	words = WhitespaceTokenizer().tokenize(lines[i])
	male.append(words[0])
	female.append(words[1])

with open("../data/gender/male.txt", "w") as f:
	print f, "\n".join(male)
