import re
import sys

print("Searching for word: \"{}\" in {}".format(sys.argv[1], sys.argv[2:]))
count = 0
for f in sys.argv[2:]:
    with open(f, 'r') as fd:
        text = fd.read()
        for word in text.split():
            validWord = re.sub(r'\W+', '', word)
            validWord = validWord.replace("_", "")
            if validWord == sys.argv[1]:
                # print(word)
                count += 1
print("{}: {}".format(sys.argv[1], count))
