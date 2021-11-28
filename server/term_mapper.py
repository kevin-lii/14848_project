import sys

for line in sys.stdin:
    line = line.strip().lower()
    words = ''.join(word for word in line if word.isalnum()
                    or word.isspace()).split()
    for word in words:
        print('%s\t%s' % (word, 1))
