from operator import itemgetter
import sys

term = sys.argv[1]
current_word = None
current_count = 0
word = None

for line in sys.stdin:
    line = line.strip()

    word, count = line.split('\t', 1)
    try:
        count = int(count)
    except ValueError:
        continue

    if term == word:
        current_count += count
        current_word = word
if (current_word == term):
    print('%s\t%s' % (term, current_count))
