import sys
import collections

top = int(sys.argv[1])
counter = collections.Counter()

for line in sys.stdin:
    k, v = line.strip().split("\t", 2)
    counter[k] += int(v)

for k, v in counter.most_common(top):
    print('%s\t%s' % (k, v))
