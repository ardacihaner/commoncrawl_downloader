import sys
import random
random.seed(3169420)

block_count = int(sys.argv[1])
req_block_count = int(sys.argv[2])

rangints = list(range(block_count))
random.shuffle(rangints)

print(",".join(str(x) for x in rangints[:req_block_count]))
