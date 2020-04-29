import math
import mmh3
from bitarray import bitarray


class BloomFilter():
    '''
    Class for Bloom filter, using murmur3 hash function
    '''

    def __init__(self, item_count, fp_probability):
        '''
        item_count: int
            No of items expected to be stored in the filter
        fp_probability: float
            False Positive Probability in decimal
        '''
        self.fp_probability = fp_probability
        self.item_count = item_count
        self.size = self.setSize(item_count, fp_probability)
        self.bit_array = bitarray(self.size)

        # initialize all bits as 0
        self.bit_array.setall(0)

        self.hash_count = self.getHashCount(self.size, item_count)

    def setSize(self, n, p):
        '''
        n- int
            no of items expected to be stored in the bloom filter
        p - float
            probability of false positives
        '''
        m = -(n * math.log(p))/(math.log(2)**2)
        return int(m)

    def getHashCount(self, m, n):
        '''
        m -int
            size of the bloom filter
        n - int
            no of items expected to be stored in the bloom filter
        '''
        k = (m/n) * math.log(2)
        return int(k)

    def add(self, item):
        if(len(self.bit_array) == round((.7 * self.size), 0)):
            self.reset()
        for i in range(self.hash_count):

            # create digest for given item.
            # i work as seed to mmh3.hash() function
            # With different seed, digest created is different
            digest = mmh3.hash(item, i) % self.size

            # set the bit True in bit_array
            self.bit_array[digest] = True

    def exists(self, item):

        for i in range(self.hash_count):
            digest = mmh3.hash(item, i) % self.size
            if self.bit_array[digest] == False:

                # if any of bit is False then,its not present
                # in filter
                # else there is probability that it exist
                return False
        return True

    def reset(self):
        self.bit_array.setall(0)
