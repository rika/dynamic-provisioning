import random

def child(x):
    return (x-1)//2
def lparent(x):
    return 2*(x)+1
def rparent(x):
    return 2*(x)+2

class MinHeap:
    def __init__(self):
        self.elems = []
    
    def swap(self, i, j):
        k = self.elems[i]
        self.elems[i] = self.elems[j]
        self.elems[j] = k
        
    def fix_down(self):
        i = 0;
        while(lparent(i) < len(self.elems)):
            j = lparent(i)
            
            if rparent(i) < len(self.elems) and \
                    self.elems[rparent(i)] < self.elems[lparent(i)]:
                j = rparent(i)
            
            if (self.elems[j] < self.elems[i]):
                self.swap(i, j)
                i = j
            else:
                i = len(self.elems)
        
    def fix_up(self):
        i = len(self.elems) - 1
        while i >= 1 and self.elems[child(i)] > self.elems[i]:
            self.swap(i, child(i))
            i = child(i)
            
    def insert(self, x):
        self.elems.append(x)
        self.fix_up()
        
    def pop_min(self):
        if len(self.elems) == 0:
            return None
        
        self.swap(0, len(self.elems) - 1)
        x = self.elems.pop()
        self.fix_down()
        return x
    
    def pt(self):
        print("N: %s" % len(self.elems))
        for x in self.elems:
            print(x, end=' ')
        print()


        
if __name__ == '__main__':
    
    SIZE = 400
    
    heap = MinHeap()
    
    for i in range(SIZE):
        x = random.randint(1,SIZE)
        print("inserindo %s" % x)
        heap.insert(x)
    
    heap.pt()
    
    x = heap.pop_min()
    print("removendo %s" % x)
    for i in range(SIZE-1):
        y = heap.pop_min()
        print("removendo %s" % y)
        if x > y:
            raise Exception("ERRADO")
        x = y        
    