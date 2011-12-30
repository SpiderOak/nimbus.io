"""
LRUCache.py
A dictionary-like object whch discards the least recently used members

Taken from the ActiveState Python Cookbook
http://aspn.activestate.com/ASPN/Cookbook/Python/Recipe/252524
"""

class LRUCache:
    """
    Implementation of a length-limited O(1) LRU queue. This is a drop in 
    replacement for a Python dict().

    Built for and used by PyPE:

    http://pype.sourceforge.net

    Copyright 2003 Josiah Carlson.

    Taken from the ActiveState Python Cookbook

    http://aspn.activestate.com/ASPN/Cookbook/Python/Recipe/252524
    """
    class _Node(object):
        __slots__ = ['prev', 'next', 'me']
        def __init__(self, prev, me):
            self.prev = prev
            self.me = me
            self.next = None
    
    def __init__(self, count, pairs=[]):
        self.count = max(count, 1)
        self.d = {}
        self.first = None
        self.last = None
        for key, value in pairs:
            self[key] = value
    def __contains__(self, obj):
        return obj in self.d
    def __getitem__(self, obj):
        a = self.d[obj].me
        self[a[0]] = a[1]
        return a[1]
    def __setitem__(self, obj, val):
        if obj in self.d:
            del self[obj]
        nobj = LRUCache._Node(self.last, (obj, val))
        if self.first is None:
            self.first = nobj
        if self.last:
            self.last.next = nobj
        self.last = nobj
        self.d[obj] = nobj
        if len(self.d) > self.count:
            if self.first == self.last:
                self.first = None
                self.last = None
                return
            a = self.first
            a.next.prev = None
            self.first = a.next
            a.next = None
            del self.d[a.me[0]]
            del a
    def __delitem__(self, obj):
        nobj = self.d[obj]
        if nobj.prev:
            nobj.prev.next = nobj.next
        else:
            self.first = nobj.next
        if nobj.next:
            nobj.next.prev = nobj.prev
        else:
            self.last = nobj.prev
        del self.d[obj]
    def __iter__(self):
        cur = self.first
        while cur != None:
            cur2 = cur.next
            yield cur.me[1]
            cur = cur2
    def size(self):
        return len(self.d)
    def has_key(self, key):
        return self.d.has_key(key)
    def get(self, key, default=None):
        try:
            return self[key]
        except KeyError:
            return default
    def iteritems(self):
        cur = self.first
        while cur != None:
            cur2 = cur.next
            yield cur.me
            cur = cur2
    def iterkeys(self):
        return iter(self.d)
    def itervalues(self):
        for i,j in self.iteritems():
            yield j
    def keys(self):
        return self.d.keys()
    def clear(self):
        self.d.clear()
        self.first = None
        self.last = None
    def update(self, other, **kw):
        if hasattr(other, 'keys'):
            for k in other.keys():
                self[k] = other[k]
        else:
            for k, v in other:
                self[k] = v
        for k in kw:
            self[k] = kw[k]


class DefaultLRUCache(LRUCache):
    def __init__(self, default_factory, count):
        LRUCache.__init__(self, count)
        self.__default_factory = default_factory
    def __getitem__(self, obj):
        try:
            item = LRUCache.__getitem__(self, obj)
        except KeyError:
            item = self[obj] = self.__default_factory()
        return item
