class MySingleton(object):
    _instance=None

    def __new__(self):
        if not self._instance:
            # self._instance=super(MySingleton,self).__new__(self)
            self.y=10
        return self._instance

x = MySingleton()
print(x.y)
x.y=20
print(x.y)
z=MySingleton()
print(z.y)

print(id(x))
print(id(z))