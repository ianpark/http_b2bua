
class Statistics:
    def __init__(self):
        self.data = {}

    def append(self, name, amount):
        if name not in self.data.keys():
            self.data[name] = 0
        self.data[name] += amount

    def increase(self, name):
        if name not in self.data.keys():
            self.data[name] = 0
        self.data[name] += 1

    def get(self, name):
        return self.data[name]

    def getMB(self, name):
        if self.data[name] == 0:
            return 0
        else:
            return self.data[name] / 1048576

    def set(self, name, value):
        self.data[name] = value

    def __str__(self):
        return str(self.data)
