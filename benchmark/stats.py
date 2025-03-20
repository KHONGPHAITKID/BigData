

class Stats:
    def __init__(self):
        self.stats = {}

    def count(self, key: str, value: int):
        self.stats[key] += value

    def get(self, key: str) -> int:
        return self.stats.get(key, 0)

    def reset(self):
        self.stats = {}