class NotFoundError(Exception):
    def __init__(self, id, value, field, *args):
        super().__init__(args)
        self.id = id
        self.value = value
        self.field = field

    def __str__(self):
        return f'Value MisMatch: [{self.id}] The value [{self.value}] for field [{self.field}] mismatched'

class MapError(Exception):
    def __init__(self, id, value, field, *args):
        super().__init__(args)
        self.id = id
        self.value = value
        self.field = field

    def __str__(self):
        return f'Mapping Error: The value [{self.value}] for field [{self.field}] was not found in input JSON'
