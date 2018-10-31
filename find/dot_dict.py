class DotDictify(dict):
    MARKER = object()

    def __init__(self, value=None):
        if value is None:
            pass
        elif isinstance(value, dict):
            for key in value:
                self.__setitem__(key, value[key])
        else:
            raise TypeError('expected dict')

    def __setitem__(self, key, value):
        if isinstance(value, dict) and not isinstance(value, DotDictify):
            value = DotDictify(value)
        super(DotDictify, self).__setitem__(key, value)

    def __getitem__(self, key):
        found = self.get(key, DotDictify.MARKER)
        if found is DotDictify.MARKER:
            found = DotDictify()
            super(DotDictify, self).__setitem__(key, found)
        return found

    __setattr__, __getattr__ = __setitem__, __getitem__


