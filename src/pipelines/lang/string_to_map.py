class StringToMap:

    # map_split expects a string in the form of key1:value1,key2:value2,key3:value3,keyN:valueN
    # it will return a dict in the form of
    # {
    #       key1: value1,
    #       key2: value2,
    #       key3: value3,
    #       keyN: valueN
    # }
    def to_map(self, str):
        entries = str.split(",")
        new_map = {}
        for entry in entries:
            kvpair = entry.split(":")
            new_map[kvpair[0]] = kvpair[1]

        return new_map
