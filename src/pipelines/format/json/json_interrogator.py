class JsonInterrogator:

    # NOTE: path is expected to ba a "." delimited string of keys.
    # This function is super simple, it will not work with anything more
    # than a simple path....arrays and other fancies will break this.
    def path_exists(self, obj, path):
        key_list = path.split(".")
        current_dict = obj
        for key in key_list:
            if key in current_dict.keys():
                current_dict = current_dict[key]
            else:
                return False

        return True

    def determine_root_node(self, obj):
        keys = list(obj.keys())
        num_keys = len(keys)
        if num_keys != 1:
            raise Exception(f'There must be only on root level node in {obj}')

        return keys[0]
