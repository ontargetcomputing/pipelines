class FilenameInvestigator:

    def in_filetypes(self, filename, filetypes):
        # filetypes must be a comma separated string with no
        # whitespace
        filetypes_tuple = tuple(filetypes.split(','))
        return filename.endswith(filetypes_tuple)

    def determine_extension(self, filename):
        if filename.find(".") > 0:
            last_index = filename.rindex(".")
            return filename[last_index:len(filename)]
        else:
            raise Exception(f'{filename} does not contain an extension')

    def determine_base_filename(self, filename):
        period_location = filename.find(".")
        if period_location > 0:
            return filename[0: period_location]
        else:
            raise Exception(f'{filename} does not contain an extension')

    def determine_simple_base_filename(self, filename):
        base_filename = self.determine_base_filename(filename)

        last_forward_slash = base_filename.rfind("/")
        if last_forward_slash > 0:
            return base_filename[last_forward_slash + 1:]
        else:
            return base_filename

    def determine_root_directory(self, filename):
        first_forward_slash = filename.find("/")
        if first_forward_slash > 0:
            return filename[0:first_forward_slash]
        else:
            raise Exception(f'{filename} is not within a directory')
