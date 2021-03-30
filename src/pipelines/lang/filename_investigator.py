class FilenameInvestigator:

    def in_filetypes(self, filename, filetypes):
        # filetypes must be a comma separated string with no
        # whitespace
        filetypes_tuple = tuple(filetypes.split(','))
        return filename.endswith(filetypes_tuple)

    # def replace_extension_if_filetype(self, filename, filetype, new_filetype):
    #     if filename.endswith(filetype):
    #         return new_filetype.join(filename.rsplit(filetype, 1))
    #     else:
    #         raise Exception(f'{filename} does not end with {filetype}')

    def determine_extension(self, filename):
        if filename.find(".") > 0:
            last_index = filename.rindex(".")
            return filename[last_index:len(filename)]
        else:
            raise Exception(f'{filename} does not contain an extension')

    def determine_base_filename(self, filename):
        if filename.find(".") > 0:
            return filename[0: filename.find(".")]
        else:
            raise Exception(f'{filename} does not contain an extension')
