import pytest
import sys
sys.path.append("src")
from pipelines.lang.filename_investigator import FilenameInvestigator

filename_investigator = FilenameInvestigator()


@pytest.mark.filename_investigator
class TestFilenameInvestigator:
    def test_in_filetypes_determines_yes_correctly(self):
        # SETUP
        filetypes = "json,txt,csv"

        # EXECUTE
        in_filetypes = filename_investigator.in_filetypes("some_file.txt", filetypes)

        # ASSERT
        assert in_filetypes is True

    def test_in_filetypes_determines_no_correctly(self):
        # SETUP
        filetypes = "json,txt,csv"

        # EXECUTE
        in_filetypes = filename_investigator.in_filetypes("some_file.pdf", filetypes)

        # ASSERT
        assert in_filetypes is False

    def test_determine_extension_works_when_no_raise(self):
        # SETUP
        filename = "foobar.txt"

        # ASSERT
        assert filename_investigator.determine_extension(filename) == ".txt"

    def test_determine_extension_works_when_raise(self):
        # SETUP
        filename = "foobar"

        # ASSERT
        with pytest.raises(Exception, match="foobar does not contain an extension"):
            filename_investigator.determine_extension(filename)

    def test_determine_extension_works_when_no_raise(self):
        # SETUP
        filename = "foobar.txt"

        # ASSERT
        assert filename_investigator.determine_extension(filename) == ".txt"

    def test_determine_base_filename_works_when_raise(self):
        # SETUP
        filename = "foobar"

        # ASSERT
        with pytest.raises(Exception, match="foobar does not contain an extension"):
            filename_investigator.determine_extension(filename)

    def test_determine_base_filename_works_when_no_raise(self):
        # SETUP
        filename = "foobar.txt"

        # ASSERT
        assert filename_investigator.determine_base_filename(filename) == "foobar"


