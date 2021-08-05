
import pytest
import sys
import logging
sys.path.append("src")


@pytest.mark.logging
class TestStepFunctions:
    def test_log_env(self):
        # SETUP
        logger = logging.getLogger('root')
        # EXECUTE

        # ASSERT
        assert logger.isEnabledFor(logging.WARN)
        assert not logger.isEnabledFor(logging.INFO)
