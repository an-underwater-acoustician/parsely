import attr
import logging

from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path
from typing import Optional, BinaryIO, List

logger = logging.getLogger(__name__)


class AbstractFileReader(ABC):
    """ Abstract sonar datafile reader class"""

    def __init__(self, file_path: Path):
        """ init method

        Extend by adding reader specific _validate() and map_file() methods

        """
        self.desc = "Abstract Sonar File Reader"

        # File attributes
        self.file: Optional[BinaryIO] = None
        self._file_path: Optional[Path] = file_path
        self._file_size: Optional[int] = None
        self.file_type: Optional[str] = None
        self.valid: bool = False

        # IO file attributes
        self.location: int = 0
        self.end_of_file: bool = False

        # File content attributes
        self.map: Optional[dict] = None
        self.number_of_datagrams: Optional[int] = None
        self.number_of_pings: Optional[int] = None
        self.max_tx_sectors: Optional[int] = None      # Type: int
        self.max_beams: Optional[int] = None
        self.datagram_splits: bool = False
        self.split_start_index: Optional[List[int]] = None  # Starting index for split datagrams
        self.split_num_dgs: Optional[List[int]] = None  # Matching list of number of datagrams for single ping cycle

    @abstractmethod
    def map_file(self):
        pass

    @abstractmethod
    def read_datagram(self):
        pass

    @abstractmethod
    def get_position(self):
        pass

    @abstractmethod
    def get_attitude(self):
        pass

    @abstractmethod
    def get_sonar_settings(self):
        pass

    def get_tvg(self):
        raise NotImplemented("Sonar specific subclass should implement own tvg "
                             "method call")

    @abstractmethod
    def get_raw_range(self):
        pass

    @abstractmethod
    def get_backscatter(self):
        pass

    def get_installation(self):
        pass

    @abstractmethod
    def _validate_file(self):
        """ Validate file integrity

        Check for valid extensions
        Check for file existence
        Check for functionality i.e. open/close/etc.
        """
        pass


@attr.s(auto_attribs=True)
class _AbstractMapEntry:
    """ Abstract map entry class, extend in file specific readers

    Private class for storing datagram record location information.
    Class should be private since there is no need for user to directly
    create/modify with a given map entry.
    """

    file_location: int
    record_datetime: datetime
    record_size: int
