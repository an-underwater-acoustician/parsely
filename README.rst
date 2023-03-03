Parsely
==========

Repository to interface and parse hydrographic and scientific sonar files

Formats in development:
- KMALL
- S7K
- MBCDF

General Information
-------------------

Parsely is a library for parsing multibeam echosounder (MBES) datagrams and files.
Currently, the package acts as a file interface that reads the binary datafiles to python objects that the user can
interface with.
In addition to the ability to parse  data, the library has a
format converter to translate MBES data from proprietary format to an open
source format built on the netCDF4 format (eventually).
  
Datagram Philosophy
-------------------

Most manufacturers package their data in a compact binary representation
stored in relatively chronological order with packets of data from various
sensors interleaved.

In order to read these packets and files, this project adopts a dataclass
approach using the `attrs <https://www.attrs.org/en/stable/index.html>`_ package. If a datagram has sub compenents (The
substructures in KMALL, the DRF/RTH/RD... of Reson S7k) these are
treated as child dataclass objects which are constructed first and
inserted into the parent datagram dataclass (dependency injection). For
documentation purposes, references to *dataclass* reference an `attrs` class.

A dataclass contains the structure of the data, and where applicable relevant
metadata and initial values. A dataclass instance can be constructed using the
class's factory method which parses the data chunk related to that datagram.
Constructing a dataclass instance without the factory method is possible, but
requires the user to unpack the data and pass the attributes (preferably by
keyword) directly, ensuring data types match.

File Reading
------------
The datagram definitions are defined independently of the readers allowing the
user to create their own readers if they choose. However, the library
includes a reader which serves as an *interface* to the file. As a result,
the MBES file_reader object behaves similar to the standard library file
object. A file_reader is associated with a single file, supports context
management using the `with` command, should be destroyed when no longer in
use.

Use of `attrs` vs `dataclass`
-----------------------------
The python standard library has support for dataclasses through the 
`dataclass` module. Initial development was done using this package. However
, development has switched to `attrs` for the following reasons:
- Maturity: `attrs` has been in development much longer than `dataclass`
- Validation: It is possible to validate data inputs to a datagram class, which
is useful when creating mock objects for testing
- Slots: The biggest reason, `attrs` objects are slotted by default when
fully typed, which reduces computational time and memory. While `dataclass`
does provide the same support, this is a new feature only available in Python
3.10, of which `Pydro <https://svn.pydro.noaa.gov/Docs/html/Pydro/universe_overview.html>`_ does not support yet.
