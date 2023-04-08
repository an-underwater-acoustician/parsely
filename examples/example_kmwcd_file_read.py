import logging

from pathlib import Path

from parsely.kmall.file_reader import KmallFileReader

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


# #### KMWCD File Reader Example Script ####
# This script is used to test/inspect the functionality of the
# kmall_file_reader for kmwcd files.
# The user/reader is encouraged to run this scrip using debug mode.

# Get path to file
dpath = Path(__file__).resolve()
data_dir = dpath.parents[1].joinpath("data").joinpath("download")

# We take the first file in the directory, the index can be changed if
# there are other files
file_path = list(data_dir.rglob("*.kmwcd"))[0]
logger.debug(f"Data file: {file_path.absolute()}")

# Create a kmall file object
km_file = KmallFileReader(file_path)

# Use map to get reference to first datagram of a type and read it into memory.
# Because get_datagrams_from_map_entry can accept a list of map_entries, the output
# is always a list.
dg_id = b'#MWC'
logger.debug(f"Number of {dg_id} datagrams: {len(km_file.map[dg_id])}")
mwc_0 = km_file.map[dg_id][0]
mwc_dg = km_file.get_datagrams_from_map_entry(mwc_0)
mwc_dg = mwc_dg[0]

# Using km_file to extract data in a lower level loop. Just grab the first 15 datagrams.
logger.debug(" Reading the first 15 datagrams ")
for ii in range(15):
    datagram = km_file.read_datagram()


# # Use a get data method to extract position data
logger.debug(" Reading in all position data from MRZ records")
record_dt, lat, lon, height, epgs = \
    km_file.get_position()  # Default source is MRZ
logger.debug(f"Number of retrieved timestamps for position: {record_dt.shape[0]}")


# When looping over datagrams, a get_data method will interupt the loop. km_file will
# pick up where the user left off when calling read_datagram()
logger.debug(" Resuming datagram loop. Reading in next 15 datagrams")
for ii in range(15):
    datagram = km_file.read_datagram()

# The following code extracts all datagram records matching an input datagram id.
dg_id = b'#SPO'
spo_list = km_file.get_all_datagrams_of_type(dg_id)
logger.debug(f"Number of {dg_id} datagrams read: {len(spo_list)}")

