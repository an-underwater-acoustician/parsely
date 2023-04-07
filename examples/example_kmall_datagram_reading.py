import logging

from pathlib import Path

from parsely.kmall.datagrams import Kmall, kmall_dispatch

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# #### Reading a Datagram Example Script ####
# This script is used to test/inspect the results of parsing a datagram.
# The user/reader is encouraged to run this scrip using debug mode.

# Get path to file
dpath = Path(__file__).resolve()
data_dir = dpath.parents[1].joinpath("data").joinpath("download")

# We take the first file in the directory, the index can be changed if
# there are other files
file_path = list(data_dir.rglob("*.kmall"))[0]
logger.debug(f"Data file: {file_path.absolute()}")

# ### Datagram Testing Loop ###
# Here we set the datagram ID to inspect.
dg_id = b'#MRZ'

# The loop will step through each datagram. If the datagram matches the
# ID set above, it will parse the datagram and place it in a list.
selected_datagrams = list()

with file_path.open(mode='rb') as f:

    # Test Kmall header dg -> Check first record IIP
    f.seek(0, 2)
    sz = f.tell()
    f.seek(0, 0)
    chunk = f.read(20)
    dg_hdr, _ = Kmall.parse(chunk)
    # logger.debug(f"ID: {dg_hdr.id.decode('utf-8'):10} Size: {dg_hdr.size: <10}")
    f.seek(0, 0)

    # Check selected datagram parses correctly
    num_dg = 0
    while True:
        chunk = f.read(20)
        logger.debug(f"File Position: {f.tell()} of {sz}")
        if chunk == b'':
            break

        dg_hdr, _ = Kmall.parse(data=chunk)
        logger.debug(f"ID: {dg_hdr.id.decode('UTF-8'):10} Size: "
                     f"{dg_hdr.size: <10}")
        num_dg += 1
        if dg_hdr.id == dg_id:
            f.seek(-dg_hdr.header_size, 1)
            chunk = f.read(dg_hdr.size)
            datagram = kmall_dispatch[dg_hdr.id].parse(chunk)
            selected_datagrams.append(datagram)

        else:
            skp = dg_hdr.size-dg_hdr.header_size
            f.seek(skp, 1)

# Add a debug point below to inspect the selected_datagram list
logger.debug(f"Total number of datagrams: {num_dg}")
logger.debug(f"Number of {dg_id} datagrams: {len(selected_datagrams)}")