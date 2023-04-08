import attr
import logging
import numpy as np

from pathlib import Path
from typing import Union, List

from parsely._internal.abstract_file_reader import AbstractFileReader, _AbstractMapEntry
from parsely.kmall.datagrams import kmall_dispatch

logger = logging.getLogger(__name__)

file_types = {
    "kmall": ["kmall file", b'#MRZ'],
    "kmwcd": ["water column file", b'#MWC']
}


class KmallFileReader(AbstractFileReader):
    """ Class for reading/extracting data from Kongsberg .kmall/.kmwcd files"""

    def __init__(self, file_path: Path):
        super().__init__(file_path)
        self.desc = "KMALL/KMWCD Sonar File Reader"

        self.valid = self._validate_file()
        self.map_file()

    @property
    def file_path(self) -> Path:
        return self._file_path

    @property
    def extension(self) -> str:
        return self.file_path.parts[-1].split('.')[-1]

    def map_file(self):
        """ Maps the datagram packets in the file

        The function maps the file and pulls some additional statistics from
        the file.

        Output is a dictionary where:
            Keys                - datagram id ex. b'mrz'
            Values              - list of individual datagrams matching id/key*.

            * Each list entry is a list containing the following information -
            [file position, datagram timestamp (datetime object), datagram
            size, datagram version]
        """

        dg_map = dict()
        with self.file_path.open(mode='rb') as self.file:
            self.file.seek(0, 0)

            # Loop through file, adding datagrams to the map
            while True:
                chunk = self.file.read(kmall_dispatch[b'KMALL'].header_size)

                # End of file check
                if chunk == b'':
                    break

                dg_header, _ = kmall_dispatch[b'KMALL'].parse(chunk)
                dg_start = self.file.tell() - dg_header.header_size
                map_entry = _KmallMapEntry(file_location=dg_start,
                                           record_datetime=dg_header.record_datetime,
                                           record_size=dg_header.size,
                                           version=dg_header.version)
                if dg_header.id in dg_map.keys():
                    dg_map[dg_header.id].append(map_entry)
                else:
                    dg_map[dg_header.id] = list()
                    dg_map[dg_header.id].append(map_entry)

                # Skip to next datagram
                skip = dg_header.size - kmall_dispatch[b'KMALL'].header_size
                self.file.seek(skip, 1)

            # Now check if records are not in chronological order, if not
            # reorder.
            for map_id, records in dg_map.items():
                if len(records) == 1:
                    continue
                records_datetimes = np.asarray(list(map(lambda x: x.record_datetime,
                                                        records)))
                tmp = np.asarray(records)
                record_index = np.argsort(records_datetimes)
                if not np.all(record_index == np.arange(len(record_index))):
                    tmp = np.take_along_axis(tmp[:, np.newaxis],
                                             record_index[:, np.newaxis], axis=0)
                    dg_map[map_id] = np.squeeze(tmp).tolist()

            # Loop through ping datagrams, getting statistics
            dg_id = file_types[self.extension][1]

            ping_index = list()
            num_dg_in_ping = list()
            file_max_beams = 0
            file_max_sectors = 0
            for index, dg_info in enumerate(dg_map[dg_id]):
                self.file.seek(dg_info.file_location)
                start_dg, num_dg, num_tx_sector, max_beams = \
                    kmall_dispatch[dg_id].stats(self.file)

                if start_dg is True:
                    ping_index.append(index)
                    num_dg_in_ping.append(num_dg)
                file_max_beams = max(file_max_beams, max_beams)
                file_max_sectors = max(file_max_sectors, num_tx_sector)
        self.map = dg_map
        self.number_of_datagrams = sum([len(dg_list) for dg_list in dg_map.values()])
        self.number_of_pings = len(ping_index)
        self.max_tx_sectors = file_max_sectors
        self.max_beams = file_max_beams

        if self.number_of_pings != len(dg_map[dg_id]):
            self.datagram_splits = True
            self.split_start_index = ping_index
            self.split_num_dgs = num_dg_in_ping

        logger.debug(f"File map success")
        return True

    def read_datagram(self):    # TODO: Create custom typing for datagram objects.
        """ Reads the next datagram in the datagram in the file

        Reads a single datagram from the file based on the following conditions:
        1. If a valid/open file object exists, it extracts the datagram at the
        current file pointer position.
        2. If the file is closed, the file is reopened and the pointer is moved to the
        position from self.location.

        This method of reading datagrams does minimal context management. When at end of
        file, the method will return an empty byte string. If a 'get_data' type
        method is used, read datagram will open a new file instance and return to the
        file location from the last call of this method, in other words it picks up
        where it left off.
        """

        hdr_size = kmall_dispatch[b'KMALL'].header_size
        # Check if file is open, if not open and move to location. If first time,
        # location will be 0 so code is valid for starting case.
        if self.file.closed is True:
            self.file = open(self.file_path, mode='rb')
            self.file.seek(self.location, 0)

        # Check if at end of file
        if self.end_of_file is True:
            logger.debug("End of File. Reset km_file to start from beginning")
            return b''

        chunk = self.file.read(hdr_size)
        if chunk == b'':
            logger.debug("End of file reached. Reset km_file to stat from beginning")
            self.end_of_file = True
            return chunk

        # Parse the header
        dg_hdr, loc = kmall_dispatch[b'KMALL'].parse(chunk)
        logger.debug(f" Datagram ID: {dg_hdr.id.decode('UTF-8'):10}Size: "
                     f"{dg_hdr.size: 10}")
        self.file.seek(-hdr_size, 1)

        # Read datagram
        chunk = self.file.read(dg_hdr.size)
        datagram = kmall_dispatch[dg_hdr.id].parse(chunk)

        self.location = self.file.tell()
        return datagram

    def reset_file(self):
        if self.file.closed is False:
            self.file.close()
        self.location = 0
        self.end_of_file = False
        logger.debug(f"km_file: {self.file_path} has been reset")
        return

    def get_datagrams_from_map_entry(self, map_entries: Union[List[_AbstractMapEntry],
                                                            _AbstractMapEntry]) -> List:
        # Convert to a list so we can loop
        if map_entries is not type(List):
            map_entries = [map_entries]

        selected_datagrams = list()
        with self.file_path.open('rb') as self.file:
            for map_entry in map_entries:
                self.file.seek(map_entry.file_location, 0)

                # Get the header to check datagram id
                chunk = self.file.read(kmall_dispatch[b'KMALL'].header_size)
                dg_hdr, _ = kmall_dispatch[b'KMALL'].parse(chunk)

                # Parse the datagram
                self.file.seek(-20, 1)
                chunk = self.file.read(map_entry.record_size)
                selected_datagrams.append(kmall_dispatch[dg_hdr.id].parse(chunk))
        return selected_datagrams

    def get_all_datagrams_of_type(self, dg_id: bytes) -> list:
        """ Extracts all records matching the input id

        Input:
            dg_id       - Datagram identifier used to indicate which datagram type to
                            pull

        Output:
            selected_datagrams  - List of all datagram objects that match the input ID.
        """
        if dg_id not in kmall_dispatch.keys():
            raise AttributeError(f"Invalid datagram id: {dg_id}")

        dg_list = self.map[dg_id]
        selected_datagrams = list()
        with self.file_path.open('rb') as self.file:
            for dg_map_entry in dg_list:
                self.file.seek(dg_map_entry.file_location)
                chunk = self.file.read(dg_map_entry.record_size)
                datagram = kmall_dispatch[dg_id].parse(chunk)
                selected_datagrams.append(datagram)

        return selected_datagrams

    def get_position(self, source: bytes = b'#SPO'):
        """ Extracts all position records within file

        Input:
            source      - Datagram identifier used to indicate which datagrams
                            to read. Default is SPO datagram

        Output:
            record_datetime - list of datetime objects corresponding to
                                sensor timestamp for each record
            lat             - numpy array of latitude values (DD)
            lon             - numpy array of longitude values (DD)
            height          - numpy array of ellipsoid height values (meters)
            epgs            - epgs code for position values

        Note on source:
            The MRZ datagram provides position at time of transmission (mid-
            point of first tx transmission). Ping time is the same as
            datagram timestamp. For the SPO, CPO, and SKM datagrams,
            this function provides the sensor timestamp, not the timestamp
            associated with the datagram.
        """

        # KMALL/KMWCD Only provide data in wgs84
        epgs = 4327

        # Check valid source
        valid_sources = [b'#MRZ', b'#SKM', b'#SPO', b'#CPO']
        if source not in valid_sources:
            raise AttributeError(f"Invalid source datagram: {source}. Valid "
                                 f"sources are: {valid_sources}")

        # Loop over records
        dg_list = self.map[source]
        record_datetime = list()

        with self.file_path.open('rb') as self.file:
            if self.datagram_splits is True:
                raise NotImplementedError("This functionality is not yet "
                                          "available")
            if source == valid_sources[0]:  # #MRZ
                lat = np.empty(shape=[self.number_of_pings])
                lon = np.empty(shape=[self.number_of_pings])
                height = np.empty(shape=[self.number_of_pings])
                ping_ct = list()
                for index, dg_info in enumerate(dg_list):
                    # TODO: Add get_position to datagram to not read in so
                    #  much data
                    self.file.seek(dg_info.file_location)
                    chunk = self.file.read(dg_info.record_size)
                    dg_datetime = dg_info.record_datetime
                    mrz = kmall_dispatch[source].parse(data=chunk)

                    record_datetime.append(dg_datetime)
                    lat[index] = mrz.ping_info.latitude
                    lon[index] = mrz.ping_info.longitude
                    height[index] = mrz.ping_info.ellipsoid_height

            elif source == valid_sources[1]:    # #SKM
                # Really we probably don't want to use this as a source. It's
                # got the resolution of the attitude records and likely
                # interpolated to an attitude record time i.e. overkill
                lat = np.empty(0)
                lon = np.empty(0)
                height = np.empty(0)

                for dg_info in dg_list:
                    self.file.seek(dg_info.file_location, 0)
                    chunk = self.file.read(dg_info.record_size)
                    skm = kmall_dispatch[source].parse(chunk)
                    lat = np.append(arr=lat, values=skm.position.latitude)
                    lon = np.append(arr=lon,
                                    values=np.asarray(skm.position.longitude))
                    height = np.append(arr=height,
                                       values=np.asarray(
                                           skm.position.ellipsoid_height))
                    record_datetime.extend(skm.record_info.date_time)

            else:   # #SPO, #CPO
                num_po = len(dg_list)
                lat = np.empty(shape=[num_po])
                lon = np.empty(shape=[num_po])
                height = np.empty(shape=[num_po])
                for index, dg_info in enumerate(dg_list):
                    self.file.seek(dg_info.file_location, 0)
                    chunk = self.file.read(dg_info.record_size)
                    spo = kmall_dispatch[source].parse(chunk)
                    lat[index] = spo.sensor_data.latitude_corrected
                    lon[index] = spo.sensor_data.longitude_corrected
                    height[index] = spo.sensor_data.ellipsoid_height
                    record_datetime.append(spo.sensor_data.date_time)

                # Clean Invalid records
                invalid_token = spo.id_pos_unavailable
                invalid_indicies = lat == invalid_token
                lat = lat[~invalid_indicies]
                lon = lon[~invalid_indicies]
                height = height[~invalid_indicies]
                tmp = np.asarray(record_datetime)
                tmp = tmp[~invalid_indicies]
                record_datetime = tmp.tolist()

        return np.asarray(record_datetime), lat, lon, height, epgs

    def get_attitude(self):
        """ Extracts all attitude records within file

        Output:
            record_datetime - list of datetime objects corresponding attitude
                                sample
            roll            - numpy array of roll values (degrees)
            pitch           - numpy array of pitch values (degrees)
            heading         - numpy array of heading values (degrees)
            heave           - numpy array of heave values (meters)
            delayed_heave   - numpy array of delayed heave timestamps
                                and values (meters) if available [N,2]
        """
        source = b'#SKM'

        # Loop over records
        dg_list = self.map[source]
        record_datetime = list()
        roll = np.empty(shape=(0, 1))
        pitch = np.empty(shape=(0, 1))
        heading = np.empty(shape=(0, 1))
        heave = np.empty(shape=(0, 1))
        dly_heave = np.empty(shape=(0, 1))
        dly_time = list()
        with self.file_path.open(mode='rb') as self.file:
            for dg_info in dg_list:
                self.file.seek(dg_info.file_location, 0)
                chunk = self.file.read(dg_info.record_size)
                skm = kmall_dispatch[source].parse(chunk)
                roll = np.append(arr=roll, values=skm.attitude.roll)
                pitch = np.append(arr=pitch, values=skm.attitude.pitch)
                heading = np.append(arr=heading, values=skm.attitude.heading)
                heave = np.append(arr=heave, values=skm.attitude.heave)
                dly_heave = np.append(arr=dly_heave,
                                      values=skm.delayed_heave.heave)
                dly_time.extend(skm.delayed_heave.date_time)
                record_datetime.extend(skm.record_info.date_time)

            if np.all(np.abs(dly_heave) > 100):
                delayed_heave = None
            else:
                delayed_heave = [np.asarray(dly_time), dly_heave]

        return np.asarray(record_datetime), roll, pitch, heading, heave,  delayed_heave

    def get_sonar_settings(self):
        """ Extracts all the ping_info records and

        Pulls pings settings from ping info

        Output:
            tx_beamwidth
            rx_beamwidth
            pulse_length_eff
            pulse_length_full
            pulse_bandwidth
            pulse_center_frequency
            sample_rate
            pulse_delay
            bs_normal
            bs_oblique
            tx_steering

            num_tx
            bs_corr_offset
            lamberts_active

        """
        source = b'#MRZ'
        dg_list = self.map[source]

        record_datetime = list()

        frequency = np.empty(shape=[self.number_of_pings, self.max_beams])
        tmit_delay = frequency.copy()
        tx_steering = frequency.copy()
        pulse_length_effective = frequency.copy()
        pulse_length_full = frequency.copy()
        bandwidth = frequency.copy()

        lamberts_law = np.empty(shape=[self.number_of_pings])
        lamberts_law.fill(np.nan)
        bs_offset = lamberts_law.copy()
        bs_normal = lamberts_law.copy()
        bs_oblique = lamberts_law.copy()
        sbi_samp_rate = lamberts_law.copy()
        tx_beamwidth = lamberts_law.copy()
        rx_beamwidth = lamberts_law.copy()
        with self.file_path.open(mode='rb') as self.file:

            # Pings are not split, loop over each datagram
            if self.datagram_splits is False:
                for ii, dg_info in enumerate(dg_list):
                    self.file.seek(dg_info.file_location)
                    chunk = self.file.read(dg_info.record_size)
                    mrz = kmall_dispatch[source].parse(chunk)

                    # Pull ping info data
                    tx_beamwidth[ii] = mrz.ping_info.tx_array_size_used
                    rx_beamwidth[ii] = mrz.ping_info.rx_array_size_used

                    bs_offset[ii] = mrz.ping_info.bs_corr_offset
                    lamberts_law[ii] = mrz.ping_info.lamberts_law_active

                    # pull rx info
                    bs_normal[ii] = mrz.rx_info.bs_normal
                    bs_oblique[ii] = mrz.rx_info.bs_oblique
                    sbi_samp_rate[ii] = mrz.rx_info.sample_rate_sb

                    # Pull tx sector info
                    for jj in range(mrz.ping_info.number_tx_sectors):
                        inds = np.asarray(mrz.detection_info.sector_index)
                        frequency[ii, inds == jj] = \
                            mrz.tx_sector_info.center_frequency[jj]
                        tmit_delay[ii, inds == jj] = \
                            mrz.tx_sector_info.delay[jj]
                        tx_steering[ii, inds == jj] = \
                                mrz.tx_sector_info.tilt_angle[jj]
                        pulse_length_effective[ii, inds == jj] = \
                                mrz.tx_sector_info.pulse_length_effective[jj]
                        pulse_length_full[ii, inds == jj] = \
                                mrz.tx_sector_info.pulse_length_total[jj]
                        bandwidth[ii, inds == jj] = \
                                mrz.tx_sector_info.bandwidth[jj]

                    record_datetime.append(dg_info.record_datetime)
            else:  # Pings are split
                raise NotImplemented("Split datagram functionality is not yet implemented")

        return np.asarray(record_datetime), tx_beamwidth, rx_beamwidth, \
               bs_offset, lamberts_law, bs_normal, bs_oblique, sbi_samp_rate, \
               frequency, tmit_delay, tx_steering, pulse_length_effective,\
               pulse_length_full, bandwidth

    def get_source_level(self):
        """ Extracts the source level directly from the sounding records

        Source level is provided as a per beam SL in the sounding record.
        """
        source = b'#MRZ'
        dg_list = self.map[source]

        record_datetime = list()

        source_level = np.empty(shape=[self.number_of_pings, self.max_beams])
        source_level.fill(np.nan)
        with self.file_path.open(mode='rb') as self.file:

            # Pings are not split, loop over each datagram
            if self.datagram_splits is False:
                for ii, dg_info in enumerate(dg_list):
                    self.file.seek(dg_info.file_location)
                    chunk = self.file.read(dg_info.record_size)
                    mrz = kmall_dispatch[source].parse(chunk)
                    source_level[ii, mrz.detection_info.index] = \
                        mrz.reflectivity.source_level
                    record_datetime.append(dg_info.record_datetime)
            else:  # Pings are split
                raise NotImplemented("Split datagram functionality is not yet implemented")
                # TODO: Implement the split datagram functionality and test.
                #  Suggested implementation below
                # for ii in range(self.number_of_pings):
                #     record_datetime.append(
                #         dg_list[self.split_start_index[ii]][1])
                #     for jj in range(self.split_num_dgs[ii]):
                #         dg_info = dg_list[self.split_start_index[ii] + jj]
                #         self.file.seek(dg_info.file_location, 0)
                #         chunk = self.file.read(dg_info.record_size)
                #         mrz = dispatch[source].parse(chunk)
                #         beam_index = mrz.detection_info.index
                #         source_level[ii, beam_index] = mrz.reflectivity.source_level

        return np.asarray(record_datetime), source_level

    def get_calibration_gains(self):
        """ Extracts the backscatter calibration values and receiver
        sensitivity

        Kongsberg backscatter values (reflectivity_2) is compensated
        according to :
        BS = EL - SL - M + TVG + BScorr
        This function returns M and BScorr for backscatter processing

        Output:
            M           - receiver sensitivity applied (dB)
            BScorr      - backscatter calibration value (dB)
        """
        source = b'#MRZ'
        dg_list = self.map[source]

        record_datetime = list()

        m = np.empty(shape=[self.number_of_pings, self.max_beams])
        m.fill(np.nan)
        bscorr = m.copy()

        with self.file_path.open(mode='rb') as self.file:

            # Pings are not split, loop over each datagram
            if self.datagram_splits is False:
                for ii, dg_info in enumerate(dg_list):
                    self.file.seek(dg_info.file_location)
                    chunk = self.file.read(dg_info.record_size)
                    mrz = kmall_dispatch[source].parse(chunk)
                    m[ii, mrz.detection_info.index] = \
                        mrz.reflectivity.rx_sensitivity
                    bscorr[ii, mrz.detection_info.index] = \
                        mrz.reflectivity.calibration

                    record_datetime.append(dg_info.record_datetime)
            else:  # Pings are split
                raise NotImplemented(
                    "Split datagram functionality is not yet implemented")

        return np.asarray(record_datetime), m, bscorr

    def get_tvg_per_sounding(self):
        """ Extracts the tvg values from the mrz records

        Kongsberg provides the per sounding tvg values for compensating
        backscatter values. This function pulls the tvg values and returns
        them in a ping x beam array

        Output:
            record_datetime - list of datetime objects corresponding to ping time
            tvg             - tvg value at detect point for each sounding (dB)
        """
        # TODO: I doubt this handles extra detections.
        source = b'#MRZ'
        dg_list = self.map[source]

        record_datetime = list()

        tvg = np.empty(shape=[self.number_of_pings, self.max_beams])
        tvg.fill(np.nan)
        with self.file_path.open(mode='rb') as self.file:

            # Pings are not split, loop over each datagram
            if self.datagram_splits is False:
                for ii, dg_info in enumerate(dg_list):
                    self.file.seek(dg_info.file_location)
                    chunk = self.file.read(dg_info.record_size)
                    mrz = kmall_dispatch[source].parse(chunk)
                    tvg[ii, mrz.detection_info.index] = mrz.reflectivity.tvg
                    record_datetime.append(dg_info.record_datetime)
            else:   # Pings are split
                raise NotImplemented("Split datagram functionality is not yet implemented")

        return np.asarray(record_datetime), tvg

    def get_tvg_gain_settings(self):
        """ Extracts the tvg values from the mrz records

        Kongsberg provides the gain settings used to calculate a beam
        relative tvg curve.
        TVG = Xlog(R) + 2 alpha*R

        Output:
            record_datetime - list of datetime objects corresponding to ping time
            X               - Spreading Gain, constant for all beams (dB)
            alpha           - Mean absorption coefficient (db/m)

        *** NOTE: Currently do not know exactly what X is....
        """
        # TODO: tvg settings requires an MWC datagram to accurately calculate
        # TODO: I doubt this handles extra detections.
        source = b'#MRZ'
        dg_list = self.map[source]

        record_datetime = list()

        x = np.empty(shape=[self.number_of_pings])
        x.fill(np.nan)
        alpha = np.empty(shape=[self.number_of_pings, self.max_beams])
        alpha.fill(np.nan)

        with self.file_path.open(mode='rb') as self.file:

            # Pings are not split, loop over each datagram
            if self.datagram_splits is False:
                for ii, dg_info in enumerate(dg_list):
                    self.file.seek(dg_info.file_location)
                    chunk = self.file.read(dg_info.record_size)
                    mrz = kmall_dispatch[source].parse(chunk)
                    alpha[ii, mrz.detection_info.index] = mrz.reflectivity.mean_abs
                    record_datetime.append(dg_info.record_datetime)
            else:   # Pings are split
                raise NotImplemented("Split datagram functionality is not yet implemented")

        return np.asarray(record_datetime), x, alpha

    def get_raw_range(self):
        """ Extracts all the raw range bathymetric information

        This method pulls only the raw range and angle data from the MRZ
        record. Use get_xyz to the Kongsberg determined positions of each
        sounding.

        Output:
            record_datetime - list of datetime objects corresponding pingtime
            c_tx            - soundspeed at transducer (m/s).
            z_tx            - depth of transducer (m)
            z_wtl           - waterline offset from refernce point (m)
            tx_angle        - along angle of the tx beam (degrees)
            tx_id           - identifer used to map a sounding to a sector
            rx_angle        - the athwartship angle of the rx beam (degrees)
            rx_angle_corr   - the angle correction for rx_angle (degrees)
            twtt            - the two way travel time (s)
            twtt_corr       - the correction for twtt (s)


        """
        source = b"#MRZ"
        dg_list = self.map[source]

        record_datetime = list()

        c_tx = np.empty(shape=(self.number_of_pings))
        z_tx = np.empty(shape=(self.number_of_pings))
        z_wtl = np.empty(shape=(self.number_of_pings))
        tx_angle = np.empty(shape=(self.number_of_pings, self.max_tx_sectors))
        tx_angle.fill(np.nan)
        tx_id = np.empty(shape=(self.number_of_pings, self.max_beams))
        tx_id.fill(np.nan)
        rx_angle = np.empty(shape=(self.number_of_pings, self.max_beams))
        rx_angle.fill(np.nan)
        rx_angle_corr = np.empty(shape=(self.number_of_pings, self.max_beams))
        rx_angle_corr.fill(np.nan)
        twtt = np.empty(shape=(self.number_of_pings, self.max_beams))
        twtt.fill(np.nan)
        twtt_corr = np.empty(shape=(self.number_of_pings, self.max_beams))
        twtt_corr.fill(np.nan)

        with self.file_path.open(mode='rb') as self.file:

            # Pings are not split, loop over each datagram
            if self.datagram_splits is False:
                for ii, dg_info in enumerate(dg_list):
                    dg_datetime = dg_info.record_datetime
                    self.file.seek(dg_info.file_location, 0)
                    chunk = self.file.read(dg_info.record_size)
                    mrz = kmall_dispatch[source].parse(chunk)

                    record_datetime.append(dg_datetime)

                    c_tx[ii] = mrz.ping_info.sound_speed_at_tx
                    z_tx[ii] = mrz.ping_info.tx_depth
                    z_wtl[ii] = mrz.ping_info.waterline

                    sector_id = mrz.tx_sector_info.sector_index
                    tx_angle[ii, sector_id] = mrz.tx_sector_info.tilt_angle

                    beam_id = mrz.detection_info.index
                    tx_id[ii, beam_id] = mrz.detection_info.sector_index
                    rx_angle[ii, beam_id] = mrz.raw_range_angle.beam_angle
                    rx_angle_corr[ii, beam_id] = mrz.raw_range_angle.beam_angle_correction
                    twtt[ii, beam_id] = mrz.raw_range_angle.twtt
                    twtt_corr[ii, beam_id] = mrz.raw_range_angle.twtt_correction

            else:   # Ping is split over multiple datagrams
                raise NotImplemented(
                    "Split datagram functionality is not yet implemented")
                # TODO: Not tested, need a file with split datagrams
                # for ii in range(self.number_of_pings):
                #
                #     # Grab ping information from first record. Assuming that
                #     # ping info between split records is constant
                #     dg_info_0 = dg_list[self.split_start_index[ii]]
                #     dg_datetime = dg_info_0[1]
                #     self.file.seek(dg_info_0[0], 0)
                #     chunk = self.file.read(dg_info_0[2])
                #
                #     mrz_0 = dispatch[source].parse(chunk)
                #     record_datetime.append(dg_datetime)
                #     c_tx[ii] = mrz_0.ping_info.sound_speed_at_tx
                #     z_tx[ii] = mrz_0.ping_info.tx_depth
                #     z_wtl[ii] = mrz_0.ping_info.waterline
                #
                #     # Fill in ping X beam of first datagram
                #     sector_id = mrz_0.tx_sector_info.sector_index
                #     tx_angle[ii, sector_id] = mrz_0.tx_sector_info.tilt_angle
                #
                #     beam_id = mrz_0.detection_info.index
                #     tx_id[ii, beam_id] = mrz_0.detection_info.sector_index
                #     rx_angle[ii, beam_id] = mrz_0.raw_range_angle.beam_angle
                #     rx_angle_corr[ii, beam_id] = mrz_0.raw_range_angle.beam_angle_correction
                #     twtt[ii, beam_id] = mrz_0.raw_range_angle.twtt
                #     twtt_corr[ii, beam_id] = mrz_0.raw_range_angle.twtt_correction
                #
                #     # Now loop over remaining datagrams and fill.
                #     for jj in range(self.split_num_dgs[ii]):
                #         dg_info = dg_list[self.split_start_index[ii] + jj]
                #         self.file.seek(dg_info.file_location, 0)
                #         chunk = self.file.read(dg_info.record_size)
                #         mrz = dispatch[source].parse(chunk)
                #         beam_id = mrz.detection_info.index
                #         tx_id[ii, beam_id] = mrz.detection_info.sector_index
                #         rx_angle[ii, beam_id] = mrz.raw_range_angle.beam_angle
                #         rx_angle_corr[ii, beam_id] = mrz.raw_range_angle.beam_angle_correction
                #         twtt[ii, beam_id] = mrz.raw_range_angle.twtt
                #         twtt_corr[ii, beam_id] = mrz.raw_range_angle.twtt_correction

        return np.asarray(record_datetime), c_tx, z_tx, z_wtl, tx_angle, tx_id,\
               rx_angle, rx_angle_corr, twtt, twtt_corr

    def get_xyz(self):
        """ Extracts the Kongsberg determined x, y, and z for each sounding

        This pulls in the kongsberg calculated positional offsets for each
        sounding. Unlike .all xyz, the output here is for the geographic
        frame (delta lon/lat) and for the surface coordinate system (x,y,z)

        Output:
            record_datetime     - ping datetime
            lat                 - geographic latitude position of ping
            lon                 - geographic longitude position of ping
            x                   - x offset of sounding re vessel reference point
            y                   - y offset of sounding re vessel reference point
            z                   - z offset of sounding re vessel reference point
        """
        # TODO: The x, y, z re ref point are relative to the surface
        #  coordinate system. This is different from the vessel coordinate
        #  system and the fixed coordinate system. Gotta think there.
        source = b"#MRZ"
        dg_list = self.map[source]

        record_datetime = list()

        lon = np.empty(shape=(self.number_of_pings, self.max_beams))
        lon.fill(np.nan)
        lat = lon.copy()
        ellipsoid_height = lon.copy()

        x = np.empty(shape=(self.number_of_pings, self.max_beams))
        y = np.empty(shape=(self.number_of_pings, self.max_beams))
        z = np.empty(shape=(self.number_of_pings, self.max_beams))

        with self.file_path.open(mode='rb') as self.file:
            # Pings are not split, loop over each datagram
            if self.datagram_splits is False:
                for ii, dg_info in enumerate(dg_list):
                    dg_datetime = dg_info.record_datetime
                    self.file.seek(dg_info.file_location, 0)
                    chunk = self.file.read(dg_info.record_size)
                    mrz = kmall_dispatch[source].parse(chunk)

                    record_datetime.append(dg_datetime)

                    beam_id = mrz.detection_info.index
                    lon[ii, beam_id] = \
                        np.asarray(mrz.geo_referenced_depths.delta_longitude) +\
                        mrz.ping_info.longitude
                    lat[ii, beam_id] = \
                        np.asarray(mrz.geo_referenced_depths.delta_latitude) + \
                        mrz.ping_info.latitude
                    ellipsoid_height[ii, beam_id] =  \
                        np.asarray(mrz.geo_referenced_depths.z) + \
                        mrz.ping_info.ellipsoid_height
                    x[ii, beam_id] = mrz.geo_referenced_depths.x
                    y[ii, beam_id] = mrz.geo_referenced_depths.y
                    z[ii, beam_id] = mrz.geo_referenced_depths.z

            else:       # Pings are split over multiple datagrams
                raise NotImplemented("Split datagram functionality is not yet implemented")

        return np.asarray(record_datetime), lat, lon, ellipsoid_height, x, y, z

    def get_backscatter(self, source: str = 'bs_2'):
        """ Extracts per beam backscatter data from within file

        Inputs:

        source              - String identifier used to determine which
                                backscatter data type to pull from file.
                                Accepts:
                                    'bs_1'
                                    'bs_2'

        Output:

        bs_data             - numpy array of data

        """
        valid_sources = ['bs_1', 'bs_2']
        if source not in valid_sources:
            raise AttributeError(f"source is not found in valid sources. "
                                 f"{source}, {valid_sources}")

        dg_list = self.map[b'#MRZ']
        record_datetime = list()
        bs_data = np.empty(shape=[self.number_of_pings, self.max_beams])
        bs_data.fill(np.nan)

        with self.file_path.open(mode='rb') as self.file:
            if self._check_for_splits(dg_list) is True:
                raise NotImplementedError()

            for ii, dg_info in enumerate(dg_list):
                record_datetime.append(dg_info.record_datetime)
                self.file.seek(dg_info.file_location, 0)
                mrz = kmall_dispatch[b'#MRZ'].parse(self.file.read(dg_info.record_size))

                beam_index = mrz.reflectivity.index
                if source == valid_sources[0]:
                    bs_data[ii, beam_index] = mrz.reflectivity.bs_type_1
                else:
                    bs_data[ii, beam_index] = mrz.reflectivity.bs_type_2

        return np.asarray(record_datetime), bs_data

    def get_snippets(self):
        """ Extracts the snippets (seabed imagery) from the file"""
        record_datetime = list()
        bs_data = list()
        beam_index = list()
        center_sample = np.empty(shape=[self.number_of_pings, self.max_beams])
        center_sample.fill(np.nan)
        start_sample = center_sample.copy()
        num_samples = start_sample.copy()
        samp_rate = np.empty(shape=[self.number_of_pings])

        source = b'#MRZ'
        dg_list = self.map[source]

        with self.file_path.open(mode='rb') as self.file:
            if self._check_for_splits(dg_list) is True:
                raise NotImplementedError

            for ii, dg_info in enumerate(dg_list):
                record_datetime.append(dg_info.record_datetime)
                self.file.seek(dg_info.file_location, 0)
                mrz = kmall_dispatch[source].parse(self.file.read(dg_info.record_size))

                index = mrz.seabed_imagery.index
                beam_index.append(index)
                samp_rate[ii] = mrz.rx_info.sample_rate_sb

                bs_data.append(mrz.seabed_imagery.snippets)
                center_sample[ii, index] = mrz.seabed_imagery.center_sample
                start_sample[ii, index] = mrz.seabed_imagery.starting_sample
                num_samples[ii, index] = mrz.seabed_imagery.number_samples

        # Package the output
        max_snip_len = int(np.nanmax(num_samples))
        snippets = np.empty(
            shape=[self.number_of_pings, self.max_beams, max_snip_len])
        snippets.fill(np.nan)

        for ii, index in enumerate(beam_index):
            # bs_data shape is not ragged, so just fill snippets based on
            # bs_data[ii] size
            _, ping_snip_len = bs_data[ii].shape
            snippets[ii, index, 0:ping_snip_len] = bs_data[ii]

        if np.all(samp_rate == samp_rate[0]):
            samp_rate = samp_rate[0]

        return np.asarray(record_datetime), snippets, center_sample, \
            start_sample, samp_rate

    def get_svp(self):
        """ Extract the Sound speed profiles from the file

        Output:

        svp_list:           - list of all svp datagrams in the file

        """
        source = b'#SVP'
        with self.file_path.open(mode='rb') as self.file:
            dg_list = self.map[source]
            svp_list = list()

            for dg_info in dg_list:
                self.file.seek(dg_info.file_location, 0)
                svp = kmall_dispatch[source].parse(self.file.read(dg_info.record_size))
                svp_list.append(svp)

        # Remove duplicate entries
        dt_p = [svp.info.date_time for svp in svp_list]
        _, inds = np.unique(np.asarray(dt_p), return_index=True)
        svp_list = np.asarray(svp_list)[inds]

        return svp_list.tolist()

    def get_installation(self):
        """ Returns the installation parameters dictionary"""
        with self.file_path.open(mode='rb') as self.file:
            dg_info = self.map[b'#IIP'][0]

            self.file.seek(dg_info.file_location)
            chunk = self.file.read(dg_info.record_size)
            iip = kmall_dispatch[b'#IIP'].parse(chunk)

        return iip.vessel_install

    # Private Methods
    def _validate_file(self):
        extension = self.extension

        if extension not in file_types.keys():
            msg = f"Invalid file extension: {self.file_path.absolute()}"
            raise OSError(msg)
        else:
            self.file_type = file_types[extension][0]

        if self.file_path.exists() is False:
            msg = f"File does not exist: {self.file_path.absolute()}"
            raise OSError(msg)

        with self.file_path.open(mode='rb') as self.file:
            self.file.seek(0, 2)
            self.file_size = self.file.tell()
            self.file.seek(0, 0)
            self.location = self.file.tell()
        return True

    def _check_for_splits(self, dg_list: list) -> bool:
        """ Checks for split datagrams in MRZ/MWC ping records"""
        if self.number_of_pings == len(dg_list):
            ping_splits = False
        else:
            ping_splits =True
        return ping_splits


@attr.s(auto_attribs=True)
class _KmallMapEntry(_AbstractMapEntry):
    """ Map entry class for KMALL data files."""
    version: int
