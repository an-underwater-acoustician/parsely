import attr
import logging
import struct

from datetime import datetime, timedelta

import numpy as np
from numpy.typing import NDArray
from typing import BinaryIO, ClassVar, List, Optional

from parsely.internal.metadata_mappings import MetadataKeys as MdK
from parsely.internal.metadata_mappings import Units
from parsely.nmea.nmea import GGA, ZDA

logger = logging.getLogger(__name__)

null_int = -999
null_float = -999.0


# #### Datagram Objects ####
# *** Note: Dataclass objects cannot contain child dataclasses without prior
# declaration. As a result, all datagrams are organized as such:
#   class child class:  (sub structure)
#       ...
#
#   class parent class: (primary datagram)
#       ...
#

# ### Datagram Header ###
@attr.s(auto_attribs=True)
class Kmall:
    """ KMALL Datagram Header Structure """
    header_fmt: ClassVar[str] = '<I4c2BH2I'
    desc: ClassVar[str] = "KMALL header"
    dg_max_size: ClassVar[int] = 64000
    header_size: ClassVar[int] = struct.calcsize(header_fmt)

    size: int
    id: bytes
    version: int
    system_id: int
    sounder_id: int
    record_datetime: datetime

    @classmethod
    def parse(cls, data: bytes, loc: int = 0):
        hdr_st = loc
        hdr_end = loc + cls.header_size
        hdr_data = struct.unpack(cls.header_fmt, data[hdr_st:hdr_end])
        dg_datetime = km_datetime(time_sec=hdr_data[8], time_nanosec=hdr_data[9])

        # noinspection PyArgumentList
        return cls(size=hdr_data[0],
                   id=b''.join(hdr_data[1:5]),
                   version=hdr_data[5],
                   system_id=hdr_data[6],
                   sounder_id=hdr_data[7],
                   record_datetime=dg_datetime), hdr_end

    @classmethod
    def skip(cls, file_obj: BinaryIO):
        return _skip_simple(file_obj, cls.header_fmt)
    # TODO: Propagate the skip method to all datagrams


# ### Installation Datagrams ###
# ## Installation Common Datagrams ##
@attr.s(auto_attribs=True)
class IInfo:
    """ Installation Info, a child structure to IIP and IOP """
    fmt: ClassVar[str] = '<3H'

    text_size: int
    info: int
    status: int

    @classmethod
    def parse(cls, data: bytes, loc: int):
        i_sz = struct.calcsize(cls.fmt)
        i_1 = loc
        i_2 = i_1 + i_sz
        i_struct = struct.unpack(cls.fmt, data[i_1:i_2])
        loc += i_sz
        text_sz = i_struct[0] - i_sz
        # noinspection PyArgumentList
        return cls(text_size=text_sz, info=i_struct[1], status=i_struct[2]), loc


# ## IIP ##
@attr.s(auto_attribs=True)
class IIP:
    """ Installation parameters Parent Structure """
    # TODO: Improve this parser. Make it match our dataclass format
    desc: ClassVar[str] = "Installation parameters and sensor setup"

    header: Kmall
    info: IInfo
    vessel_install: dict

    @classmethod
    def parse(cls, data: bytes, loc: int = 0):
        km_header, loc = Kmall.parse(data=data, loc=loc)
        i_info, loc = IInfo.parse(data=data, loc=loc)

        # Parse the text structure
        iip_sz = i_info.text_size
        iip_fmt = f'{iip_sz}c'
        iip_1 = loc
        iip_2 = iip_1 + iip_sz
        iip_text = struct.unpack(iip_fmt, data[iip_1:iip_2])
        iip_text = b''.join(iip_text).decode('utf-8').replace('\n', '')
        v_install = cls.iip_text_parser(iip_text)
        # TODO: Verify. It's not mentioned in the docs but if the text size
        #  is odd, a byte is padded after the text
        loc = iip_2
        if iip_sz % 2 == 1:
            loc += 1
        checksum(data=data, dg_sz=km_header.size, chk_st=loc)
        # noinspection PyArgumentList
        return cls(header=km_header, info=i_info, vessel_install=v_install)

    @classmethod
    def iip_instrument_parser(cls, device_id: str, values_string: str) -> dict:
        """ Parses the iip instument parameters and settings"""
        # device translator will use the device identifier plus the values here,
        # ex: 'TRAI_HD1' + '_serial_number'
        translate_device_ident = {
            'TRAI_TX1': 'transducer_1',
            'TRAI_TX2': 'transducer_2',
            'TRAI_RX1': 'receiver_1',
            'TRAI_RX2': 'receiver_2',
            'ATTI_1': 'motion_sensor_1',
            'ATTI_2': 'motion_sensor_2',
            'ATTI_3': 'motion_sensor_3',
            'POSI_1': 'position_1',
            'POSI_2': 'position_2',
            'POSI_3': 'position_3',
            'CLCK': 'clock',
            'SVPI': 'sound_velocity_1',
            'DPHI': 'depth_pressure_sensor',
            'EMXI': 'system'
        }

        translate_transducer = {
            'N': 'serial_number',
            'X': 'along_location',
            'Y': 'athwart_location',
            'Z': 'vertical_location',
            'R': 'roll_angle',
            'P': 'pitch_angle',
            'H': 'heading_angle',
            'G': 'gain',
            'S': 'sounder_size_deg',
            'V': 'version',
            'W': 'system_description',
            'IPX=': 'port_sector_forward',
            'IPY=': 'port_sector_starboard',
            'IPZ=': 'port_sector_down',
            'ICX=': 'center_sector_forward',
            'ICY=': 'center_sector_starboard',
            'ICZ=': 'center_sector_down',
            'ISX=': 'starboard_sector_forward',
            'ISY=': 'starboard_sector_starboard',
            'ISZ=': 'starboard_sector_down',
            'ITX=': 'tx_forward',
            'ITY=': 'tx_starboard',
            'ITZ=': 'tx_down',
            'IRX=': 'rx_forward',
            'IRY=': 'rx_starboard',
            'IRZ=': 'rx_down'
        }
        translate_position = {
            'X': 'along_location',
            'Y': 'athwart_location',
            'Z': 'vertical_location',
            'D': 'time_delay',
            'G': 'datum',
            'T': 'time_stamp',
            'C': 'motion_compensation',
            'F': 'data_format',
            'Q': 'quality_check',
            'I': 'input_source',
            'U': 'active_passive',
        }
        translate_motion = {
            'X': 'along_location',
            'Y': 'athwart_location',
            'Z': 'vertical_location',
            'R': 'roll_angle',
            'P': 'pitch_angle',
            'H': 'heading_angle',
            'D': 'time_delay',
            'M': 'motion_reference',
            'F': 'data_format',
            'I': 'input_source',
            'U': 'active_passive',
        }
        translate_clock = {
            'F': 'data_format',
            'S': 'synchonisation',
            'A': 'IPPS_setting',
            'I': 'input_source',
            'Q': 'sync'
        }
        translate_depth = {
            'X': 'along_location',
            'Y': 'athwart_location',
            'Z': 'vertical_location',
            'D': 'time_delay',
            'O': 'offset',
            'S': 'scale',
            'A': 'added_heave',
            'F': 'data_format',
            'I': 'input_source',
            'U': 'active_passive',
        }
        translate_svp = {
            'F': 'data_format',
            'I': 'input_source',
            'U': 'active_passive',
        }
        translate_system = {
            'SSNL': 'ship_noise',
            'SWLZ': 'water_line'
        }

        id_dispatch = {
            'TRAI_TX1': translate_transducer,
            'TRAI_TX2': translate_transducer,
            'TRAI_RX1': translate_transducer,
            'TRAI_RX2': translate_transducer,
            'ATTI_1': translate_motion,
            'ATTI_2': translate_motion,
            'ATTI_3': translate_motion,
            'POSI_1': translate_position,
            'POSI_2': translate_position,
            'POSI_3': translate_position,
            'CLCK': translate_clock,
            'SVPI': translate_svp,
            'DPHI': translate_depth,
            'EMXI': translate_system
        }

        instrument_dict = dict()

        # get the right dict
        try:
            translate = id_dispatch[device_id]
        except KeyError as e:
            logger.error(f'{e}: Unrecognized device identifier')
            return instrument_dict

        instrument_dict['name'] = translate_device_ident[device_id]

        tokens = values_string.split(';')
        for token in tokens:
            values = token.split('=')
            instrument_dict[translate[values[0]]] = values[1]

        return instrument_dict

    @classmethod
    def iip_text_parser(cls, iip_text: str) -> dict:
        """ Parses iip text to a dictionary

        This function is an adapted version of the
        translate_installation_parameters_todict() function from the kmall
        package written by Val Schmidt (CCOM/JHC 2020)
        https://github.com/valschmidt/kmall/blob/f6a7465b570d8315c5ee60d78545256984756b8b/KMALL/kmall.py#L3980

        The primary difference in this function is an assumed fixed iip
        format so we are going field by field. Loops are conducted over block
        sections such as VERSIONS / SERIAL.
        """
        translate_versions = {
            'OSCV': 'operator_controller_version',
            'EMXV': 'sonar_model_number',
            'PU': 'pu_id_type',
            'SN': 'pu_serial_number',
            'IP': 'ip_address_subnet_mask',
            'UDP': 'command_tcpip_port',
            'TYPE': 'cpu_type',
            'CPU': 'cpu_software_version',
            'VXW': 'vxw_software_version',
            'FILTER': 'filter_software_version',
            'CBMF': 'cbmf_software_version',
            'TX': 'tx_software_version',
            'RX': 'rx_software_version',
            'DCL': 'dcl_version',
            'KMALL': 'kmall_version'
        }
        translate_serial = {
            'TX': 'tx_serial_number',
            'RX': 'rx_serial_number'}

        records = iip_text.split(',')
        iip_dict = dict()

        # Parse the OSCV Section
        tokens = records[0].split(':')
        iip_dict[translate_versions[tokens[0]]] = tokens[1]

        # Parse the multibeam system versions description - Info section
        tokens = records[1].split(':')
        iip_dict[translate_versions[tokens[0]]] = tokens[1]
        tokens = records[2].split('_')
        iip_dict[translate_versions[tokens[0]]] = tokens[1]
        for ii in range(3, 7):
            tokens = records[ii].split('=')
            iip_dict[translate_versions[tokens[0]]] = tokens[1]

        # Parse the multibeam system versions description - Versions section
        stop = False
        ii = 7
        while stop is not True:
            tokens = records[ii].split(':')
            if tokens[0] == 'VERSIONS-END':
                stop = True
                continue

            try:
                iip_dict[translate_versions[tokens[0]]] = tokens[1]
            except KeyError:
                iip_dict[tokens[0]] = tokens[1]
            ii += 1

        # Parse the serial numbers section
        # TODO: Let kongsberg know they are missing a comma
        ii += 1
        tokens = records[ii].split(':')
        if tokens[0] != 'SERIALno':
            raise RuntimeError(f"Issue parsing Installation parameters text: "
                               f"Expected'SERIALno:', got {records[ii]}")
        else:
            iip_dict[translate_serial[tokens[1]]] = tokens[2]
            ii += 1

        stop = False
        while stop is not True:
            tokens = records[ii].split(':')
            if tokens[0] == 'SERIALno-END':
                stop = True
                continue

            try:
                iip_dict[translate_serial[tokens[0]]] = tokens[1]
            except KeyError:
                iip_dict[tokens[0]] = tokens[1]
            ii += 1

        # Parse the last two fields related to versions
        ii += 1
        tokens = records[ii].split(':')
        iip_dict[translate_versions[tokens[0]]] = tokens[1]
        ii += 1
        tokens = records[ii].split(':')
        iip_dict[translate_versions[tokens[0]]] = tokens[1]

        # Now parse installation parameters and settings
        # Note: the format for the rest of the records should be consistent.
        # As a result of the original split, the last record in the records
        # list should be an empty string.
        start = ii + 1
        for ii in range(start, len(records)):
            if records[ii] == '':
                continue
            tokens = records[ii].split(':')
            device_dict = cls.iip_instrument_parser(tokens[0], tokens[1])
            device_id = device_dict.pop('name')
            iip_dict[device_id] = device_dict

        return iip_dict


# ## IOP ##
@attr.s(auto_attribs=True)
class IOP:
    """ Installation Runtime Parameters parent Structure"""
    desc: ClassVar[str] = "Runtime parameters as chosen by operator"

    header: Kmall
    info: IInfo
    runtime_txt: str

    @classmethod
    def parse(cls, data: bytes, loc: int = 0):
        km_header, loc = Kmall.parse(data=data, loc=loc)
        i_info, loc = IInfo.parse(data=data, loc=loc)

        # In future parse text. For now just store
        iop_sz = i_info.text_size
        iop_fmt = f'{iop_sz}c'
        iop_1 = loc
        iop_2 = iop_1 + iop_sz
        iop_text = struct.unpack(iop_fmt, data[iop_1:iop_2])
        iop_text = b''.join(iop_text).decode('utf-8')
        loc = iop_2
        if iop_sz % 2 == 1:
            loc += 1

        checksum(data=data, dg_sz=km_header.size, chk_st=loc)
        # noinspection PyArgumentList
        return cls(header=km_header, info=i_info, runtime_txt=iop_text)


# ## IBE ##
@attr.s(auto_attribs=True)
class IBEInfo:
    """ BIST Info Error Child Structure """
    fmt: ClassVar[str] = '<H3Bb'

    info: int
    style: int
    number: int
    status: int
    text_size: int

    @classmethod
    def parse(cls, data: bytes, loc: int):
        i_sz = struct.calcsize(cls.fmt)
        i_1 = loc
        i_2 = i_1 + i_sz
        i_struct = struct.unpack(cls.fmt, data[i_1:i_2])
        loc += i_sz
        text_sz = i_struct[0] - i_sz

        # noinspection PyArgumentList
        return cls(text_size=text_sz, info=i_struct[1], style=i_struct[2],
                   number=i_struct[3], status=i_struct[4]), loc


@attr.s(auto_attribs=True)
class IBE:
    """ BIST Error Report Parent Structure"""
    # TODO: Not tested
    desc = "Built in test (B) error (E) report."

    header: Kmall
    info: IBEInfo
    bist_text: str

    @classmethod
    def parse(cls, data: bytes, loc: int):
        km_header, loc = Kmall.parse(data=data)
        i_info, loc = IBEInfo.parse(data=data, loc=loc)

        # In future parse text, For now just store
        ib_sz = i_info.text_size
        ib_fmt = f'{ib_sz}'
        ib_1 = loc
        ib_2 = ib_1 + ib_sz
        ib_text = struct.unpack(ib_fmt, data[ib_1:ib_2])
        ib_text = b''.join(ib_text).decode('utf-8')
        loc = ib_2

        checksum(data=data, dg_sz=km_header.size, chk_st=loc)
        # noinspection PyArgumentList
        return cls(header=km_header, info=i_info, bist_text=ib_text)


# ## IBR ##
@attr.s(auto_attribs=True)
class IBR:
    """ BIST Reply Parent Structure"""
    desc = "Built in test (BIST) reply."

    def __attrs_post_init__(self):
        raise NotImplemented("IBR is not implemented yet")


# ## IBS ##
@attr.s(auto_attribs=True)
class IBS:
    """ BIST Short Reply Parent Structure """
    desc = "Built in test (BIST) short reply."

    def __attrs_pos_init__(self):
        raise NotImplemented("IBS is not implemented yet")


# ### Multibeam Datagrams ###
# ## Multibeam Common Structs ##
@attr.s(auto_attribs=True)
class MPartition:
    """ M Partition, a child structure of MRZ and MWC """
    fmt: ClassVar[str] = '<2H'

    number_of_datagrams: int
    datagram_number: int

    @classmethod
    def parse(cls, data: bytes, loc: int):
        p_sz = struct.calcsize(cls.fmt)
        p_st = loc
        p_end = loc + p_sz
        p_struct = struct.unpack(cls.fmt, data[p_st:p_end])
        loc += p_sz
        # noinspection PyArgumentList
        return cls(number_of_datagrams=p_struct[0],
                   datagram_number=p_struct[1]), loc

    @classmethod
    def skip(cls, file_obj: BinaryIO):
        _skip_simple(file_obj, fmt=cls.fmt)


@attr.s(auto_attribs=True)
class MBody:
    """ M Body, a child Structure of MRZ and MWC """
    fmt: ClassVar[str] = '<2H8B'

    # size_info: int
    ping_count: int
    num_rx_fans: int
    rx_fan_index: int
    num_swaths: int
    along_position: int
    tx_id: int
    rx_id: int
    num_rx_transducers: int
    algorithm: int

    @classmethod
    def parse(cls, data: bytes, loc: int):
        m_sz = struct.calcsize(cls.fmt)
        m_st = loc
        m_end = m_st + m_sz
        m_struct = struct.unpack(cls.fmt, data[m_st:m_end])
        loc += m_struct[0]

        # noinspection PyArgumentList
        return cls(ping_count=m_struct[1],
                   num_rx_fans=m_struct[2],
                   rx_fan_index=m_struct[3],
                   num_swaths=m_struct[4],
                   along_position=m_struct[5],
                   tx_id=m_struct[6],
                   rx_id=m_struct[7],
                   num_rx_transducers=m_struct[8],
                   algorithm=m_struct[9]), loc

    @classmethod
    def skip(cls, file_obj: BinaryIO):
        _skip_from_struct(file_obj)


# ## MRZ ##
@attr.s(auto_attribs=True)
class MRZPingInfo:
    """Ping info, an MRZ child structure"""
    fmt: ClassVar[str] = '<2Hf6BH11f2H2BHI3f2Hf2H6f4B2df'
    v1_fmt: ClassVar[str] = 'f2BH'

    dict_beam_spacing: ClassVar[dict] = {
        0: "Equidistant",
        1: "Equiangle",
        2: "High density"
    }
    dict_depth_mode: ClassVar[dict] = {
        0: "Very shallow",
        1: "Shallow",
        2: "Medium",
        3: "Deep",
        4: "Deeper",
        5: "Very deep",
        6: "Extra deep",
        7: "Extreme deep",
        100: "Very shallow (Manually set)",
        101: "Shallow (Manually set)",
        102: "Medium (Manually set)",
        103: "Deep (Manually set)",
        104: "Deeper (Manually set)",
        105: "Very deep (Manually set)",
        106: "Extra deep (Manually set)",
        107: "Extreme deep (Manually set)",
    }
    dict_detect_mode: ClassVar[dict] = {
        0: "Normal",
        1: "Waterway",
        2: "Tracking",
        3: "Minimum depth",
        100: "Normal (Simulation)",
        101: "Waterway (Simulation)",
        102: "Tracking (Simulation)",
        103: "Minimum depth (Simulation)",
    }
    dict_pulse_form: ClassVar[dict] = {
        0: "CW",
        1: "mix",
        2: "FM"
    }

    # size: int
    # padding: int
    ping_rate: float = attr.ib(metadata={MdK.UNITS: Units.HZ})
    beam_spacing: str
    depth_mode: str
    sub_depth_mode: int
    distance_between_swath: int
    detection_mode: str
    pulse_form: str
    # padding: int
    frequency_mode: float
    frequency_limit_low: float = attr.ib(metadata={MdK.UNITS: Units.HZ})
    frequency_limit_high: float = attr.ib(metadata={MdK.UNITS: Units.HZ})
    pulse_length_max: float = attr.ib(metadata={MdK.UNITS: Units.SECOND})
    pulse_length_effective: float = attr.ib(metadata={MdK.UNITS: Units.SECOND})
    bandwidth_effective: float = attr.ib(metadata={MdK.UNITS: Units.HZ})
    absorption_coeff: float = attr.ib(metadata={MdK.UNITS: Units.DBPERMETER})
    sector_edge_port: float = attr.ib(metadata={MdK.UNITS: Units.DEGREE})
    sector_edge_stbd: float = attr.ib(metadata={MdK.UNITS: Units.DEGREE})
    sector_angular_coverage_port: float = attr.ib(
        metadata={MdK.UNITS: Units.DEGREE})
    sector_angular_coverage_stbd: float = attr.ib(
        metadata={MdK.UNITS: Units.DEGREE})
    sector_metric_coverage_port: int = attr.ib(metadata={MdK.UNITS: Units.METER})
    sector_metric_coverage_stbd: int = attr.ib(metadata={MdK.UNITS: Units.METER})

    pipe_tracking: int
    tx_array_size_used: float = attr.ib(metadata={MdK.UNITS: Units.DEGREE})
    rx_array_size_used: float = attr.ib(metadata={MdK.UNITS: Units.DEGREE})
    source_level: float = attr.ib(metadata={MdK.UNITS: Units.DB})
    SL_ramp_time: int = attr.ib(metadata={MdK.UNITS: Units.PERCENT})
    # padding: int
    yaw_angle: float = attr.ib(metadata={MdK.UNITS: Units.DEGREE})

    number_tx_sectors: int
    num_bytes_tx: int

    heading: float = attr.ib(metadata={MdK.UNITS: Units.DEGREE})
    sound_speed_at_tx: float = attr.ib(
        metadata={MdK.UNITS: Units.METERSPERSECOND})
    tx_depth: float = attr.ib(metadata={MdK.UNITS: Units.METER})
    waterline: float = attr.ib(metadata={MdK.UNITS: Units.METER})
    x_kmall2all: float = attr.ib(metadata={MdK.UNITS: Units.METER})
    y_kmall2all: float = attr.ib(metadata={MdK.UNITS: Units.METER})
    latlon_info: int
    pos_sensor_status: int
    attitude_sensor_status: int
    # padding: int
    latitude: float = attr.ib(metadata={MdK.UNITS: Units.DEGREES_DD})
    longitude: float = attr.ib(metadata={MdK.UNITS: Units.DEGREES_DD})
    ellipsoid_height: float = attr.ib(metadata={MdK.UNITS: Units.METER})

    bs_corr_offset: float = attr.ib(metadata={MdK.UNITS: Units.DB},
                                    default=null_int)
    lamberts_law_active: int = attr.ib(default=null_int)
    ice_window: int = attr.ib(default=null_int)
    active_modes: int = attr.ib(default=null_int)
    mode_stabilization: dict = attr.ib(factory=dict)
    runtime_filter_1: dict = attr.ib(default=dict)
    runtime_filter_2: dict = attr.ib(default=dict)

    @classmethod
    def parse(cls, data: bytes, loc: int, version: int):
        if version >= 1:
            fmt = cls.fmt + cls.v1_fmt
        else:
            fmt = cls.fmt
        pi_sz = struct.calcsize(fmt)
        pi_st = loc
        pi_end = pi_st + pi_sz
        pi_struct = struct.unpack(fmt, data[pi_st:pi_end])
        sz_pi = pi_struct[0]
        loc += sz_pi

        # noinspection PyArgumentList
        ping_info = cls(
            ping_rate=pi_struct[2],
            beam_spacing=cls.dict_beam_spacing[pi_struct[3]],
            depth_mode=cls.dict_depth_mode[pi_struct[4]],
            sub_depth_mode=pi_struct[5],
            distance_between_swath=pi_struct[6],
            detection_mode=cls.dict_detect_mode[pi_struct[7]],
            pulse_form=cls.dict_pulse_form[pi_struct[8]],
            frequency_mode=cls._read_frequency_mode(pi_struct[10]),
            frequency_limit_low=pi_struct[11],
            frequency_limit_high=pi_struct[12],
            pulse_length_max=pi_struct[13],
            pulse_length_effective=pi_struct[14],
            bandwidth_effective=pi_struct[15],
            absorption_coeff=pi_struct[16] / 1000.0,
            sector_edge_port=pi_struct[17],
            sector_edge_stbd=pi_struct[18],
            sector_angular_coverage_port=pi_struct[19],
            sector_angular_coverage_stbd=pi_struct[20],
            sector_metric_coverage_port=pi_struct[21],
            sector_metric_coverage_stbd=pi_struct[22],
            mode_stabilization=cls._read_mode_and_stab(pi_struct[23]),
            runtime_filter_1=cls._read_filters_1(pi_struct[24]),
            runtime_filter_2=cls._read_filters_2(pi_struct[25]),
            pipe_tracking=pi_struct[26],
            tx_array_size_used=pi_struct[27],
            rx_array_size_used=pi_struct[28],
            source_level=pi_struct[29],
            SL_ramp_time=pi_struct[30],
            yaw_angle=pi_struct[32],
            number_tx_sectors=pi_struct[33],
            num_bytes_tx=pi_struct[34],
            heading=pi_struct[35],
            sound_speed_at_tx=pi_struct[36],
            tx_depth=pi_struct[37],
            waterline=pi_struct[38],
            x_kmall2all=pi_struct[39],
            y_kmall2all=pi_struct[40],
            latlon_info=pi_struct[41],
            pos_sensor_status=pi_struct[42],
            attitude_sensor_status=pi_struct[43],
            latitude=pi_struct[45],
            longitude=pi_struct[46],
            ellipsoid_height=pi_struct[47]
        )

        if version >= 1:
            ping_info.bs_corr_offset = pi_struct[48]
            ping_info.lamberts_law_active = pi_struct[49]
            ping_info.ice_window = pi_struct[50]
            ping_info.active_modes = pi_struct[51]
        return ping_info, loc

    @classmethod
    def skip(cls, file_obj: BinaryIO):
        _skip_from_struct(file_obj)

    @classmethod
    def get_tx_size_info(cls, file_obj: BinaryIO):
        """ Retrieves the tx structure info and skips to start of tx structs """
        loc = file_obj.tell()

        # Get the byte size and formats to read
        fmt_skip = cls.fmt[:21]
        fmt_sz = struct.calcsize(fmt_skip)

        fmt_tx = '<' + cls.fmt[21:23]
        fmt_tx_sz = struct.calcsize(fmt_tx)

        # Read tx information and skip to end of ping info
        file_obj.seek(fmt_sz, 1)
        num_tx_sectors, num_bytes_tx = struct.unpack(fmt_tx, file_obj.read(fmt_tx_sz))
        file_obj.seek(loc, 0)
        cls.skip(file_obj)  # My gut feeling is this is not okay to do...
        return num_tx_sectors, num_bytes_tx

    @staticmethod
    def _read_frequency_mode(val: float) -> float:
        if val >= 100.:
            return val
        else:
            msg = f"value {val} is center frequency of frequency "
            f"range valid for EM712. See Kmall Specification "
            f"for more information"

            if val == 4.:
                val = 40000.
            elif val == 3:
                val = 50000.
            elif val == 2.:
                val = 85000.
            elif val == 1.:
                val = 75000.
            elif val == 0.:
                val = 70000.
            else:
                val_msg = val
                # msg = f"Frequency Mode is {val_msg}, not used."
                val = null_float
            # logger.warning(msg=msg)
            return val

    @staticmethod
    def _read_mode_and_stab(val: int) -> dict:
        """ Parses the mode and stabalization settings"""
        c_keys = ['pitch', 'yaw', 'sonar mode', 'angular coverage mode',
                  'sector mode', 'swath along position']
        num_entries = len(c_keys)
        content = dict()

        flags = get_flags(val, num_entries=num_entries)
        for index, flag in enumerate(flags):
            if index == num_entries - 1:
                if flag is True:
                    value = 'dynamic'
                else:
                    value = 'fixed'
                content[c_keys[index]] = value
            else:
                content[c_keys[index]] = flag

        return content

    @staticmethod
    def _read_filters_1(val: int) -> dict:
        """ Parses the first set of runtime filter settings"""
        c_keys = ['slope', 'aeration', 'sector', 'interference',
                  'special amplitude detect']
        content = dict()
        # TODO: Inconsistent bit indexing. Here bits start at 1, in SPO they
        #  start at 0

        flags = get_flags(val, len(c_keys))
        for index, flag in enumerate(flags):
            content[c_keys[index]] = flag
        return content

    @staticmethod
    def _read_filters_2(val: int) -> dict:
        """ Parses the second set of runtime filter settings"""
        c_keys = ['range gate', 'spike filter', 'penetration filter',
                  'phase ramp']
        content = dict()
        flags = get_flags(val, rt_int=True)
        flags.reverse()
        rng_flag = int("".join(str(flag) for flag in flags[12:16]), 2)
        spike_flag = int("".join(str(flag) for flag in flags[8:12]), 2)
        pnt_flag = int("".join(str(flag) for flag in flags[4:8]), 2)
        phs_flag = int("".join(str(flag) for flag in flags[0:4]), 2)

        if rng_flag == 0:
            content[c_keys[0]] = 'small'
        elif rng_flag == 1:
            content[c_keys[0]] = 'normal'
        else:
            content[c_keys[0]] = 'large'

        if spike_flag == 0:
            content[c_keys[1]] = 'off'
        elif spike_flag == 1:
            content[c_keys[1]] = 'weak'
        elif spike_flag == 2:
            content[c_keys[1]] = 'medium'
        else:
            content[c_keys[1]] = 'strong'

        if pnt_flag == 0:
            content[c_keys[2]] = 'off'
        elif pnt_flag == 1:
            content[c_keys[2]] = 'weak'
        elif pnt_flag == 2:
            content[c_keys[2]] = 'medium'
        else:
            content[c_keys[2]] = 'strong'

        if phs_flag == 0:
            content[c_keys[3]] = 'short'
        elif phs_flag == 1:
            content[c_keys[3]] = 'normal'
        else:
            content[c_keys[3]] = 'long'

        return content


@attr.s(auto_attribs=True)
class MRZTxSectorInfo:
    """ Transmit Sector Info, am MRZ child structure"""
    fmt: ClassVar[str] = '<4B7f2BH'
    v1_fmt: ClassVar[str] = '3f'

    dict_sector_pulse_form: ClassVar[dict] = {
        0: "CW",
        1: "FM Up",
        2: "FM Down"
    }

    sector_index: List[int] = attr.ib(factory=list)
    array_index: List[int] = attr.ib(factory=list)
    sub_array: List[int] = attr.ib(factory=list)
    # padding: uint8
    delay: List[float] = attr.ib(metadata={MdK.UNITS: Units},
                                 factory=list)
    tilt_angle: List[float] = attr.ib(metadata={MdK.UNITS: Units.DEGREE},
                                      factory=list)
    nominal_source_level: List[float] = attr.ib(metadata={MdK.UNITS: Units.DB},
                                                factory=list)
    focus_range: List[float] = attr.ib(metadata={MdK.UNITS: Units.METER},
                                       factory=list)
    center_frequency: List[float] = attr.ib(metadata={MdK.UNITS: Units.HZ},
                                            factory=list)
    bandwidth: List[float] = attr.ib(metadata={MdK.UNITS: Units.HZ},
                                     factory=list)
    pulse_length_total: List[float] = attr.ib(metadata={MdK.UNITS: Units.SECOND},
                                              factory=list)
    pulse_shading: List[int] = attr.ib(metadata={MdK.UNITS: Units.PERCENT},
                                       factory=list)
    waveform: List[str] = attr.ib(factory=list)
    # padding: uint16

    voltage_level: List[float] = attr.ib(metadata={MdK.UNITS: Units.DB},
                                         factory=list)
    tracking_correction: List[float] = attr.ib(metadata={MdK.UNITS: Units.DB},
                                               factory=list)
    pulse_length_effective: List[float] = attr.ib(
        metadata={MdK.UNITS: Units.SECOND},
        factory=list)

    @classmethod
    def parse(cls, data: bytes, loc: int, version: int,
              tx_size_ping_info: int, num_tx_records: int):
        if version >= 1:
            fmt = cls.fmt + cls.v1_fmt
        else:
            fmt = cls.fmt

        tx_sz = struct.calcsize(fmt)
        tx_1 = loc

        tx_sector_info = cls()
        for ii in range(num_tx_records):
            tx_2 = tx_1 + tx_sz
            txi_struct = struct.unpack(fmt, data[tx_1:tx_2])

            tx_sector_info.sector_index.append(txi_struct[0])
            tx_sector_info.array_index.append(txi_struct[1])
            tx_sector_info.sub_array.append(txi_struct[2])
            # _ = txi_struct[3]
            tx_sector_info.delay.append(txi_struct[4])
            tx_sector_info.tilt_angle.append(txi_struct[5])
            tx_sector_info.nominal_source_level.append(txi_struct[6])
            tx_sector_info.focus_range.append(txi_struct[7])
            tx_sector_info.center_frequency.append(txi_struct[8])
            tx_sector_info.bandwidth.append(txi_struct[9])
            tx_sector_info.pulse_length_total.append(txi_struct[10])
            tx_sector_info.pulse_shading.append(txi_struct[11])
            tx_sector_info.waveform.append(
                cls.dict_sector_pulse_form[txi_struct[12]])
            # _ = txi_struct[13]

            if version >= 1:
                tx_sector_info.voltage_level.append(txi_struct[14])
                tx_sector_info.tracking_correction.append(txi_struct[15])
                tx_sector_info.pulse_length_effective.append(txi_struct[16])

            tx_1 = tx_1 + tx_size_ping_info

        loc += tx_size_ping_info * num_tx_records
        return tx_sector_info, loc


@attr.s(auto_attribs=True)
class MRZRXInfo:
    """ Receiver info, an MRZ child structure"""
    fmt: ClassVar[str] = '<4H4f4H'

    # size: int
    max_number_soundings: int
    valid_number_soundings: int
    num_bytes_sounding: int
    sample_rate_wc: float = attr.ib(metadata={MdK.UNITS: 'Hz'})
    sample_rate_sb: float = attr.ib(metadata={MdK.UNITS: 'Hz'})
    bs_normal: float = attr.ib(metadata={MdK.UNITS: 'dB'})
    bs_oblique: float = attr.ib(metadata={MdK.UNITS: 'dB'})
    extra_detect_flags: int
    number_extra_detects: int
    number_extra_detect_classes: int
    num_bytes_ed_record: int

    @classmethod
    def parse(cls, data: bytes, loc: int):
        rx_sz = struct.calcsize(cls.fmt)
        rx_st = loc
        rx_end = rx_st + rx_sz
        rxi_struct = struct.unpack(cls.fmt, data[rx_st:rx_end])
        num_bytes = rxi_struct[0]
        loc += num_bytes

        # noinspection PyArgumentList
        return cls(max_number_soundings=rxi_struct[1],
                   valid_number_soundings=rxi_struct[2],
                   num_bytes_sounding=rxi_struct[3],
                   sample_rate_wc=rxi_struct[4],
                   sample_rate_sb=rxi_struct[5],
                   bs_normal=rxi_struct[6],
                   bs_oblique=rxi_struct[7],
                   extra_detect_flags=rxi_struct[8],
                   number_extra_detects=rxi_struct[9],
                   number_extra_detect_classes=rxi_struct[10],
                   num_bytes_ed_record=rxi_struct[11]), loc

    @classmethod
    def skip(cls, file_obj: BinaryIO):
        _skip_from_struct(file_obj)

    @classmethod
    def get_max_beams(cls, file_obj: BinaryIO):
        loc = file_obj.tell()
        fmt_rx = '<2h'
        fmt_sz = struct.calcsize(fmt_rx)
        _, max_beams = struct.unpack(fmt_rx, file_obj.read(fmt_sz))
        file_obj.seek(loc, 0)
        return max_beams


@attr.s(auto_attribs=True)
class MRZExtraDetects:
    """ Extra detects class info, customized child structure of MRZ"""
    fmt: ClassVar[str] = '<H2B'

    number_detections_in_class: List[int] = attr.ib(factory=list)
    # padding: int8
    alarm_flag: List[int] = attr.ib(factory=list)

    @classmethod
    def parse(cls, data: bytes, loc: int, num_edc: int, num_bytes_edc: int):
        edc = cls()
        edc_sz = struct.calcsize(cls.fmt)
        edc_1 = loc
        for ii in range(num_edc):
            edc_2 = edc_1 + edc_sz
            edc_struct = struct.unpack(cls.fmt, data[edc_1:edc_2])

            edc.number_detections_in_class.append(edc_struct[0])
            # _ = edc_struct[1]
            edc.alarm_flag.append(edc_struct[2])
            edc_1 = edc_1 + num_bytes_edc

        loc += (num_edc * num_bytes_edc)
        return edc, loc


@attr.s(auto_attribs=True)
class MRZDetectionInfo:
    """ Detection info, custom child structure in MRZ"""
    fmt: ClassVar[str] = 'HB7BH6f'

    dict_sounding_detect_type: ClassVar[dict] = {
        0: "normal",
        1: "extra detection",
        2: "rejected detection"
    }

    dict_sounding_detect_method: ClassVar[dict] = {
        0: "no valid detection",
        1: "amplitude detection",
        2: "phase detection"
    }

    index: List[int] = attr.ib(factory=list)
    sector_index: List[int] = attr.ib(factory=list)
    type: List[str] = attr.ib(factory=list)
    method: List[str] = attr.ib(factory=list)
    reject_info_1: List[int] = attr.ib(factory=list)
    reject_info_2: List[int] = attr.ib(factory=list)
    post_processing: List[int] = attr.ib(factory=list)
    detect_class: List[int] = attr.ib(factory=list)
    confidence: List[int] = attr.ib(factory=list)
    range_factor: List[float] = attr.ib(metadata={MdK.UNITS: Units.PERCENT},
                                        factory=list)
    quality_factor: List[float] = attr.ib(metadata={MdK.UNITS: Units.PERCENT},
                                          factory=list)
    uncertainty_vertical: List[float] = attr.ib(metadata={MdK.UNITS: Units.METER},
                                                factory=list)
    uncertainty_horizontal: List[float] = attr.ib(
        metadata={MdK.UNITS: Units.METER},
        factory=list)
    window: List[float] = attr.ib(metadata={MdK.UNITS: Units.SECOND},
                                  factory=list)
    echo_length: List[float] = attr.ib(metadata={MdK.UNITS: Units.SECOND},
                                       factory=list)


@attr.s(auto_attribs=True)
class MRZWaterColumnParams:
    """ Water column parameters, custom child structure in MRZ"""
    fmt: ClassVar[str] = '2Hf'

    index: List[int] = attr.ib(factory=list)
    sector_index: List[int] = attr.ib(factory=list)
    beam_number: List[int] = attr.ib(factory=list)
    range_samples: List[int] = attr.ib(metadata={MdK.UNITS: Units.SAMPLE},
                                       factory=list)
    across_beam_angle: List[float] = attr.ib(metadata={MdK.UNITS: Units.DEGREE},
                                             factory=list)


@attr.s(auto_attribs=True)
class MRZReflectivity:
    """ Reflectivity data, custom child structure in MRZ"""
    fmt: ClassVar[str] = '7f'

    index: List[int] = attr.ib(factory=list)
    sector_index: List[int] = attr.ib(factory=list)
    mean_abs: List[float] = attr.ib(metadata={MdK.UNITS: Units.DBPERMETER},
                                    factory=list)
    bs_type_1: List[float] = attr.ib(metadata={MdK.UNITS: Units.DB},
                                     factory=list)
    bs_type_2: List[float] = attr.ib(metadata={MdK.UNITS: Units.DB},
                                     factory=list)
    rx_sensitivity: List[float] = attr.ib(metadata={MdK.UNITS: Units.DB},
                                          factory=list)
    source_level: List[float] = attr.ib(metadata={MdK.UNITS: Units.DB},
                                        factory=list)
    calibration: List[float] = attr.ib(metadata={MdK.UNITS: Units.DB},
                                       factory=list)
    tvg: List[float] = attr.ib(metadata={MdK.UNITS: Units.DB},
                               factory=list)


@attr.s(auto_attribs=True)
class MRZRangeAngle:
    """ Raw range and angle, custom child structure in MRZ"""
    fmt: ClassVar[str] = '4f'

    index: List[int] = attr.ib(factory=list)
    sector_index: List[int] = attr.ib(factory=list)
    beam_angle: List[float] = attr.ib(metadata={MdK.UNITS: Units.DEGREE},
                                      factory=list)
    beam_angle_correction: List[float] = attr.ib(
        metadata={MdK.UNITS: Units.DEGREE},
        factory=list)
    twtt: List[float] = attr.ib(metadata={MdK.UNITS: Units.SECOND},
                                factory=list)
    twtt_correction: List[float] = attr.ib(metadata={MdK.UNITS: Units.SECOND},
                                           factory=list)


@attr.s(auto_attribs=True)
class MRZGeoReferencedDepths:
    """ Georeferenced depths, custom child structure in MRZ"""
    fmt: ClassVar[str] = '6fH'

    index: List[int] = attr.ib(factory=list)
    sector_index: List[int] = attr.ib(factory=list)
    delta_latitude: List[float] = attr.ib(metadata={MdK.UNITS: Units.DEGREES_DD},
                                          factory=list)
    delta_longitude: List[float] = attr.ib(metadata={MdK.UNITS: Units.DEGREES_DD},
                                           factory=list)
    z: List[float] = attr.ib(metadata={MdK.UNITS: Units.METER},
                             factory=list)
    y: List[float] = attr.ib(metadata={MdK.UNITS: Units.METER},
                             factory=list)
    x: List[float] = attr.ib(metadata={MdK.UNITS: Units.METER},
                             factory=list)
    inc_angle_adjustment: List[float] = attr.ib(metadata={MdK.UNITS: Units.METER},
                                                factory=list)
    realtime_cleaning: List[float] = attr.ib(factory=list)


@attr.s(auto_attribs=True)
class MRZSeabedImage:
    """ Seabed Image/Snippet data, custom child structure in MRZ"""
    fmt_snd: ClassVar[str] = '3H'
    fmt_sbi: ClassVar[str] = 'h'

    index: List[int] = attr.ib(factory=list)
    sector_index: List[int] = attr.ib(factory=list)
    starting_sample: List[int] = attr.ib(metadata={MdK.UNITS: Units.SAMPLE},
                                         factory=list)
    center_sample: List[int] = attr.ib(metadata={MdK.UNITS: Units.SAMPLE},
                                       factory=list)
    number_samples: List[int] = attr.ib(factory=list)
    snippets: NDArray = attr.ib(metadata={MdK.UNITS: Units.DB},
                                default=np.empty(shape=(1, 1)))

    @property
    def number_of_snippets_in_ping(self):
        return sum(self.number_samples)


@attr.s(auto_attribs=True)
class MRZ:
    """ Parent datagram class, contains ping related data"""
    # snd_fmt: ClassVar[str] = '<HB7BH6f2Hf7f4f6fH3H'

    desc = "Multibeam (M) raw range (R) and depth (Z)"
    header: Kmall
    partition: MPartition
    mb_body: MBody
    ping_info: MRZPingInfo
    tx_sector_info: MRZTxSectorInfo
    rx_info: MRZRXInfo
    extra_detect_class: MRZExtraDetects
    detection_info: MRZDetectionInfo
    water_column_params: MRZWaterColumnParams
    reflectivity: MRZReflectivity
    raw_range_angle: MRZRangeAngle
    geo_referenced_depths: MRZGeoReferencedDepths
    seabed_imagery: MRZSeabedImage

    @classmethod
    def parse(cls, data: bytes, loc: int = 0):
        km_header, loc = Kmall.parse(data=data, loc=loc)
        part, loc = MPartition.parse(data=data, loc=loc)
        mbody, loc = MBody.parse(data=data, loc=loc)
        ping_info, loc = MRZPingInfo.parse(data=data, loc=loc,
                                           version=km_header.version)
        tx_info, loc = MRZTxSectorInfo.parse(data=data, loc=loc,
                                             version=km_header.version,
                                             num_tx_records=ping_info.number_tx_sectors,
                                             tx_size_ping_info=ping_info.num_bytes_tx)
        rx_info, loc = MRZRXInfo.parse(data=data, loc=loc)
        extra_detect_class, loc = MRZExtraDetects.parse(data=data, loc=loc,
                                                        num_edc=rx_info.number_extra_detect_classes,
                                                        num_bytes_edc=rx_info.num_bytes_ed_record)

        # Create empty substructures to take sounding data
        detection_info = MRZDetectionInfo()
        water_column_params = MRZWaterColumnParams()
        reflectivity = MRZReflectivity()
        range_angle = MRZRangeAngle()
        geo_referenced_depths = MRZGeoReferencedDepths()
        seabed_imagery = MRZSeabedImage()

        # Parse Sounding structure
        snd_fmt = '<' + detection_info.fmt + water_column_params.fmt + \
                  reflectivity.fmt + range_angle.fmt + \
                  geo_referenced_depths.fmt + seabed_imagery.fmt_snd

        snd_sz = struct.calcsize(snd_fmt)
        num_bytes_snd = rx_info.num_bytes_sounding
        num_snd = rx_info.max_number_soundings + rx_info.number_extra_detects
        snd_1 = loc
        for ii in range(num_snd):
            snd_2 = snd_1 + snd_sz
            snd_struct = struct.unpack(snd_fmt, data[snd_1:snd_2])
            detection_info.index.append(snd_struct[0])
            detection_info.sector_index.append(snd_struct[1])
            detection_info.type.append(
                detection_info.dict_sounding_detect_type[snd_struct[2]])
            detection_info.method.append(
                detection_info.dict_sounding_detect_method[snd_struct[3]])
            detection_info.reject_info_1.append(snd_struct[4])
            detection_info.reject_info_2.append(snd_struct[5])
            detection_info.post_processing.append(snd_struct[6])
            detection_info.detect_class.append(snd_struct[7])
            detection_info.confidence.append(snd_struct[8])
            # _ = snd_struct[9]
            detection_info.range_factor.append(snd_struct[10])
            detection_info.quality_factor.append(snd_struct[11])
            detection_info.uncertainty_vertical.append(snd_struct[12])
            detection_info.uncertainty_horizontal.append(snd_struct[13])
            detection_info.window.append(snd_struct[14])
            detection_info.echo_length.append(snd_struct[15])

            water_column_params.beam_number.append(snd_struct[16])
            water_column_params.range_samples.append(snd_struct[17])
            water_column_params.across_beam_angle.append(snd_struct[18])

            reflectivity.mean_abs.append(snd_struct[19] / 1000.0)
            reflectivity.bs_type_1.append(snd_struct[20])
            reflectivity.bs_type_2.append(snd_struct[21])
            reflectivity.rx_sensitivity.append(snd_struct[22])
            reflectivity.source_level.append(snd_struct[23])
            reflectivity.calibration.append(snd_struct[24])
            reflectivity.tvg.append(snd_struct[25])

            range_angle.beam_angle.append(snd_struct[26])
            range_angle.beam_angle_correction.append(snd_struct[27])
            range_angle.twtt.append(snd_struct[28])
            range_angle.twtt_correction.append(snd_struct[29])

            geo_referenced_depths.delta_latitude.append(snd_struct[30])
            geo_referenced_depths.delta_longitude.append(snd_struct[31])
            geo_referenced_depths.z.append(snd_struct[32])
            geo_referenced_depths.y.append(snd_struct[33])
            geo_referenced_depths.x.append(snd_struct[34])
            geo_referenced_depths.inc_angle_adjustment.append(
                snd_struct[35])
            geo_referenced_depths.realtime_cleaning.append(snd_struct[36])

            seabed_imagery.starting_sample.append(snd_struct[37])
            seabed_imagery.center_sample.append(snd_struct[38])
            seabed_imagery.number_samples.append(snd_struct[39])

            snd_1 = snd_1 + num_bytes_snd

        loc += (num_snd * num_bytes_snd)

        # Pass copies of beam and sector index to make structures independent
        water_column_params.index = detection_info.index.copy()
        water_column_params.sector_index = detection_info.sector_index.copy()
        reflectivity.index = detection_info.index.copy()
        reflectivity.sector_index = detection_info.sector_index.copy()
        range_angle.index = detection_info.index.copy()
        range_angle.sector_index = detection_info.sector_index.copy()
        geo_referenced_depths.index = detection_info.index.copy()
        geo_referenced_depths.sector_index = detection_info.sector_index.copy()
        seabed_imagery.index = detection_info.index.copy()
        seabed_imagery.sector_index = detection_info.sector_index.copy()

        # Parse the snippets
        sbi_num_snippets = seabed_imagery.number_of_snippets_in_ping
        sbi_fmt = '<' + f'{sbi_num_snippets}' + seabed_imagery.fmt_sbi
        sbi_sz = struct.calcsize(sbi_fmt)
        sbi_1 = loc
        sbi_2 = sbi_1 + sbi_sz
        sbi_struct = struct.unpack(sbi_fmt, data[sbi_1:sbi_2])

        seabed_imagery.snippets = np.empty(shape=(rx_info.max_number_soundings,
                                           max(seabed_imagery.number_samples)))
        seabed_imagery.snippets.fill(np.nan)
        sb_index_1 = 0
        # TODO: I don't think this can handle extra detections
        for ii in range(len(seabed_imagery.number_samples)):
            beam_index = seabed_imagery.index[ii]
            num_smp = seabed_imagery.number_samples[ii]
            sb_index_2 = sb_index_1 + num_smp
            beam_samps = sbi_struct[sb_index_1:sb_index_2]
            seabed_imagery.snippets[beam_index, 0:num_smp] = np.asarray(beam_samps) / 10.0
            sb_index_1 = sb_index_2
        loc += sbi_sz
        checksum(data=data, dg_sz=km_header.size, chk_st=loc)

        # noinspection PyArgumentList
        return cls(header=km_header, partition=part, mb_body=mbody,
                   ping_info=ping_info, tx_sector_info=tx_info,
                   rx_info=rx_info, extra_detect_class=extra_detect_class,
                   detection_info=detection_info,
                   water_column_params=water_column_params,
                   reflectivity=reflectivity, raw_range_angle=range_angle,
                   geo_referenced_depths=geo_referenced_depths,
                   seabed_imagery=seabed_imagery)

    @classmethod
    def skip(cls, file_obj: BinaryIO):
        sz = struct.unpack('<I', file_obj.read(4))[0]
        file_obj.seek(sz - 4, 1)

    @classmethod
    def stats(cls, file_object: BinaryIO):
        """ Extracts some useful data about the ping

        Assumes we are at the start of a MRZ datagram. Typically, used in
        map_file function. will move file pointer to end of datagram
        """
        loc = file_object.tell()
        # Skip Header
        Kmall.skip(file_obj=file_object)

        # Read Partition
        m_sz = struct.calcsize(MPartition.fmt)
        mrz_prt, _ = MPartition.parse(data=file_object.read(m_sz), loc=0)
        if mrz_prt.datagram_number == 1:
            start_dg = True
        else:
            start_dg = False

        # Skip MBody
        MBody.skip(file_obj=file_object)

        # Get tx struct size from PingInfo
        num_tx_sectors, num_bytes_tx = MRZPingInfo.get_tx_size_info(file_obj=file_object)

        # Skip TxSector
        file_object.seek(num_tx_sectors * num_bytes_tx, 1)

        # Read Num beams
        max_beams = MRZRXInfo.get_max_beams(file_obj=file_object)
        file_object.seek(loc, 0)
        MRZ.skip(file_obj=file_object)
        return start_dg, mrz_prt.number_of_datagrams, num_tx_sectors, max_beams


# ## MWC ##
@attr.s(auto_attribs=True)
class MWCTxInfo:
    """ Transmit sector info, customized child structure of MWC"""
    # TX Info and Sector Info combined
    fmt_txi: ClassVar[str] = '<3Hhf'
    fmt_sct: ClassVar[str] = '<3fHh'

    # size: int
    number_tx_sectors: int
    # num_bytes_sector: int
    # padding: int16
    heave: float = attr.ib(metadata={MdK.UNITS: Units.METER})

    tilt_angle: List[float] = attr.ib(metadata={MdK.UNITS: Units.DEGREE},
                                      factory=list)
    center_frequency: List[float] = attr.ib(metadata={MdK.UNITS: Units.HZ},
                                            factory=list)
    beamwidth_along: List[float] = attr.ib(metadata={MdK.UNITS: Units.DEGREE},
                                           factory=list)
    sector_number: List[int] = attr.ib(factory=list)

    @classmethod
    def parse(cls, data: bytes, loc: int):
        """ Parses both the TX Info struct and the Sector struct"""
        txi_sz = struct.calcsize(cls.fmt_txi)
        txi_1 = loc
        txi_2 = txi_1 + txi_sz
        txi_struct = struct.unpack(cls.fmt_txi, data[txi_1:txi_2])
        num_bytes_txi = txi_struct[0]
        num_bytes_sector = txi_struct[2]

        # noinspection PyArgumentList
        tx_info = cls(number_tx_sectors=txi_struct[1], heave=txi_struct[4])
        loc += num_bytes_txi

        # Parse sectors
        sct_sz = struct.calcsize(cls.fmt_sct)
        sct_1 = loc
        for ii in range(tx_info.number_tx_sectors):
            sct_2 = sct_1 + sct_sz
            sct_struct = struct.unpack(cls.fmt_sct, data[sct_1:sct_2])
            tx_info.tilt_angle.append(sct_struct[0])
            tx_info.center_frequency.append(sct_struct[1])
            tx_info.beamwidth_along.append(sct_struct[2])
            tx_info.sector_number.append(sct_struct[3])
            # _ = sct_struct[4]

            sct_1 = sct_1 + num_bytes_sector

        loc += (num_bytes_sector * tx_info.number_tx_sectors)
        return tx_info, loc


@attr.s(auto_attribs=True)
class MWCRXInfo:
    """ Receiver info, child structure of MWC"""
    fmt: ClassVar[str] = '<2H3Bb2f'

    # size: uint16
    number_beams: int
    num_bytes_beam: int
    phase_flag: int
    tvg_function_applied: int
    tvg_offset: int = attr.ib(metadata={MdK.UNITS: Units.DB})
    sample_rate: float = attr.ib(metadata={MdK.UNITS: Units.HZ})
    sound_velocity: float = attr.ib(metadata={MdK.UNITS: Units.METERSPERSECOND})

    @classmethod
    def parse(cls, data: bytes, loc):
        rx_sz = struct.calcsize(cls.fmt)
        rx_1 = loc
        rx_2 = rx_1 + rx_sz
        rxi_struct = struct.unpack(cls.fmt, data[rx_1:rx_2])
        rxi_num_bytes = rxi_struct[0]
        loc += rxi_num_bytes

        # noinspection PyArgumentList
        return cls(
            number_beams=rxi_struct[1],
            num_bytes_beam=rxi_struct[2],
            phase_flag=rxi_struct[3],
            tvg_function_applied=rxi_struct[4],
            tvg_offset=rxi_struct[5],
            sample_rate=rxi_struct[6],
            sound_velocity=rxi_struct[7]), loc


@attr.s(auto_attribs=True)
class MWCData:
    """ Water column data, child structure of MWC"""
    fmt: ClassVar[str] = '<f4Hf'

    beam_pointing_angle: List[float] = attr.ib(metadata={MdK.UNITS: Units.DEGREE},
                                               factory=list)
    starting_sample: List[int] = attr.ib(metadata={MdK.UNITS: Units.SAMPLE},
                                         factory=list)
    detection_sample: List[int] = attr.ib(metadata={MdK.UNITS: Units.SAMPLE},
                                          factory=list)
    tx_sector_number: List[int] = attr.ib(factory=list)
    number_of_samples: List[int] = attr.ib(factory=list)
    high_res_detect_sample: List[float] = attr.ib(
        metadata={MdK.UNITS: Units.SAMPLE},
        factory=list)
    samples: NDArray = attr.ib(metadata={MdK.UNITS: Units.DB},
                               factory=list)

    @classmethod
    def parse(cls, data: bytes, loc: int, num_beams: int, num_bytes_beam: int,
              phase_flag: int):

        # Determine struct fmt
        wcd_sz = struct.calcsize(cls.fmt)
        if phase_flag == 0:
            blk_fmts = f'<&b'
        elif phase_flag == 1:
            blk_fmts = f'<&b&b'
        elif phase_flag == 2:
            blk_fmts = f'<&b&h'
        else:
            raise ValueError(f"Phase flag must be 0/1/2. Phase flag is: "
                             f"{phase_flag}")

        wcd_1 = loc
        wcd_data = cls()
        max_samps = -1000
        samples = list()
        for ii in range(num_beams):
            wcd_2 = wcd_1 + wcd_sz
            wcd_struct = struct.unpack(cls.fmt, data[wcd_1:wcd_2])

            wcd_data.beam_pointing_angle.append(wcd_struct[0])
            wcd_data.starting_sample.append(wcd_struct[1])
            wcd_data.detection_sample.append(wcd_struct[2])
            wcd_data.tx_sector_number.append(wcd_struct[3])
            wcd_num_samps_beam = wcd_struct[4]
            max_samps = np.maximum(max_samps, wcd_num_samps_beam)
            wcd_data.number_of_samples.append(wcd_num_samps_beam)
            wcd_data.high_res_detect_sample.append(wcd_struct[5])

            blk_fmt = blk_fmts.replace('&', str(wcd_num_samps_beam))
            blk_sz = struct.calcsize(blk_fmt)
            blk_1 = wcd_1 + num_bytes_beam
            blk_2 = blk_1 + blk_sz
            blk_struct = struct.unpack(blk_fmt, data[blk_1:blk_2])
            samples.append(np.asarray(blk_struct) / 2.0)
            # TODO: ^^^^ Verify this is correct behavior

            wcd_1 = wcd_1 + num_bytes_beam + blk_sz

        # Convert WCD sample list to numpy array
        wcd_data.samples = np.empty(shape=(max_samps, num_beams))
        wcd_data.samples.fill(np.nan)
        for beam, samps in enumerate(samples):
            wcd_data.samples[:wcd_data.number_of_samples[beam], beam] = samps[np.newaxis]

        # Calc new loc
        wcd_off = (num_bytes_beam * num_beams)
        blk_off = sum(wcd_data.number_of_samples) * struct.calcsize(
            blk_fmts.replace('&', str(1)))
        loc += wcd_off + blk_off
        return wcd_data, loc


@attr.s(auto_attribs=True)
class MWC:
    """ Water column parent datagram class"""
    desc = "Multibeam (M) water (W) column (C)"
    header: Kmall
    partition: MPartition
    mb_body: MBody
    tx_info: MWCTxInfo
    rx_info: MWCRXInfo
    wcd_data: MWCData

    @classmethod
    def parse(cls, data: bytes, loc: int = 0):
        km_header, loc = Kmall.parse(data=data, loc=loc)
        part, loc = MPartition.parse(data=data, loc=loc)
        mbody, loc = MBody.parse(data=data, loc=loc)
        tx_info, loc = MWCTxInfo.parse(data=data, loc=loc)
        rx_info, loc = MWCRXInfo.parse(data=data, loc=loc)
        wcd_data, loc = MWCData.parse(data=data, loc=loc,
                                      num_beams=rx_info.number_beams,
                                      num_bytes_beam=rx_info.num_bytes_beam,
                                      phase_flag=rx_info.phase_flag)
        checksum(data=data, dg_sz=km_header.size, chk_st=loc)

        # noinspection PyArgumentList
        return cls(header=km_header, partition=part, mb_body=mbody,
                   tx_info=tx_info, rx_info=rx_info, wcd_data=wcd_data)

    @classmethod
    def skip(cls, file_obj: BinaryIO):
        _skip_from_datagram(file_obj)

    @classmethod
    def stats(cls, file_object: BinaryIO):
        """ Extracts some useful data about the ping

                Assumes we are at the start of a MWC datagram. Typically, used in
                map_file function. will move file pointer to end of datagram
                """

        loc = file_object.tell()
        # Skip Header
        Kmall.skip(file_obj=file_object)

        # Read Partition
        m_sz = struct.calcsize(MPartition.fmt)
        mrz_prt, _ = MPartition.parse(data=file_object.read(m_sz), loc=0)
        if mrz_prt.datagram_number == 1:
            start_dg = True
        else:
            start_dg = False

        # Skip MBody
        MBody.skip(file_obj=file_object)

        # Get tx struct size from PingInfo
        fmt = MWCTxInfo.fmt_txi
        tx_info = struct.unpack(fmt, file_object.read(struct.calcsize(fmt)))
        num_bytes_tx = tx_info[0]
        num_tx_sectors = tx_info[1]
        num_bytes_tx_sector = tx_info[2]

        # Skip TxSector
        file_object.seek(num_tx_sectors * num_bytes_tx, 1)

        # Read Num beams
        fmt = MWCRXInfo.fmt[:3]
        rx_info = struct.unpack(fmt, file_object.read(struct.calcsize(fmt)))
        max_beams = rx_info[1]
        file_object.seek(loc, 0)
        cls.skip(file_obj=file_object)
        return start_dg, mrz_prt.number_of_datagrams, num_tx_sectors, max_beams


# ### External Sensor Output Datagrams ###
# ## Sensor Common Structs ##
@attr.s(auto_attribs=True)
class SInfo:
    """ Sensor information, child structure to the Sensor class datagrams """
    fmt: ClassVar[str] = '<4H'

    # size_info: int
    system: int
    status: dict = attr.ib(factory=dict)

    # padding: bytes

    @classmethod
    def parse(cls, data: bytes, loc: int):
        sni_sz = struct.calcsize(cls.fmt)
        sni_st = loc
        sni_end = sni_st + sni_sz
        sni_struct = struct.unpack(cls.fmt, data[sni_st:sni_end])
        sz_sni = sni_struct[0]
        status = cls.read_status(sni_struct[2])
        loc += sz_sni
        # noinspection PyArgumentList
        return cls(system=sni_struct[1],
                   status=status), loc

    @staticmethod
    def read_status(val: int) -> dict:
        # Set up base dictionary
        sensor_status = dict()
        sensor_status['sensor_active'] = True
        sensor_status['data_valid_1'] = "Data OK"
        sensor_status['data_valid_2'] = "Data OK"
        sensor_status['velocity_source'] = "Sensor"

        # These values only valid for SPO and CPO
        sensor_status['time_source'] = "PU"
        sensor_status['motion_corrected'] = False
        sensor_status['quality_check'] = "Normal"

        flags = get_flags(val)

        if flags[0] == 0:
            sensor_status['sensor_active'] = False
        if flags[2] == 1:
            sensor_status['data_valid_1'] = "Reduced Performance"
        if flags[4] == 1:
            sensor_status['data_valid_2'] = "Invalid data"
        if flags[6] == 1:
            sensor_status['velocity_source'] = 'PU'

        # These values only valid for SPO and CPO
        if flags[9] == 1:
            sensor_status['time_source'] = 'Datagram'
        if flags[10] == 1:
            sensor_status['motion_corrected'] = True
        if flags[11] == 1:
            sensor_status['quality_check'] = 'Operator'

        return sensor_status


# ## SPO ##
@attr.s(auto_attribs=True)
class SPOData:
    """ Postion data, child structure of SPO"""
    fmt: ClassVar[str] = '<2If2d3f'

    date_time: datetime
    pos_fix_quality: float = attr.ib(metadata={MdK.UNITS: Units.METER})
    latitude_corrected: float = attr.ib(metadata={MdK.UNITS: Units.DEGREES_DD})
    longitude_corrected: float = attr.ib(metadata={MdK.UNITS: Units.DEGREES_DD})
    sog: float = attr.ib(metadata={MdK.UNITS: Units.METERSPERSECOND})
    cog: float = attr.ib(metadata={MdK.UNITS: Units.DEGREE})
    ellipsoid_height: float = attr.ib(metadata={MdK.UNITS: Units.METER})
    raw_position: Optional[GGA] = attr.ib(default=None)

    @classmethod
    def parse(cls, data: bytes, loc: int, size: int):
        spo_sz = struct.calcsize(cls.fmt)
        spo_st = loc
        spo_end = spo_st + spo_sz
        spo_d = struct.unpack(cls.fmt, data[spo_st:spo_end])

        # Parse the raw sensor data
        raw_st = spo_end
        raw_end = size - 4  # This assumes you only passed a single SPO dg
        raw_position = GGA.parse(str(data[raw_st:raw_end]))
        # TODO: Assumes GGA, should implement verification

        # noinspection PyArgumentList
        return cls(
            date_time=km_datetime(spo_d[0], spo_d[1]),
            pos_fix_quality=spo_d[2],
            latitude_corrected=spo_d[3],
            longitude_corrected=spo_d[4],
            sog=spo_d[5],
            cog=spo_d[6],
            ellipsoid_height=spo_d[7],
            raw_position=raw_position
        ), raw_end


@attr.s(auto_attribs=True)
class SPO:
    """ Position datagram parent class """
    desc = "Sensor (S) data for postiion (PO)"
    max_spo_size: ClassVar[int] = 250
    id_pos_unavailable: ClassVar[int] = 200

    header: Kmall
    sensor_info: SInfo
    sensor_data: SPOData

    @classmethod
    def parse(cls, data: bytes, loc: int = 0):
        km_header, loc = Kmall.parse(data=data, loc=loc)
        s_info, loc = SInfo.parse(data=data, loc=loc)
        s_data, loc = SPOData.parse(data=data, loc=loc, size=km_header.size)
        checksum(data=data, dg_sz=km_header.size, chk_st=loc)

        # noinspection PyArgumentList
        return cls(header=km_header,
                   sensor_info=s_info,
                   sensor_data=s_data)


# ## SKM ##
# TODO: Note the KMbinary substructure separates data by source (pos, att...)
#  sorta. As such we should segment the data by substructure and do away with
#  the extra data level. But the kmall grouping is not 100% intuitive, with 8
#  substructures. Here we define 4 substructures. The trade off is
#  substructures do not have a parse function. Changes to KMbinary will span
#  entire SKM datagram
@attr.s(auto_attribs=True)
class SKMSensorInfo:
    """ Binary sensor info, child strucuture to SKM"""
    fmt: ClassVar[str] = '<H2B4H'

    dict_input_fmt: ClassVar[dict] = {
        1: "KM binary sensor input",
        2: "EM3000 data",
        3: "Sagem",
        4: "Seapath binary 11",
        5: "Seapath binary 23",
        6: "Seapath binary 26",
        7: "POS M/V GRP 102/103",
        8: "Coda Octopus MCOM"
    }

    # size_info: int
    system: int
    status: dict
    input_format: str
    num_samples: int
    sample_size: int
    content: dict

    @classmethod
    def parse(cls, data: bytes, loc: int):
        snri_sz = struct.calcsize(cls.fmt)
        snri_st = loc
        snri_end = snri_st + snri_sz
        snri_struct = struct.unpack(cls.fmt, data[snri_st:snri_end])
        sz_snri = snri_struct[0]
        status = cls.read_status(snri_struct[2])
        loc += sz_snri
        # noinspection PyArgumentList
        return cls(system=snri_struct[1],
                   status=status,
                   input_format=cls.dict_input_fmt[snri_struct[3]],
                   num_samples=snri_struct[4],
                   sample_size=snri_struct[5],
                   content=cls.read_content(snri_struct[6])), loc

    @staticmethod
    def read_status(val: int) -> dict:
        # TODO: Subclassing is not encouraged for attrs. So this is the work
        #  around
        sensor_status = SInfo.read_status(val)
        for ii in range(3):
            sensor_status.popitem()

        return sensor_status

    @staticmethod
    def read_content(val: int) -> dict:
        # Set up base dictionary
        c_keys = ['horizontal position/velocity', 'roll and pitch', 'heading',
                  'heave', 'acceleration', 'delayed heave', 'delayed heave 2']
        content = dict()
        flags = get_flags(val, len(c_keys))
        for index, flag in enumerate(flags):
            content[c_keys[index]] = flag
        return content


@attr.s(auto_attribs=True)
class SKMRecordInfo:
    """ Attitude block record sensor information, custom child structure of SKM """
    dgm_type: List[bytes] = attr.ib(factory=list)
    dgm_version: List[int] = attr.ib(factory=list)
    date_time: List[datetime] = attr.ib(factory=list)
    status: List[int] = attr.ib(factory=list)


@attr.s(auto_attribs=True)
class SKMPosition:
    """ Attitude block record position, custom child structure of SKM"""
    latitude: List[float] = attr.ib(metadata={MdK.UNITS: Units.DEGREES_DD},
                                    factory=list)
    longitude: List[float] = attr.ib(metadata={MdK.UNITS: Units.DEGREES_DD},
                                     factory=list)
    ellipsoid_height: List[float] = attr.ib(
        metadata={MdK.UNITS: Units.DEGREES_DD},
        factory=list)
    error_lat: List[float] = attr.ib(metadata={MdK.UNITS: Units.METER},
                                     factory=list)
    error_lon: List[float] = attr.ib(metadata={MdK.UNITS: Units.METER},
                                     factory=list)
    error_height: List[float] = attr.ib(metadata={MdK.UNITS: Units.METER},
                                        factory=list)
    velocity_north: List[float] = attr.ib(
        metadata={MdK.UNITS: Units.METERSPERSECOND},
        factory=list)
    velocity_east: List[float] = attr.ib(
        metadata={MdK.UNITS: Units.METERSPERSECOND},
        factory=list)
    velocity_down: List[float] = attr.ib(
        metadata={MdK.UNITS: Units.METERSPERSECOND},
        factory=list)
    acceleration_north: List[float] = attr.ib(
        metadata={MdK.UNITS: Units.METERSPERSECOND2},
        factory=list)
    acceleration_east: List[float] = attr.ib(
        metadata={MdK.UNITS: Units.METERSPERSECOND2},
        factory=list)
    acceleration_down: List[float] = attr.ib(
        metadata={MdK.UNITS: Units.METERSPERSECOND2},
        factory=list)


@attr.s(auto_attribs=True)
class SKMAttitude:
    """ Attitude block record attitude, custom child structure of SKM"""
    roll: List[float] = attr.ib(metadata={MdK.UNITS: Units.DEGREE},
                                factory=list)
    pitch: List[float] = attr.ib(metadata={MdK.UNITS: Units.DEGREE},
                                 factory=list)
    heading: List[float] = attr.ib(metadata={MdK.UNITS: Units.DEGREE},
                                   factory=list)
    heave: List[float] = attr.ib(metadata={MdK.UNITS: Units.METER},
                                 factory=list)
    roll_rate: List[float] = attr.ib(metadata={MdK.UNITS: Units.DEGREESPERSECOND},
                                     factory=list)
    pitch_rate: List[float] = attr.ib(
        metadata={MdK.UNITS: Units.DEGREESPERSECOND},
        factory=list)
    yaw_rate: List[float] = attr.ib(metadata={MdK.UNITS: Units.DEGREESPERSECOND},
                                    factory=list)
    error_roll: List[float] = attr.ib(metadata={MdK.UNITS: Units.DEGREE},
                                      factory=list)
    error_pitch: List[float] = attr.ib(metadata={MdK.UNITS: Units.DEGREE},
                                       factory=list)
    error_yaw: List[float] = attr.ib(metadata={MdK.UNITS: Units.DEGREE},
                                     factory=list)
    error_heave: List[float] = attr.ib(metadata={MdK.UNITS: Units.METER},
                                       factory=list)


@attr.s(auto_attribs=True)
class SKMDelayHeave:
    """ Attitude block record delayed heave, custom child structure of SKM"""
    fmt: ClassVar[str] = '<2If'

    date_time: List[datetime] = attr.ib(factory=list)
    heave: List[int] = attr.ib(metadata={MdK.UNITS: 'meters'},
                               factory=list)


@attr.s(auto_attribs=True)
class SKM:
    """ Binary Sensor/ Attitude parent datagram class """
    bin_fmt: ClassVar[str] = '<4c2H3I2df4f3f3f7f3f'
    max_att_samples: ClassVar[int] = 148

    desc = "Sensor (S) KM binary senor format"
    header: Kmall
    sensor_info: SKMSensorInfo
    record_info: SKMRecordInfo
    position: SKMPosition
    attitude: SKMAttitude
    delayed_heave: SKMDelayHeave

    @classmethod
    def parse(cls, data: bytes, loc: int = 0):
        km_header, loc = Kmall.parse(data=data, loc=loc)
        s_info, loc = SKMSensorInfo.parse(data=data, loc=loc)

        km_bin_sz = s_info.sample_size
        num_samples = s_info.num_samples
        total_size = km_bin_sz * num_samples

        # Check if there is delayed heave
        # TODO: I am not sure I like this, assuming delayed heave is
        #  present if both entries are listed. That said, just checking the
        #  size is insufficient if they end up extending the KMbinary struct...
        tmp = s_info.content.values().__reversed__()
        dlyh1 = next(tmp)
        dlyh2 = next(tmp)
        dly_here = dlyh1 and dlyh2
        if dly_here:
            bin_fmt = cls.bin_fmt + SKMDelayHeave.fmt[1:]

        else:
            bin_fmt = cls.bin_fmt
        bin_sz = struct.calcsize(bin_fmt)

        # Loop over each km-binary record. Filling the dataclass arrays
        rec_info = SKMRecordInfo()
        pos = SKMPosition()
        att = SKMAttitude()
        dly = SKMDelayHeave()

        for ii in range(s_info.num_samples):
            loc2 = loc + bin_sz
            bin_struct = struct.unpack(bin_fmt, data[loc:loc2])

            # Record Info
            rec_info.dgm_type.append(b''.join(bin_struct[0:4]))
            rec_info.dgm_version.append(bin_struct[5])
            rec_info.date_time.append(
                km_datetime(time_sec=bin_struct[6], time_nanosec=bin_struct[7]))
            rec_info.status.append(bin_struct[8])

            # Position
            pos.latitude.append(bin_struct[9])
            pos.longitude.append(bin_struct[10])
            pos.ellipsoid_height.append(bin_struct[11])

            # Attitude
            att.roll.append(bin_struct[12])
            att.pitch.append(bin_struct[13])
            att.heading.append(bin_struct[14])
            att.heave.append(bin_struct[15])

            # Rates
            att.roll_rate.append(bin_struct[16])
            att.pitch_rate.append(bin_struct[17])
            att.yaw_rate.append(bin_struct[18])

            # Velocities
            pos.velocity_north.append(bin_struct[19])
            pos.velocity_east.append(bin_struct[20])
            pos.velocity_down.append(bin_struct[21])

            # Errors (deviations lol)
            pos.error_lat.append(bin_struct[22])
            pos.error_lon.append(bin_struct[23])
            pos.error_height.append(bin_struct[24])
            att.error_roll.append(bin_struct[25])
            att.error_pitch.append(bin_struct[26])
            att.error_yaw.append(bin_struct[27])
            att.error_heave.append(bin_struct[28])

            # Acceleration
            pos.acceleration_north.append(bin_struct[29])
            pos.acceleration_east.append(bin_struct[30])
            pos.acceleration_down.append(bin_struct[31])

            # Delayed heave
            if dly_here is True:
                dly.date_time.append(
                    km_datetime(bin_struct[32], bin_struct[33]))
                dly.heave.append(bin_struct[34])

            loc += km_bin_sz

        # Hopefully we are at the check sum
        checksum(data=data, dg_sz=km_header.size, chk_st=loc)

        # noinspection PyArgumentList
        return cls(header=km_header, sensor_info=s_info,
                   record_info=rec_info,
                   position=pos,
                   attitude=att,
                   delayed_heave=dly)


# ## SVP ##
@attr.s(auto_attribs=True)
class SVPInfo:
    """ Sound velocity profile info, a child structure to SVP"""
    fmt: ClassVar[str] = '<2H4cI2d'

    dict_sensor_format: ClassVar[dict] = {
        b'S00': "sound velocity profile",
        b'S01': "CTD profile"
    }

    # size: unint16
    number_samples: int
    format: str
    date_time: datetime
    lattitude: float = attr.ib(metadata={MdK.UNITS: Units.DEGREES_DD})
    longitude: float = attr.ib(metadata={MdK.UNITS: Units.DEGREES_DD})

    @classmethod
    def parse(cls, data: bytes, loc: int):
        sz = struct.calcsize(cls.fmt)
        si_1 = loc
        si_2 = si_1 + sz
        si_struct = struct.unpack(cls.fmt, data[si_1:si_2])
        loc += si_struct[0]
        in_fmt = cls.dict_sensor_format[b''.join(si_struct[2:5])]

        # noinspection PyArgumentList
        return cls(number_samples=si_struct[1], format=in_fmt,
                   date_time=km_datetime(time_sec=si_struct[6]),
                   lattitude=si_struct[7], longitude=si_struct[8]), loc


@attr.s(auto_attribs=True)
class SVPData:
    """ Sound velocity data, a child structure to SVP """
    fmt: ClassVar[str] = '<2fI2f'

    depth: List[float] = attr.ib(metadata={MdK.UNITS: Units.METER},
                                 factory=list)
    sound_velocity: List[float] = attr.ib(metadata={MdK.UNITS: Units.METER},
                                          factory=list)
    temperature: List[float] = attr.ib(metadata={MdK.UNITS: Units.CELSIUS},
                                       factory=list)
    salinity: List[float] = attr.ib(metadata={MdK.UNITS: Units.PSU},
                                    factory=list)


@attr.s(auto_attribs=True)
class SVP:
    """ Sound velocity profile datagram, parent class"""
    max_svp_samples: ClassVar[int] = 2000

    desc = "Sensor (S) data from sound velocity (V) profile (P)"
    header: Kmall
    info: SVPInfo
    data: SVPData

    @classmethod
    def parse(cls, data: bytes, loc: int = 0):
        km_header, loc = Kmall.parse(data=data, loc=loc)
        s_info, loc = SVPInfo.parse(data=data, loc=loc)

        # Parse profile data
        sv_1 = loc
        svp_d = SVPData()
        svp_sz = struct.calcsize(
            svp_d.fmt)  # TODO: I guess the SVP format will never grow...
        for ii in range(s_info.number_samples):
            sv_2 = sv_1 + svp_sz
            svp_struct = struct.unpack(svp_d.fmt, data[sv_1:sv_2])
            svp_d.depth.append(svp_struct[0])
            svp_d.sound_velocity.append(svp_struct[1])
            # _ = svp_struct[2]
            svp_d.temperature.append(svp_struct[3])
            svp_d.salinity.append(svp_struct[4])

            sv_1 = sv_2
        loc += s_info.number_samples * svp_sz
        checksum(data=data, dg_sz=km_header.size, chk_st=loc)

        # noinspection PyArgumentList
        return cls(header=km_header, info=s_info, data=svp_d)


# ## SVT ##
@attr.s(auto_attribs=True)
class SVTInfo:
    """ Sound velocity sensor, child class of SVT """
    fmt: ClassVar[str] = '<6H2f'

    dict_sensor_input_format: ClassVar[dict] = {
        1: "AML NMEA",
        2: "AML SV",
        3: "AML SVT",
        4: "AML SVP",
        5: "Micro SV",
        6: "Micro SVT",
        7: "Micro SVP",
        8: "Valeport MiniSVS",
        9: "KSSIS 80",
        10: "KSSIS 43"
    }

    # size: uint16
    status: dict
    input_format: str
    number_samples: int
    num_bytes_samp: int
    filter_time: float = attr.ib(metadata={MdK.UNITS: Units.SECOND})
    sound_velocity_offset: float = attr.ib(
        metadata={MdK.UNITS: Units.METERSPERSECOND})
    data_content: dict = attr.ib(factory=dict)

    @classmethod
    def parse(cls, data: bytes, loc: int):
        si_sz = struct.calcsize(cls.fmt)
        si_1 = loc
        si_2 = si_1 + si_sz
        si_struct = struct.unpack(cls.fmt, data[si_1:si_2])

        loc += si_struct[0]

        # noinspection PyArgumentList
        return cls(status=cls.read_status(si_struct[1]),
                   input_format=cls.dict_sensor_input_format[si_struct[2]],
                   number_samples=si_struct[3], num_bytes_samp=si_struct[4],
                   data_content=cls.read_content(si_struct[5]),
                   filter_time=si_struct[6],
                   sound_velocity_offset=si_struct[7]), loc

    @staticmethod
    def read_status(val: int) -> dict:
        #  TODO: Subclassing is not encouraged for attrs. So this is the work around
        sensor_status = SInfo.read_status(val)
        for ii in range(4):
            sensor_status.popitem()
        return sensor_status

    @staticmethod
    def read_content(val: int) -> dict:
        # Generate a default dictionary output
        c_keys = ['Sound Velocity', 'Temperature', 'Pressure', 'Salinity']
        content = dict()
        flags = get_flags(val, num_entries=len(c_keys))
        for index, flag in enumerate(flags):
            content[c_keys[index]] = flag
        return content


@attr.s(auto_attribs=True)
class SVTData:
    """  Sound velocity sensor data, a child structure of SVT """
    fmt: ClassVar[str] = '<2I4f'
    max_svt_samples: ClassVar[int] = 1

    date_time: List[datetime] = attr.ib(metadata={MdK.UNITS: Units.SECOND},
                                        factory=list)
    sound_velocity: List[float] = attr.ib(
        metadata={MdK.UNITS: Units.METERSPERSECOND},
        factory=list)
    temperature: List[float] = attr.ib(metadata={MdK.UNITS: Units.CELSIUS},
                                       factory=list)
    pressure: List[float] = attr.ib(metadata={MdK.UNITS: Units.PASCAL},
                                    factory=list)
    salinity: List[float] = attr.ib(metadata={MdK.UNITS: Units.PSU},
                                    factory=list)


@attr.s(auto_attribs=True)
class SVT:
    """ Sound velocity sensor datagram, parent class"""
    desc = "Sensor (S) data from sound velocity (V) at transducer (T)"

    header: Kmall
    sensor_info: SVTInfo
    sensor_data: SVTData

    @classmethod
    def parse(cls, data: bytes, loc: int = 0):
        km_header, loc = Kmall.parse(data=data, loc=loc)
        s_info, loc = SVTInfo.parse(data=data, loc=loc)

        s_data = SVTData()
        sv_1 = loc
        sv_sz = struct.calcsize(s_data.fmt)
        for ii in range(s_info.number_samples):
            sv_2 = sv_1 + sv_sz
            sv_struct = struct.unpack(s_data.fmt, data[sv_1:sv_2])
            s_data.date_time.append(km_datetime(time_sec=sv_struct[0],
                                                time_nanosec=sv_struct[1]))
            s_data.sound_velocity.append(sv_struct[2])
            s_data.temperature.append(sv_struct[3])
            s_data.pressure.append(sv_struct[4])
            s_data.salinity.append(sv_struct[5])
            sv_1 = sv_1 + s_info.num_bytes_samp
        loc += (s_info.num_bytes_samp * s_info.number_samples)
        checksum(data=data, dg_sz=km_header.size, chk_st=loc)

        # noinspection PyArgumentList
        return cls(header=km_header, sensor_info=s_info, sensor_data=s_data)


# ## SCL ##
@attr.s(auto_attribs=True)
class SCLSensorInfo:
    """ Clock sensor information, child structure to SCL """
    fmt: ClassVar[str] = '<4H'

    # TODO: This assumes that synchronisation can only come from one source.
    #  Which I think is probabaly a reasonable thing to assume
    dict_sensor_system: ClassVar[dict] = {
        0: "Time synchronisation from clock data, no 1PPS",
        2: "Time synchronisation from active position data",
        4: "Time synchronisation from clock data, 1 PPS active",
        5: "Time synchronisation from active position data, 1 PPS active"
    }

    # size: uint16
    system: str
    status: dict = attr.ib(factory=dict)

    @classmethod
    def parse(cls, data: bytes, loc: int):
        sni_sz = struct.calcsize(cls.fmt)
        sni_st = loc
        sni_end = sni_st + sni_sz
        sni_struct = struct.unpack(cls.fmt, data[sni_st:sni_end])
        sz_sni = sni_struct[0]
        status = cls.read_status(sni_struct[2])
        loc += sz_sni
        # noinspection PyArgumentList
        return cls(system=cls.dict_sensor_system[sni_struct[1]],
                   status=status), loc

    @staticmethod
    def read_status(val: int) -> dict:
        sensor_status = SInfo.read_status(val)

        if sensor_status['sensor_active'] is True:
            sensor_status['sensor_active'] = 'Valid data and 1PPS OK'
        if sensor_status['data_valid_1'] != 'Data OK':
            sensor_status['data_valid_1'] = 'No time synchronisation of PU'
        for ii in range(4):
            sensor_status.popitem()
        return sensor_status


@attr.s(auto_attribs=True)
class SCLData:
    """ Clock sensor data, child structure of SCL """
    fmt: ClassVar[str] = '<fI'
    max_scl_sz: ClassVar[int] = 64

    offset: float = attr.ib(metadata={MdK.UNITS: Units.SECOND})
    clock_deviation_to_pu: int = attr.ib(metadata={MdK.UNITS: Units.NANOSECOND})
    raw_data: str

    @classmethod
    def parse(cls, data: bytes, loc: int, dg_size: int, start_pos: int):
        sz = struct.calcsize(cls.fmt)
        c_1 = loc
        c_2 = c_1 + sz
        c_struct = struct.unpack(cls.fmt, data[c_1:c_2])
        loc += sz  # TODO: Another instance where we don't know the length of the structure

        # determine end loc
        end_loc = start_pos + dg_size
        raw = ZDA.parse(str(data[loc:end_loc - 4]))
        loc = end_loc - 4

        # noinspection PyArgumentList
        return cls(offset=c_struct[0], clock_deviation_to_pu=c_struct[1],
                   raw_data=raw), loc


@attr.s(auto_attribs=True)
class SCL:
    """ Clock sensor datagram, parent class """
    desc = "Sensor (S) data from clock (CL)"

    header: Kmall
    sensor_info: SCLSensorInfo
    sensor_data: SCLData

    @classmethod
    def parse(cls, data: bytes, loc: int = 0):
        st_loc = loc
        km_header, loc = Kmall.parse(data=data, loc=loc)
        s_info, loc = SCLSensorInfo.parse(data=data, loc=loc)
        s_data, loc = SCLData.parse(data=data, loc=loc, dg_size=km_header.size,
                                    start_pos=st_loc)

        checksum(data=data, dg_sz=km_header.size, chk_st=loc)
        # noinspection PyArgumentList
        return cls(header=km_header, sensor_info=s_info, sensor_data=s_data)


# ## SDE ##
@attr.s(auto_attribs=True)
class SDEInfo:
    """ Depth sensor information, child structure of SDE"""
    fmt: ClassVar[str] = '<4H'

    # size: uint16
    system: int
    status: dict = attr.ib(factory=dict)

    # padding: unint16

    @classmethod
    def parse(cls, data: bytes, loc: int):
        # TODO: Is it okay to instead call SInfo.parse? and then map to SDEInfo?
        # This is very repetitive code. And subclassing is not suggested,
        # and I can't seem to get suclassing to work

        sni_sz = struct.calcsize(cls.fmt)
        sni_st = loc
        sni_end = sni_st + sni_sz
        sni_struct = struct.unpack(cls.fmt, data[sni_st:sni_end])
        sz_sni = sni_struct[0]
        status = cls.read_status(sni_struct[2])
        loc += sz_sni

        # noinspection PyArgumentList
        return cls(system=sni_struct[1],
                   status=status), loc

    @staticmethod
    def read_status(val: int) -> dict:
        sensor_status = SInfo.read_status(val)
        for ii in range(3):
            sensor_status.popitem()
            return sensor_status


@attr.s(auto_attribs=True)
class SDEData:
    """ Depth sensor data, a child structure of SDE"""
    # TODO: This Datagram will definitely break between Rev I and Rev H
    fmt: ClassVar[str] = '<4f2d'
    max_datalength: ClassVar[int] = 32

    depth_used: float = attr.ib(metadata={MdK.UNITS: Units.METER})
    depth_raw: float = attr.ib(metadata={MdK.UNITS: Units.METER})  # Added in Rev I
    offset: float = attr.ib(metadata={MdK.UNITS: Units.METER})
    scale: float
    latitude: float = attr.ib(metadata={MdK.UNITS: Units.DEGREES_DD})
    longitude: float = attr.ib(metadata={MdK.UNITS: Units.DEGREES_DD})
    raw: bytes

    @classmethod
    def parse(cls, data: bytes, loc: int, dg_size: int, start_pos: int):
        sdt_sz = struct.calcsize(cls.fmt)
        sdt_st = loc
        sdt_end = sdt_st + sdt_sz
        sdt_struct = struct.unpack(cls.fmt, data[sdt_st:sdt_end])

        loc += sdt_sz  # TODO: Another instance where we don't know the length of the structure

        # determine end loc
        end_loc = start_pos + dg_size
        raw = data[loc:end_loc - 4]
        loc = end_loc - 4

        # noinspection PyArgumentList
        return cls(
            depth_used=sdt_struct[0],
            depth_raw=sdt_struct[1],
            offset=sdt_struct[2],
            scale=sdt_struct[3],
            latitude=sdt_struct[4],
            longitude=sdt_struct[5],
            raw=raw), loc


@attr.s(auto_attribs=True)
class SDE:
    """ Depth sensor datagram, parent class """
    # TODO: SDE Datagram not tested
    desc = "Sensor (S) data from clock (CL)"

    header: Kmall
    sensor_info: SDEInfo
    sensor_data: SDEData

    @classmethod
    def parse(cls, data: bytes, loc: int = 0):
        st_loc = loc
        km_header, loc = Kmall.parse(data=data, loc=loc)
        s_info, loc = SDEInfo.parse(data=data, loc=loc)
        s_data, loc = SDEData.parse(data=data, loc=loc, dg_size=km_header.size,
                                    start_pos=st_loc)

        checksum(data=data, dg_sz=km_header.size, chk_st=loc)
        # noinspection PyArgumentList
        return cls.parse(header=km_header, sensor_info=s_info, sensor_data=s_data)


# ## SHI ##
@attr.s(auto_attribs=True)
class SHIData:
    """ Height sensor data, a child class of SHI """
    fmt: ClassVar[str] = '<Hf'

    sensor_type: int
    height_used: float = attr.ib(metadata={MdK.UNITS: Units.METER})
    raw: bytes

    @classmethod
    def parse(cls, data: bytes, loc: int, dg_size: int, start_pos: int):
        sh_sz = struct.calcsize(cls.fmt)
        sh_1 = loc
        sh_2 = sh_1 + sh_sz
        sh_struct = struct.unpack(cls.fmt, data[sh_1:sh_2])

        loc += sh_sz  # TODO: Another instance where we don't know the length of the structure

        # determine end loc
        end_loc = start_pos + dg_size
        raw = data[loc:end_loc - 4]
        loc = end_loc - 4

        # noinspection PyArgumentList
        return cls(sensor_type=sh_struct[0], height_used=sh_struct[1],
                   raw=raw), loc


@attr.s(auto_attribs=True)
class SHI:
    """ Height sensor datagram, parent class"""
    desc = "Sensor (S) data for height (HI)"

    header: Kmall
    sensor_info: SDEInfo  # TODO: Structures match. Would rather subclass...
    sensor_data: SHIData

    @classmethod
    def parse(cls, data: bytes, loc: int = 0):
        st_loc = loc
        km_header, loc = Kmall.parse(data=data, loc=loc)
        s_info, loc = SDEInfo.parse(data=data, loc=loc)
        s_data, loc = SHIData.parse(data=data, loc=loc, dg_size=km_header.size,
                                    start_pos=st_loc)

        checksum(data=data, dg_sz=km_header.size, chk_st=loc)
        # noinspection PyArgumentList
        return cls.parse(header=km_header, sensor_info=s_info, sensor_data=s_data)


# ### Compatibility datagrams ###
# ## CPO ##
@attr.s(auto_attribs=True)
class CPO(SPO):
    """ Compatibility position datagram, subclassed from SPO"""
    desc = "Compatibility (C) data for position (PO)"


# ## CHE ##
@attr.s(auto_attribs=True)
class CHEData:
    """ Heave sensor data, child structure of CHE """
    fmt: ClassVar[str] = '<f'

    heave: float = attr.ib(metadata={MdK.UNITS: Units.METER})

    @classmethod
    def parse(cls, data: bytes, loc: int):
        ch_sz = struct.calcsize(cls.fmt)
        ch_1 = loc
        ch_2 = ch_1 + ch_sz
        ch_struct = struct.unpack(cls.fmt, data[ch_1:ch_2])
        loc += ch_sz

        # noinspection PyArgumentList
        return cls(heave=ch_struct[0]), loc


@attr.s(auto_attribs=True)
class CHE:
    """ Compatibility heave datagram, parent class"""
    desc = "Compatibility (C)data for heave (HE)"

    header: Kmall
    mb_body: MBody
    data: CHEData

    @classmethod
    def parse(cls, data: bytes, loc: int = 0):
        km_header, loc = Kmall.parse(data=data, loc=loc)
        m_body, loc = MBody.parse(data=data, loc=loc)
        h_data, loc = CHEData.parse(data=data, loc=loc)

        checksum(data=data, dg_sz=km_header.size, chk_st=loc)
        # noinspection PyArgumentList
        return cls(header=km_header, mb_body=m_body, data=h_data)


# ### File Datagrams ###
# ## FCF ##
@attr.s(auto_attribs=True)
class FCFCommon:
    """ General file information, child structure to FCF """
    max_filename_length: ClassVar[int] = 64
    max_file_size: ClassVar[int] = 63000
    fmt: ClassVar[str] = f'<HbBI{max_filename_length}c'

    dict_status: ClassVar[dict] = {
        -1: "File not found",
        0: "OK",
        1: "File too large (cropped)"
    }

    # size: uint16
    # padding: uint16
    file_size: int
    file_name: str
    file_status: str

    @classmethod
    def parse(cls, data: bytes, loc: int):
        fc_sz = struct.calcsize(cls.fmt)
        fc_1 = loc
        fc_2 = fc_1 + fc_sz
        fc_struct = struct.unpack(cls.fmt, data[fc_1:fc_2])
        fname = b''.join(fc_struct[4:]).decode('utf-8')
        loc += fc_struct[0]

        # noinspection PyArgumentList
        return cls(file_status=cls.dict_status[fc_struct[1]],
                   file_size=fc_struct[3], file_name=fname), loc


@attr.s(auto_attribs=True)
class FCF:
    """ Backscatter calibration/BSCORR file datagram, parent class """
    # TODO: Not tested
    desc = "Backscatter calibration (C) file (F) datagram"

    header: Kmall
    partition: MPartition
    file_info: FCFCommon
    calibration_file: str

    @classmethod
    def parse(cls, data: bytes, loc: int = 0):
        km_header, loc = Kmall.parse(data=data, loc=loc)
        prt, loc = MPartition.parse(data=data, loc=loc)
        f_info, loc = FCFCommon.parse(data=data, loc=loc)

        # Parse File
        fl_fmt = f'<{f_info.file_size}c'
        fl_sz = struct.calcsize(fl_fmt)
        fl_1 = loc
        fl_2 = fl_1 + fl_sz
        fl_txt = struct.unpack(fl_fmt, data[fl_1:fl_2])
        fl_txt = b''.join(fl_txt).decode('utf-8')
        loc = fl_2

        checksum(data=data, dg_sz=km_header.size, chk_st=loc)
        # noinspection PyArgumentList
        return cls(header=km_header, partition=prt, file_info=f_info,
                   calibration_file=fl_txt)


# #### Module Functions ####

def km_datetime(time_sec: int, time_nanosec: int = 0):
    """ Generates datetime from km time format"""

    return datetime.utcfromtimestamp(time_sec) + timedelta(microseconds=(
        (time_nanosec / 1000.0)))


def get_flags(val: int, num_entries: int = 16, rt_int: bool = False) -> list:
    """ Returns the bytes encoded flags as a list of bool"""

    flags = list(f'{val:016b}')
    if rt_int is False:
        flags = list(map(lambda x: bool(int(x)), flags))
    else:
        flags = list(map(int, flags))

    flags.reverse()
    flags = flags[:num_entries]  # Drop padded/unused bits'

    return flags


def checksum(data, dg_sz: int, chk_st: int) -> bool:
    chk_fmt = '<I'
    chk_sz = struct.calcsize(chk_fmt)
    chk_end = chk_st + chk_sz
    chk = struct.unpack(chk_fmt, data[chk_st:chk_end])[0]

    if chk != dg_sz:
        raise IOError("Checksum and datagram size do not match")
    return True


def _skip_simple(file_obj: BinaryIO, fmt: str):
    """ Skips over structure using coded structure format"""

    bytes_to_skip = struct.calcsize(fmt)
    file_obj.seek(bytes_to_skip, 1)


def _skip_from_struct(file_obj: BinaryIO):
    """ Skip over structure using struct size in datagram """

    fmt = '<H'
    fmt_sz = struct.calcsize(fmt)
    bytes_to_skip = struct.unpack(fmt, file_obj.read(fmt_sz))[0]
    file_obj.seek(bytes_to_skip - fmt_sz, 1)


def _skip_from_datagram(file_obj: BinaryIO):
    """ Skip over entire datagram"""

    fmt = '<I'
    fmt_sz = struct.calcsize(fmt)
    bytes_to_skip = struct.unpack(fmt, file_obj.read(fmt_sz))[0]
    file_obj.seek(bytes_to_skip - fmt_sz, 1)


# #### Dispatch Table ####

# Dispatch table is used for late binding (i.e. calling dynamically at
# runtime). The table is used to replace long/slow if/elif/else code blocks.

kmall_dispatch = {
    b'KMALL': Kmall,

    b'#IIP': IIP,
    b'#IOP': IOP,
    b'#IBE': IBE,
    b'#IBR': IBR,
    b'#IBS': IBS,
    #
    b'#MRZ': MRZ,
    b'#MWC': MWC,

    b'#SPO': SPO,
    b'#SKM': SKM,
    b'#SVP': SVP,
    b'#SVT': SVT,
    b'#SCL': SCL,
    b'#SDE': SDE,
    b'#SHI': SHI,
    #
    b'#CPO': CPO,
    b'#CHE': CHE,
}
