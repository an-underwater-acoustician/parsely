import attr
import logging

from datetime import datetime, date, time

from parsely._internal.metadata_mappings import MetadataKeys as MdK
from parsely._internal.metadata_mappings import Units


logger = logging.getLogger(__name__)

# Module Dictionaries
gga_fix_type = {
        0: "Invalid",
        1: "Autonomous GPS",
        2: "DGPS",
        3: "PPS",
        4: "RTK",
        5: "RTK Float",
        6: "Estimated",
        7: "Manual Input",
        8: "Simulation",
        9: "WAAS"
    }


@attr.s(auto_attribs=True)
class GGA:
    time: time
    latitude: float = attr.ib(metadata={MdK.UNITS: Units.DEGREES_DD})
    longitude: float = attr.ib(metadata={MdK.UNITS: Units.DEGREES_DD})
    fix: str
    num_satellites: int
    hdop: float
    altitude: float = attr.ib(metadata={MdK.UNITS: Units.METER})
    height_geoid: float = attr.ib(metadata={MdK.UNITS: Units.METER})
    correction_age: float = attr.ib(metadata={MdK.UNITS: Units.SECOND})
    correction_station: int

    @classmethod
    def parse(cls, text: str):
        tokens = text.split(',')
        fix_time = tokens[1]
        nmea_time = str_2_time(fix_time)

        lat_str = tokens[2]
        deg = int(lat_str[0:2])
        deg_min = float(lat_str[2:])
        hemi = tokens[3]
        lat = deg + deg_min / 60
        if hemi == 'S':
            lat = -lat

        lon_str = tokens[4]
        deg = int(lon_str[0:3])
        deg_min = float(lat_str[3:])
        hemi = tokens[5]
        lon = deg + deg_min / 60
        if hemi == 'W':
            lon = -lon

        # noinspection PyArgumentList
        return cls(
            time=nmea_time,
            latitude=lat,
            longitude=lon,
            fix=gga_fix_type[int(tokens[6])],
            num_satellites=int(tokens[7]),
            hdop=float(tokens[8]),
            altitude=float(tokens[9]),
            height_geoid=float(tokens[11]),
            correction_age=float(tokens[13]),
            correction_station=int(tokens[14].split('*')[0])
        )


@attr.s(auto_attribs=True)
class ZDA:
    date_time: datetime

    @classmethod
    def parse(cls, text: str):
        tokens = text.split(',')
        utc_time = tokens[1]
        nema_time = str_2_time(utc_time)
        clk_date = date(year=int(tokens[4]), month=int(tokens[3]),
                        day=int(tokens[2]))

        # noinspection PyArgumentList
        return cls(date_time=datetime.combine(date=clk_date, time=nema_time))


# ### Module Methods ###
def str_2_time(time_str: str) -> time:
    hh = int(time_str[0:2])
    mm = int(time_str[2:4])
    ss = int(float(time_str[4:]))
    us = int(round(float(time_str[4:]) % 1, 2) * 1e6)
    nema_time = time(hh, mm, ss, us)
    return nema_time
