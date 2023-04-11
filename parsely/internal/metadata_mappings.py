from enum import Enum, auto


class MetadataKeys(Enum):
    UNITS = 1
    PULSE = 2
    BEAM_SPACING = 3


class Units(Enum):
    # Time
    NANOSECOND = auto()
    SECOND = auto()
    MINUTE = auto()
    HOUR = auto()

    # Base Units (not base SI, just base for the program)
    METER = auto()
    DEGREE = auto()
    HZ = auto()

    CELSIUS = auto()
    PSU = auto()
    PASCAL = auto()

    DB = auto()
    SAMPLE = auto()

    # Geographic Specific Units
    DEGREES_DD = auto()

    # Derived Units
    METERSPERSECOND = auto()
    METERSPERSECOND2 = auto()
    DBPERMETER = auto()

    DEGREESPERSECOND = auto()

    # Miscellaneous
    PERCENT = auto()


class Pulse(Enum):
    CW = auto()
    FM = auto()
    FMUP = auto()
    FMDOWN = auto()


class BeamSpacing(Enum):
    EQUIDISTANT = auto()
    EQUIANGLE = auto()
    HIGHDENSITY = auto()


# Dictionary of unit descriptions in format:
#   Category :: Standard Name ::
unit_desc = {
    Units.NANOSECOND: "Nanoseconds (ns)",
    Units.SECOND: "Seconds (s)",
    Units.MINUTE: "Minutes (mm)",
    Units.HOUR: "Hours (hh)",
    Units.METER: "Meters (m)",
    Units.DEGREE: "Angular Degrees (deg)",  # Unit for angular/rotational sensor measurments
    Units.HZ: "Hertz (Hz -> s^-1)",
    Units.CELSIUS: "Degrees Celsius",
    Units.PSU: "Practical Salinity Units (psu)",
    Units.PASCAL: "Pascal (Pa)",
    Units.DB: "Decibel (dB)",
    Units.SAMPLE: "sample point (samp)",
    Units.METERSPERSECOND: "Meters per second (m/s)",
    Units.METERSPERSECOND2: "Meters per second squared (m/s^2)",
    Units.DBPERMETER: "decibels per meter (dB/m)",
    Units.DEGREESPERSECOND: "Angular degrees per second (deg/s)",
    Units.PERCENT: "Percentage (%)"
}

pulse_desc = {
    Pulse.CW: "Continuous Wave",
    Pulse.FM: "Frequency Modulated Wave",
    Pulse.FMUP: "Upsweep FM Wave",
    Pulse.FMDOWN: "Downsweep FM Wave"
}

beam_spacing_desc = {
    BeamSpacing.EQUIDISTANT: "Equidistant beam spacing",
    BeamSpacing.EQUIANGLE: "Equiangular beam spacing",
    BeamSpacing.HIGHDENSITY: "Kongsberg high density beam spacing"
}
