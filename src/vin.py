import typing as ty

Vin = ty.NewType("Vin", str)
ShortVin = ty.NewType("ShortVin", str)


def truncate_vin(vin: Vin) -> ShortVin:
    return ShortVin(vin[:11])



# the ravvy <3
TEST_VIN = "JTEHD20V650050824"
assert len(TEST_VIN) == 17
