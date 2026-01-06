from Tools.cell_mapper import decode_location_info

samples = [
    '0x823708401b59370840000fb20d',
    '0x823708401b593708400012290b',
    '0x823708401b593708400012220c',
    '0x823708401b59370840000fc70e',
    '0x823708401b59370840000ff50c',
]

for hex_val in samples:
    result = decode_location_info(hex_val)
    print(f"{hex_val} ➤ {result}")
