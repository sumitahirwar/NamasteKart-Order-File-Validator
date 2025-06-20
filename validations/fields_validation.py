def validate_field(line):
    fields = line.strip().split(",")
    if len(fields) != 6:
        return "some fields are blank"