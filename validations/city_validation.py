def validate_city(line):
    city = line.strip().split(",")[-1]
    if city not in ["Mumbai","Bangalore"]:
        return "city is not in Mumbai or Bangalore"