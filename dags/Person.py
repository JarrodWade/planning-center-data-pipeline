from dataclasses import dataclass


# Person class with the variables we want to eventually put out to CSV
# really bare-bones now with use of dataclass decorator
@dataclass
class Person:
    person_id: str
    person_list: str
    name: str
    primary_email: str
    primary_phone_number: str
    grade: str = ''
    age: str = ''
