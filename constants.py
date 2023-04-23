HIRED_EMPLOYEES_DT = {
    "ID": "int",
    "NAME": "str",
    "DATETIME": "date",
    "DEPARTMENT_ID": "int",
    "JOB_ID": "int",
}

DEPARTMENTS_DT = {"ID": "int", "DEPARTMENT": "str"}

JOBS_DT = {"ID": "int", "JOB": "str"}

VALID_ENTITIES = ['departments', 'hired_employees', 'jobs']

DICT_DT = {
    "departments": JOBS_DT,
    "hired_employees": HIRED_EMPLOYEES_DT,
    "jobs": JOBS_DT
}

HIRE_EMPLOYEES_SCHEMA = {
    "name": "hire_employees",
    "type": "record",
    "fields": [
        {"name": "ID", "type": ["float", "null"]},
        {"name": "NAME", "type": ["string", "null"]},
        {"name": "DATETIME", "type": ["string", "null"]},
        {"name": "DEPARTMENT_ID", "type": ["float", "null"]},
        {"name": "JOB_ID", "type": ["float", "null"]},
    ],
}

DEPARMENT_SCHEMA = {
    "name": "departments",
    "type": "record",
    "fields": [
        {"name": "ID", "type": ["float", "null"]},
        {"name": "DEPARTMENT", "type": ["string", "null"]},
    ],
}

JOB_SCHEMA = {
    "name": "jobs",
    "type": "record",
    "fields": [
        {"name": "ID", "type": ["float", "null"]},
        {"name": "JOB", "type": ["string", "null"]},
    ],
}

AVRO_SCHEMA = {
    "departments": DEPARMENT_SCHEMA,
    "hired_employees": HIRE_EMPLOYEES_SCHEMA,
    "jobs": JOB_SCHEMA
}
