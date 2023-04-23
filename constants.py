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
