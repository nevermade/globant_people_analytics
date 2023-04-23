with hired_by_department as (
select a.department_id as id,b.department,count(a.id) hired
from "hired_employees" a
left join "departments" b on a.department_id = b.id
where date_part('year',cast(a.datetime as datetime)) = 2021
group by a.department_id,b.department
)
select id, department, hired as counts
from hired_by_department a
where hired > (select AVG(hired) from hired_by_department)
order by hired desc