with quarter_hr as (
select b.department,c.job, 
    date_part('month',cast(a.datetime as datetime)) as month
from "hired_employees" a
left join "departments" b on a.department_id = b.id
left join "jobs" c on a.job_id = c.id
where date_part('year',cast(a.datetime as datetime)) = 2021
)
select 
    department, job, 
    sum(case when month in (1,2,3) then 1 else 0 end) Q1,
    sum(case when month in (4,5,6) then 1 else 0 end) Q2,
    sum(case when month in (7,8,9) then 1 else 0 end) Q3,
    sum(case when month in (10,11,12) then 1 else 0 end) Q4
from quarter_hr
group by department,job
order by department, job