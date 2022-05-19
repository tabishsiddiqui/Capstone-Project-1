invalidate metadata tabish_project.employees;
invalidate metadata tabish_project.titles;
invalidate metadata tabish_project.departments;
invalidate metadata tabish_project.salaries;
invalidate metadata tabish_project.dept_manager;
invalidate metadata tabish_project.dept_emp;

use tabish_project;

select t1.emp_no, t1.first_name, t1.last_name, t1.sex, t2.salary from employees t1 join salaries t2 on t1.emp_no = t2.emp_no limit 5;

select t1.dept_no, t1.dept_name, t3.emp_no, t3.first_name, t3.last_name from departments t1 left join dept_manager t2 on t1.dept_no=t2.dept_no left join employees t3 on t2.emp_no=t3.emp_no limit 5;

select t1.emp_no, t1.first_name, t1.last_name, t3.dept_name from employees t1 join dept_emp t2 on t1.emp_no=t2.emp_no join departments t3 on t2.dept_no=t3.dept_no limit 5;

select first_name, last_name, sex from employees where first_name = 'Hercules' and last_name like 'B%' limit 5;

select t1.emp_no, t1.first_name, t1.last_name, t3.dept_name from employees t1 join dept_emp t2 on t1.emp_no=t2.emp_no join departments t3 on t2.dept_no=t3.dept_no where t3.dept_name='"Sales"' limit 5;

select t1.emp_no, t1.first_name, t1.last_name, t3.dept_name from employees t1 join dept_emp t2 on t1.emp_no=t2.emp_no join departments t3 on t2.dept_no=t3.dept_no where t3.dept_name = '"Sales"' or t3.dept_name = '"development"' limit 5;

select last_name, count(last_name) as freq from employees group by last_name order by freq desc limit 5;

select no_of_projects, count(emp_no) from employees group by no_of_projects order by count(emp_no) desc;

select last_performance_rating, count(emp_no) from employees group by last_performance_rating order by count(emp_no) desc;

select t1.dept_name, count(t2.emp_no) from departments t1 join dept_emp t2 on t1.dept_no=t2.dept_no group by t1.dept_name;

select t1.dept_name, sum(t3.salary) from departments t1 join dept_emp t2 on t1.dept_no=t2.dept_no join salaries t3 on t2.emp_no=t3.emp_no group by t1.dept_name order by sum(t3.salary) desc;