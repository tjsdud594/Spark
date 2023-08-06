create table if not exists examples.emp_work(
	emp_no INT not null,
	first_name VARCHAR(14),
	last_name VARCHAR(16),
	gender VARCHAR(1),
	dept_no VARCHAR(4),
	dept_name VARCHAR(40),
	dept_work_years VARCHAR(20),
	work_years DECIMAL(17,2),
	create_date TIMESTAMP,
	CONSTRAINT emp_work_PK PRIMARY KEY(emp_no)
);