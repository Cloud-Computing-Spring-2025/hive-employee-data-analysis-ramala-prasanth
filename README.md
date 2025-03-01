# Hive Assignment - Employee and Department Data Analysis

## **Overview**
This project involves analyzing employee and department data using Apache Hive. We will load the data from CSV files, transform it, store it in partitioned tables, and perform various analytical queries. The output of each query will be saved and exported for further analysis.

---

## **Dataset Details**

### **1. employees.csv**
This dataset contains employee details, including department, job role, salary, and project assignment.

| Column Name | Description |
|-------------|-------------|
| emp_id | Unique employee ID |
| name | Employee's full name |
| age | Employee's age |
| job_role | Employee's designation |
| salary | Annual salary |
| project | Assigned project (Alpha, Beta, Gamma, Delta, Omega) |
| join_date | Date the employee joined |
| department | Department to which the employee belongs (Used for partitioning) |

### **2. departments.csv**
This dataset contains department information.

| Column Name | Description |
|-------------|-------------|
| dept_id | Unique department ID |
| department_name | Name of the department |
| location | Location of the department |

---

## **Steps to Set Up Data in Hadoop and Hive**

### **Step 1: Upload CSV Files to HDFS**
Use the following commands to upload the datasets to HDFS:
```sh
hdfs dfs -mkdir -p /user/hive/data
hdfs dfs -put employees.csv /user/hive/data/
hdfs dfs -put departments.csv /user/hive/data/
```

### **Step 2: Create Hive Tables**
#### **Create a Temporary Table for Employees**
```sql
CREATE TABLE employees_temp (
    emp_id INT,
    name STRING,
    age INT,
    job_role STRING,
    salary DOUBLE,
    project STRING,
    join_date STRING,
    department STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;
```
#### **Load Data into the Temporary Table**
```sql
LOAD DATA INPATH '/user/hive/data/employees.csv' INTO TABLE employees_temp;
```

#### **Create a Partitioned Table for Employees**
```sql
CREATE TABLE employees (
    emp_id INT,
    name STRING,
    age INT,
    job_role STRING,
    salary DOUBLE,
    project STRING,
    join_date STRING
)
PARTITIONED BY (department STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS PARQUET;
```
#### **Insert Data into the Partitioned Table**
```sql
SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;

INSERT INTO TABLE employees PARTITION (department)
SELECT emp_id, name, age, job_role, salary, project, join_date, department FROM employees_temp;
```

#### **Create the Departments Table**
```sql
CREATE TABLE departments (
    dept_id INT,
    department_name STRING,
    location STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;
```

#### **Load Data into Departments Table**
```sql
LOAD DATA INPATH '/user/hive/data/departments.csv' INTO TABLE departments;
```

---

## **SQL Queries and Sample Output**

### **1. Retrieve Employees Who Joined After 2015**
```sql
SELECT * FROM employees WHERE year(TO_DATE(join_date, 'yyyy-MM-dd')) > 2015;
```
**Sample Output:**
| emp_id | name | age | job_role | salary | project | join_date | department |
|--------|------|-----|----------|--------|---------|-----------|------------|
| 101 | Alice | 30 | Engineer | 70000 | Alpha | 2016-07-10 | IT |

---

### **2. Average Salary of Employees by Department**
```sql
SELECT department, AVG(salary) AS avg_salary FROM employees GROUP BY department;
```
**Sample Output:**
| department | avg_salary |
|------------|------------|
| IT | 75000 |
| HR | 60000 |

---

### **3. Employees Working on 'Alpha' Project**
```sql
SELECT * FROM employees WHERE project = 'Alpha';
```

---

### **4. Employee Count by Job Role**
```sql
SELECT job_role, COUNT(*) AS employee_count FROM employees GROUP BY job_role;
```

---

### **5. Employees Earning Above Department Average**
```sql
SELECT e.* FROM employees e
JOIN (
    SELECT department, AVG(salary) AS avg_salary
    FROM employees
    GROUP BY department
) dept_avg
ON e.department = dept_avg.department
WHERE e.salary > dept_avg.avg_salary;
```

---

### **6. Department with the Highest Employee Count**
```sql
SELECT department, COUNT(*) AS employee_count
FROM employees
GROUP BY department
ORDER BY employee_count DESC
LIMIT 1;
```

---

### **7. Exclude Employees with Null Values**
```sql
SELECT * FROM employees
WHERE emp_id IS NOT NULL
AND name IS NOT NULL
AND age IS NOT NULL
AND job_role IS NOT NULL
AND salary IS NOT NULL
AND project IS NOT NULL
AND join_date IS NOT NULL
AND department IS NOT NULL;
```

---

### **8. Join Employees and Departments for Location Details**
```sql
SELECT e.*, d.location
FROM employees e
JOIN departments d
ON e.department = d.department_name;
```

---

### **9. Rank Employees by Salary Within Each Department**
```sql
SELECT emp_id, name, department, salary,
       RANK() OVER (PARTITION BY department ORDER BY salary DESC) AS salary_rank
FROM employees;
```

---

### **10. Top 3 Highest-Paid Employees in Each Department**
```sql
SELECT emp_id, name, department, salary, salary_rank
FROM (
    SELECT emp_id, name, department, salary,
           RANK() OVER (PARTITION BY department ORDER BY salary DESC) AS salary_rank
    FROM employees
) ranked
WHERE salary_rank <= 3;
```

---

## **Exporting Query Results in Hue**
Since we are using the Hue Editor:
1. Run the `SELECT` query.
2. Click on the **Export** button in the results section.
3. Choose the format (CSV, Excel, JSON) and download the file.

Alternatively, for automated output storage:
```sql
INSERT OVERWRITE DIRECTORY '/user/hive/output/employees_after_2015'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT * FROM employees WHERE year(TO_DATE(join_date, 'yyyy-MM-dd')) > 2015;
```
Retrieve using:
```sh
hdfs dfs -cat /user/hive/output/employees_after_2015/*
```

---

## **Conclusion**
This project covers Hive query execution, data partitioning, and exporting query results. The structured approach ensures efficient analysis of employee and department data.

---
**Author:** Prasanth Reddy Ramala
**Date:** 2025-02-28

