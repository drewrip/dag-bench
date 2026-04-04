import duckdb, random, sys, os
from datetime import date, timedelta

sf = float(sys.argv[1]) if len(sys.argv) > 1 else 1.0
ND, NE, NS, NPR, NLR = (
    max(a, int(b * sf))
    for a, b in [(5, 30), (20, 800), (30, 2000), (20, 1600), (10, 1200)]
)
os.makedirs("data", exist_ok=True)
con = duckdb.connect("data/warehouse.duckdb")
con.execute("""
DROP TABLE IF EXISTS leave_requests; DROP TABLE IF EXISTS performance_reviews;
DROP TABLE IF EXISTS salaries; DROP TABLE IF EXISTS employees; DROP TABLE IF EXISTS departments;
CREATE TABLE departments(dept_id INTEGER PRIMARY KEY,name VARCHAR,division VARCHAR,
  location VARCHAR,budget DECIMAL(14,2),headcount_target INTEGER);
CREATE TABLE employees(emp_id INTEGER PRIMARY KEY,dept_id INTEGER,manager_id INTEGER,
  first_name VARCHAR,last_name VARCHAR,gender VARCHAR,hire_date DATE,
  job_title VARCHAR,employment_type VARCHAR,is_active BOOLEAN);
CREATE TABLE salaries(salary_id INTEGER PRIMARY KEY,emp_id INTEGER,effective_date DATE,
  base_salary DECIMAL(12,2),bonus DECIMAL(10,2),currency VARCHAR);
CREATE TABLE performance_reviews(review_id INTEGER PRIMARY KEY,emp_id INTEGER,
  review_date DATE,reviewer_id INTEGER,score DECIMAL(4,2),category VARCHAR);
CREATE TABLE leave_requests(leave_id INTEGER PRIMARY KEY,emp_id INTEGER,
  leave_type VARCHAR,start_date DATE,end_date DATE,approved BOOLEAN);
""")
base = date(2015, 1, 1)
divs = ["Engineering", "Sales", "Operations", "Finance", "Marketing", "HR"]
locs = ["NYC", "London", "Berlin", "Tokyo", "Sydney", "Toronto"]
titles = ["Engineer", "Manager", "Director", "Analyst", "Specialist", "Lead", "VP"]
etypes = ["full_time", "part_time", "contractor"]
ltypes = ["annual", "sick", "parental", "unpaid", "bereavement"]
cats = ["technical", "leadership", "communication", "teamwork", "delivery"]
con.executemany(
    "INSERT INTO departments VALUES(?,?,?,?,?,?)",
    [
        (
            i,
            f"Dept-{i}",
            random.choice(divs),
            random.choice(locs),
            round(random.uniform(100000, 10000000), 2),
            random.randint(5, 100),
        )
        for i in range(1, ND + 1)
    ],
)
mgrs = list(range(1, max(2, NE // 10) + 1))
con.executemany(
    "INSERT INTO employees VALUES(?,?,?,?,?,?,?,?,?,?)",
    [
        (
            i,
            random.randint(1, ND),
            random.choice(mgrs) if i not in mgrs else None,
            f"First{i}",
            f"Last{i}",
            random.choice(["M", "F", "NB"]),
            base + timedelta(days=random.randint(0, 3000)),
            random.choice(titles),
            random.choice(etypes),
            random.random() > 0.07,
        )
        for i in range(1, NE + 1)
    ],
)
con.executemany(
    "INSERT INTO salaries VALUES(?,?,?,?,?,?)",
    [
        (
            i,
            random.randint(1, NE),
            base + timedelta(days=random.randint(0, 3000)),
            round(random.uniform(30000, 300000), 2),
            round(random.uniform(0, 50000), 2),
            "USD",
        )
        for i in range(1, NS + 1)
    ],
)
con.executemany(
    "INSERT INTO performance_reviews VALUES(?,?,?,?,?,?)",
    [
        (
            i,
            random.randint(1, NE),
            base + timedelta(days=random.randint(365, 3650)),
            random.randint(1, NE),
            round(random.uniform(1, 5), 2),
            random.choice(cats),
        )
        for i in range(1, NPR + 1)
    ],
)
con.executemany(
    "INSERT INTO leave_requests VALUES(?,?,?,?,?,?)",
    [
        (
            i,
            random.randint(1, NE),
            random.choice(ltypes),
            sd := base + timedelta(days=random.randint(0, 3000)),
            sd + timedelta(days=random.randint(1, 30)),
            random.random() > 0.1,
        )
        for i in range(1, NLR + 1)
    ],
)
con.close()
print(f"p04 done depts={ND} emps={NE}")
