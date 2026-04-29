import duckdb, random, sys, os
from datetime import date, timedelta
from concurrent.futures import ProcessPoolExecutor
from utils.synth_utils import batched_insert, run_parallel


def generate_departments_chunk(start, end, divs, locs):
    return [
        (
            i,
            f"Dept-{i}",
            random.choice(divs),
            random.choice(locs),
            round(random.uniform(100000, 10000000), 2),
            random.randint(5, 100),
        )
        for i in range(start, end)
    ]

def generate_employees_chunk(start, end, ND, mgrs, base, titles, etypes):
    return [
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
        for i in range(start, end)
    ]

def generate_salaries_chunk(start, end, NE, base):
    return [
        (
            i,
            random.randint(1, NE),
            base + timedelta(days=random.randint(0, 3000)),
            round(random.uniform(30000, 300000), 2),
            round(random.uniform(0, 50000), 2),
            "USD",
        )
        for i in range(start, end)
    ]

def generate_performance_reviews_chunk(start, end, NE, base, cats):
    return [
        (
            i,
            random.randint(1, NE),
            base + timedelta(days=random.randint(365, 3650)),
            random.randint(1, NE),
            round(random.uniform(1, 5), 2),
            random.choice(cats),
        )
        for i in range(start, end)
    ]

def generate_leave_requests_chunk(start, end, NE, base, ltypes):
    return [
        (
            i,
            random.randint(1, NE),
            random.choice(ltypes),
            sd := base + timedelta(days=random.randint(0, 3000)),
            sd + timedelta(days=random.randint(1, 30)),
            random.random() > 0.1,
        )
        for i in range(start, end)
    ]


def main():
    sf = float(sys.argv[1]) if len(sys.argv) > 1 else 1.0
    sf *= 100
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

    mgrs = list(range(1, max(2, NE // 10) + 1))
    cpu_count = min(4, os.cpu_count() or 1)
    
    with ProcessPoolExecutor(max_workers=cpu_count) as executor:
        batched_insert(con, "departments", ['dept_id', 'name', 'division', 'location', 'budget', 'headcount_target'], 
                       run_parallel(executor, generate_departments_chunk, ND, divs, locs))
        
        batched_insert(con, "employees", ['emp_id', 'dept_id', 'manager_id', 'first_name', 'last_name', 'gender', 'hire_date', 'job_title', 'employment_type', 'is_active'],
                       run_parallel(executor, generate_employees_chunk, NE, ND, mgrs, base, titles, etypes))
        
        batched_insert(con, "salaries", ['salary_id', 'emp_id', 'effective_date', 'base_salary', 'bonus', 'currency'],
                       run_parallel(executor, generate_salaries_chunk, NS, NE, base))
        
        batched_insert(con, "performance_reviews", ['review_id', 'emp_id', 'review_date', 'reviewer_id', 'score', 'category'],
                       run_parallel(executor, generate_performance_reviews_chunk, NPR, NE, base, cats))
        
        batched_insert(con, "leave_requests", ['leave_id', 'emp_id', 'leave_type', 'start_date', 'end_date', 'approved'],
                       run_parallel(executor, generate_leave_requests_chunk, NLR, NE, base, ltypes))


    con.close()
    print(f"p04 done depts={ND} emps={NE}")

if __name__ == "__main__":
    main()
