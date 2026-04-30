import duckdb, sys, os
import numpy as np
from datetime import date, timedelta
from concurrent.futures import ProcessPoolExecutor
from utils.synth_utils import batched_insert, run_parallel


def generate_departments_chunk(start, end, divs, locs):
    rng = np.random.default_rng(start)
    size = end - start
    div_idx = rng.integers(0, len(divs), size)
    loc_idx = rng.integers(0, len(locs), size)
    bud_rand = rng.uniform(100000, 10000000, size)
    hc_rand = rng.integers(5, 101, size)
    return [
        (
            i,
            f"Dept-{i}",
            divs[div_idx[i - start]],
            locs[loc_idx[i - start]],
            round(float(bud_rand[i - start]), 2),
            int(hc_rand[i - start]),
        )
        for i in range(start, end)
    ]


def generate_employees_chunk(start, end, ND, mgrs, base, titles, etypes):
    rng = np.random.default_rng(start)
    size = end - start
    dept_rand = rng.integers(1, ND + 1, size)
    mgr_idx = rng.integers(0, len(mgrs), size)
    gen_idx = rng.integers(0, 3, size)
    days_rand = rng.integers(0, 3001, size)
    title_idx = rng.integers(0, len(titles), size)
    etype_idx = rng.integers(0, len(etypes), size)
    active_rand = rng.random(size)
    genders = ["M", "F", "NB"]
    return [
        (
            i,
            int(dept_rand[i - start]),
            int(mgrs[mgr_idx[i - start]]) if i not in mgrs else None,
            f"First{i}",
            f"Last{i}",
            genders[gen_idx[i - start]],
            base + timedelta(days=int(days_rand[i - start])),
            titles[title_idx[i - start]],
            etypes[etype_idx[i - start]],
            bool(active_rand[i - start] > 0.07),
        )
        for i in range(start, end)
    ]


def generate_salaries_chunk(start, end, NE, base):
    rng = np.random.default_rng(start)
    size = end - start
    emp_rand = rng.integers(1, NE + 1, size)
    days_rand = rng.integers(0, 3001, size)
    sal_rand = rng.uniform(30000, 300000, size)
    bon_rand = rng.uniform(0, 50000, size)
    return [
        (
            i,
            int(emp_rand[i - start]),
            base + timedelta(days=int(days_rand[i - start])),
            round(float(sal_rand[i - start]), 2),
            round(float(bon_rand[i - start]), 2),
            "USD",
        )
        for i in range(start, end)
    ]


def generate_performance_reviews_chunk(start, end, NE, base, cats):
    rng = np.random.default_rng(start)
    size = end - start
    emp_rand = rng.integers(1, NE + 1, size)
    days_rand = rng.integers(365, 3651, size)
    reviewer_rand = rng.integers(1, NE + 1, size)
    score_rand = rng.uniform(1, 5, size)
    cat_idx = rng.integers(0, len(cats), size)
    return [
        (
            i,
            int(emp_rand[i - start]),
            base + timedelta(days=int(days_rand[i - start])),
            int(reviewer_rand[i - start]),
            round(float(score_rand[i - start]), 2),
            cats[cat_idx[i - start]],
        )
        for i in range(start, end)
    ]


def generate_leave_requests_chunk(start, end, NE, base, ltypes):
    rng = np.random.default_rng(start)
    size = end - start
    emp_rand = rng.integers(1, NE + 1, size)
    type_idx = rng.integers(0, len(ltypes), size)
    sd_days = rng.integers(0, 3001, size)
    len_days = rng.integers(1, 31, size)
    app_rand = rng.random(size)
    return [
        (
            i,
            int(emp_rand[i - start]),
            ltypes[type_idx[i - start]],
            (sd := base + timedelta(days=int(sd_days[i - start]))),
            sd + timedelta(days=int(len_days[i - start])),
            bool(app_rand[i - start] > 0.1),
        )
        for i in range(start, end)
    ]


def main():
    sf = float(sys.argv[1]) if len(sys.argv) > 1 else 1.0
    sf_adj = sf * 1
    ND, NE, NS, NPR, NLR = (
        max(a, int(b * sf_adj))
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
    cpu_count = os.cpu_count()

    with ProcessPoolExecutor(max_workers=cpu_count) as executor:
        batched_insert(
            con,
            "departments",
            ["dept_id", "name", "division", "location", "budget", "headcount_target"],
            run_parallel(executor, generate_departments_chunk, ND, divs, locs),
        )

        batched_insert(
            con,
            "employees",
            [
                "emp_id",
                "dept_id",
                "manager_id",
                "first_name",
                "last_name",
                "gender",
                "hire_date",
                "job_title",
                "employment_type",
                "is_active",
            ],
            run_parallel(
                executor, generate_employees_chunk, NE, ND, mgrs, base, titles, etypes
            ),
        )

        batched_insert(
            con,
            "salaries",
            [
                "salary_id",
                "emp_id",
                "effective_date",
                "base_salary",
                "bonus",
                "currency",
            ],
            run_parallel(executor, generate_salaries_chunk, NS, NE, base),
        )

        batched_insert(
            con,
            "performance_reviews",
            ["review_id", "emp_id", "review_date", "reviewer_id", "score", "category"],
            run_parallel(
                executor, generate_performance_reviews_chunk, NPR, NE, base, cats
            ),
        )

        batched_insert(
            con,
            "leave_requests",
            ["leave_id", "emp_id", "leave_type", "start_date", "end_date", "approved"],
            run_parallel(
                executor, generate_leave_requests_chunk, NLR, NE, base, ltypes
            ),
        )

    con.close()
    print(f"p04 done depts={ND} emps={NE}")


if __name__ == "__main__":
    main()
