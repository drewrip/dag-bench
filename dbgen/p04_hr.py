import duckdb, numpy as np, sys, os
from datetime import date, timedelta
from concurrent.futures import ProcessPoolExecutor
from utils.synth_utils import (
    GenerationProgress,
    batched_insert,
    get_worker_count,
    print_generation_summary,
    run_parallel,
)


def generate_departments_chunk(start, end, divs, locs):
    size = end - start
    rng = np.random.default_rng(start)
    div_indices = rng.integers(0, len(divs), size)
    loc_indices = rng.integers(0, len(locs), size)
    budgets = rng.uniform(100000, 10000000, size)
    headcount_targets = rng.integers(5, 101, size)
    
    rows = []
    for idx, i in enumerate(range(start, end)):
        rows.append((
            i,
            f"Dept-{i}",
            divs[div_indices[idx]],
            locs[loc_indices[idx]],
            round(float(budgets[idx]), 2),
            int(headcount_targets[idx]),
        ))
    return rows

def generate_employees_chunk(start, end, ND, mgr_ids, base, ttitles, etypes):
    size = end - start
    rng = np.random.default_rng(start)
    dept_ids = rng.integers(1, ND + 1, size)
    mgr_indices = rng.integers(0, len(mgr_ids), size)
    genders = ["M", "F", "NB"]
    gender_indices = rng.integers(0, len(genders), size)
    days_offset = rng.integers(0, 3001, size)
    ttitle_indices = rng.integers(0, len(ttitles), size)
    etype_indices = rng.integers(0, len(etypes), size)
    active_probs = rng.random(size)
    
    # Pre-check for manager_id exclusion
    mgr_ids_set = set(mgr_ids)

    rows = []
    for idx, i in enumerate(range(start, end)):
        rows.append((
            i,
            int(dept_ids[idx]),
            int(mgr_ids[mgr_indices[idx]]) if i not in mgr_ids_set else None,
            f"First{i}",
            f"Last{i}",
            genders[gender_indices[idx]],
            base + timedelta(days=int(days_offset[idx])),
            ttitles[ttitle_indices[idx]],
            etypes[etype_indices[idx]],
            bool(active_probs[idx] > 0.07),
        ))
    return rows

def generate_salaries_chunk(start, end, NE, base):
    size = end - start
    rng = np.random.default_rng(start)
    emp_ids = rng.integers(1, NE + 1, size)
    days_offset = rng.integers(0, 3001, size)
    base_salaries = rng.uniform(30000, 300000, size)
    bonuses = rng.uniform(0, 50000, size)

    rows = []
    for idx, i in enumerate(range(start, end)):
        rows.append((
            i,
            int(emp_ids[idx]),
            base + timedelta(days=int(days_offset[idx])),
            round(float(base_salaries[idx]), 2),
            round(float(bonuses[idx]), 2),
            "USD",
        ))
    return rows

def generate_performance_reviews_chunk(start, end, NE, base, cats):
    size = end - start
    rng = np.random.default_rng(start)
    emp_ids = rng.integers(1, NE + 1, size)
    days_offset = rng.integers(365, 3651, size)
    reviewer_ids = rng.integers(1, NE + 1, size)
    scores = rng.uniform(1, 5, size)
    cat_indices = rng.integers(0, len(cats), size)

    rows = []
    for idx, i in enumerate(range(start, end)):
        rows.append((
            i,
            int(emp_ids[idx]),
            base + timedelta(days=int(days_offset[idx])),
            int(reviewer_ids[idx]),
            round(float(scores[idx]), 2),
            cats[cat_indices[idx]],
            f"Review notes for review {i}",
        ))
    return rows

def generate_leave_requests_chunk(start, end, NE, base, ltypes):
    size = end - start
    rng = np.random.default_rng(start)
    emp_ids = rng.integers(1, NE + 1, size)
    ltype_indices = rng.integers(0, len(ltypes), size)
    days_offset = rng.integers(0, 3001, size)
    duration_days = rng.integers(1, 31, size)
    approved_probs = rng.random(size)

    rows = []
    for idx, i in enumerate(range(start, end)):
        sd = base + timedelta(days=int(days_offset[idx]))
        rows.append((
            i,
            int(emp_ids[idx]),
            ltypes[ltype_indices[idx]],
            sd,
            sd + timedelta(days=int(duration_days[idx])),
            bool(approved_probs[idx] > 0.1),
        ))
    return rows


def main():
    sf = float(sys.argv[1]) if len(sys.argv) > 1 else 1.0
    sf_adj = sf * 8989
    ND = max(5, int(30 * sf_adj))
    NE = max(20, int(800 * sf_adj))
    NS = max(30, int(2000 * sf_adj))
    NPR = max(20, int(1600 * sf_adj))
    NLR = max(10, int(1200 * sf_adj))


    os.makedirs("data", exist_ok=True)
    con = duckdb.connect("data/warehouse.duckdb")

    con.execute("""
    DROP TABLE IF EXISTS leave_requests; DROP TABLE IF EXISTS performance_reviews;
    DROP TABLE IF EXISTS salaries; DROP TABLE IF EXISTS employees;
    DROP TABLE IF EXISTS departments;
    CREATE TABLE departments(dept_id INTEGER PRIMARY KEY, name VARCHAR,
        division VARCHAR, location VARCHAR, budget DECIMAL(14,2), headcount_target INTEGER);
    CREATE TABLE employees(emp_id INTEGER PRIMARY KEY, dept_id INTEGER,
        manager_id INTEGER, first_name VARCHAR, last_name VARCHAR,
        gender VARCHAR, hire_date DATE, job_title VARCHAR,
        employment_type VARCHAR, is_active BOOLEAN);
    CREATE TABLE salaries(salary_id INTEGER PRIMARY KEY, emp_id INTEGER,
        effective_date DATE, base_salary DECIMAL(12,2), bonus DECIMAL(10,2),
        currency VARCHAR);
    CREATE TABLE performance_reviews(review_id INTEGER PRIMARY KEY, emp_id INTEGER,
        review_date DATE, reviewer_id INTEGER, score DECIMAL(4,2),
        category VARCHAR, notes VARCHAR);
    CREATE TABLE leave_requests(leave_id INTEGER PRIMARY KEY, emp_id INTEGER,
        leave_type VARCHAR, start_date DATE, end_date DATE, approved BOOLEAN);
    """)

    base = date(2015, 1, 1)
    divs = ["Engineering", "Sales", "Operations", "Finance", "Marketing", "HR"]
    locs = ["NYC", "London", "Berlin", "Tokyo", "Sydney", "Toronto"]
    ttitles = ["Engineer", "Manager", "Director", "Analyst", "Specialist", "Lead", "VP", "C-Suite"]
    etypes = ["full_time", "part_time", "contractor"]
    ltypes = ["annual", "sick", "parental", "unpaid", "bereavement"]
    cats = ["technical", "leadership", "communication", "teamwork", "delivery"]

    mgr_ids = list(range(1, max(2, NE // 10) + 1))

    cpu_count = get_worker_count()
    progress = GenerationProgress("p04_hr", 5)
    with ProcessPoolExecutor(max_workers=cpu_count) as executor:
        progress.advance("departments")
        batched_insert(con, "departments", ['dept_id', 'name', 'division', 'location', 'budget', 'headcount_target'],
                       run_parallel(executor, generate_departments_chunk, ND, divs, locs))
        progress.advance("employees")
        batched_insert(con, "employees", ['emp_id', 'dept_id', 'manager_id', 'first_name', 'last_name', 'gender', 'hire_date', 'job_title', 'employment_type', 'is_active'],
                       run_parallel(executor, generate_employees_chunk, NE, ND, mgr_ids, base, ttitles, etypes))
        progress.advance("salaries")
        batched_insert(con, "salaries", ['salary_id', 'emp_id', 'effective_date', 'base_salary', 'bonus', 'currency'],
                       run_parallel(executor, generate_salaries_chunk, NS, NE, base))
        progress.advance("performance_reviews")
        batched_insert(con, "performance_reviews", ['review_id', 'emp_id', 'review_date', 'reviewer_id', 'score', 'category', 'notes'],
                       run_parallel(executor, generate_performance_reviews_chunk, NPR, NE, base, cats))
        progress.advance("leave_requests")
        batched_insert(con, "leave_requests", ['leave_id', 'emp_id', 'leave_type', 'start_date', 'end_date', 'approved'],
                       run_parallel(executor, generate_leave_requests_chunk, NLR, NE, base, ltypes))


    con.close()
    print_generation_summary(
        "p04_hr",
        sf,
        {
            "departments": ND,
            "employees": NE,
            "salaries": NS,
            "performance_reviews": NPR,
            "leave_requests": NLR,
        },
    )

if __name__ == "__main__":
    main()
