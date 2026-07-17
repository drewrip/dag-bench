use crate::common::{round_to, weighted_choice, PopularityWeights};
use chrono::{Duration, NaiveDate};
use duckdb::DuckdbConnectionManager;
use indicatif::{ProgressBar, ProgressStyle};
use r2d2::Pool;
use rand::prelude::*;
use rand::rngs::SmallRng;

pub fn run(sf: f64, pool: &mut Pool<DuckdbConnectionManager>) -> duckdb::Result<()> {
    let nd = (122133.0 * sf).max(5.0) as usize;
    let ne = (3257326.0 * sf).max(20.0) as usize;
    // Leave_requests fan out off employees (~2.5/year over an effective ~3
    // year average tenure within the window).
    let nlr = ((ne as f64) * 7.5).max(10.0) as usize;

    let con = &pool.get().expect("couldn't get connection");

    con.execute_batch(
        "DROP TABLE IF EXISTS leave_requests; DROP TABLE IF EXISTS performance_reviews;
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
             leave_type VARCHAR, start_date DATE, end_date DATE, approved BOOLEAN);",
    )?;

    let base_date = NaiveDate::from_ymd_opt(2015, 1, 1).unwrap();
    let observation_end = base_date + Duration::days(3651);

    // Fixed vocabularies.
    let divisions = [
        "Engineering", "Sales", "Customer Success", "Marketing", "Operations", "Finance",
        "Product", "HR", "Legal",
    ];
    let division_weights = [25.0, 18.0, 12.0, 10.0, 10.0, 8.0, 8.0, 5.0, 4.0];
    let locations = ["New York", "San Francisco", "Austin", "London", "Berlin", "Remote"];
    let location_weights = [25.0, 20.0, 15.0, 10.0, 10.0, 20.0];
    let genders = ["female", "male", "nonbinary_other"];
    let gender_weights = [48.0, 48.0, 4.0];
    let ic_titles = ["IC1", "IC2", "IC3", "IC4", "IC5"];
    let ic_title_weights = [15.0, 25.0, 30.0, 20.0, 10.0];
    let mgr_titles = ["Manager", "Senior Manager", "Director", "VP"];
    let mgr_title_weights = [60.0, 25.0, 10.0, 5.0];
    let employment_types = ["full_time", "contractor", "part_time", "intern"];
    let employment_type_weights = [85.0, 8.0, 5.0, 2.0];
    let review_categories = ["exceeds_expectations", "meets_expectations", "needs_improvement", "unsatisfactory"];
    let review_category_weights = [20.0, 60.0, 15.0, 5.0];
    let leave_types = ["vacation", "sick", "parental", "unpaid", "bereavement", "jury_duty"];
    let leave_type_weights = [55.0, 25.0, 8.0, 5.0, 4.0, 3.0];

    // Top ~9% of earliest hires are manager-eligible; everyone else reports
    // into that pool. Department sizes and leave-taking are popularity-skewed.
    let mgr_id_limit = ((ne as f64 * 0.09).round() as usize).max(2);
    let department_popularity = PopularityWeights::new(nd, 0.9, 91);
    let manager_popularity = PopularityWeights::new(mgr_id_limit, 1.1, 92);
    let leave_popularity = PopularityWeights::new(ne, 1.2, 93);

    let pb = ProgressBar::new(5);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("[{elapsed_precise}] {bar:40.cyan/blue} {pos}/{len} {msg}")
            .unwrap(),
    );

    // 1. Departments
    crate::generate_table_parallel(
        pool,
        "departments",
        nd,
        &pb,
        "Generating departments...",
        |i| {
            let mut rng = SmallRng::seed_from_u64(i as u64);
            let name = format!("Dept-{}", i);
            let div = weighted_choice(&mut rng, &divisions, &division_weights);
            let loc = weighted_choice(&mut rng, &locations, &location_weights);
            let budget = round_to(rng.gen_range(100000.0..10000000.0), 2);
            let headcount = rng.gen_range(5..=101);
            (i as i32, name, div, loc, budget, headcount)
        },
    )?;

    // 2. Employees. Job title band determines salary band (below): only manager-eligible
    // employees (id <= mgr_id_limit) can hold Manager/Director/VP titles.
    crate::generate_table_parallel(pool, "employees", ne, &pb, "Generating employees...", |i| {
        let mut rng = SmallRng::seed_from_u64(i as u64);
        let dept_id = department_popularity.sample(&mut rng) as i32;
        let manager_id = if i <= mgr_id_limit {
            None
        } else {
            Some(manager_popularity.sample(&mut rng) as i32)
        };
        let first_name = format!("First{}", i);
        let last_name = format!("Last{}", i);
        let gender = weighted_choice(&mut rng, &genders, &gender_weights);
        let hire_date = base_date + Duration::days(rng.gen_range(0..3001));
        let title = if i <= mgr_id_limit {
            weighted_choice(&mut rng, &mgr_titles, &mgr_title_weights)
        } else {
            weighted_choice(&mut rng, &ic_titles, &ic_title_weights)
        };
        let etype = weighted_choice(&mut rng, &employment_types, &employment_type_weights);
        let active = rng.gen_bool(0.93);
        (
            i as i32, dept_id, manager_id, first_name, last_name, gender, hire_date, title, etype,
            active,
        )
    })?;

    // Materialize hire_date/job_title so salaries/reviews can derive from real tenure and
    // seniority band instead of independent random figures.
    let mut stmt = con.prepare("SELECT hire_date, job_title, manager_id FROM employees ORDER BY emp_id")?;
    let employee_facts: Vec<(NaiveDate, String, Option<i32>)> = stmt
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))?
        .collect::<Result<Vec<_>, _>>()?;

    fn band_salary_range(title: &str) -> (f64, f64) {
        match title {
            "VP" => (220000.0, 350000.0),
            "Director" => (150000.0, 220000.0),
            "Senior Manager" => (120000.0, 160000.0),
            "Manager" => (90000.0, 130000.0),
            _ => (50000.0, 95000.0), // IC1-IC5
        }
    }

    // 3. Salaries: one row at hire plus roughly one more every ~18 months of tenure
    // precomputed as a per-employee schedule rather than an unrelated flat
    // row count.
    let mut salary_assignments: Vec<(i32, NaiveDate)> = Vec::new();
    for (idx, (hire_date, _title, _mgr)) in employee_facts.iter().enumerate() {
        let emp_id = (idx + 1) as i32;
        let tenure_days = (observation_end - *hire_date).num_days().max(0);
        let n_raises = 1 + (tenure_days as f64 / (18.0 * 30.0)).floor() as i32;
        let mut eff = *hire_date;
        for _ in 0..n_raises {
            salary_assignments.push((emp_id, eff));
            eff = eff + Duration::days(540);
        }
    }
    let ns = salary_assignments.len();
    crate::generate_table_parallel(pool, "salaries", ns, &pb, "Generating salaries...", |i| {
        let mut rng = SmallRng::seed_from_u64(i as u64);
        let (emp_id, eff_date) = salary_assignments[i - 1];
        let (lo, hi) = band_salary_range(&employee_facts[(emp_id as usize) - 1].1);
        let base = round_to(rng.gen_range(lo..hi), 2);
        let bonus = round_to(base * rng.gen_range(0.0..0.2), 2);
        (i as i32, emp_id, eff_date, base, bonus, "USD")
    })?;

    // 4. Performance Reviews: ~1 per employee per year of tenure, reviewed by their actual
    // manager rather than an unrelated random employee.
    let mut review_assignments: Vec<(i32, NaiveDate, i32)> = Vec::new();
    for (idx, (hire_date, _title, manager_id)) in employee_facts.iter().enumerate() {
        let emp_id = (idx + 1) as i32;
        let reviewer_id = manager_id.unwrap_or(emp_id);
        let tenure_years = ((observation_end - *hire_date).num_days() / 365).max(1);
        for y in 1..=tenure_years.min(10) {
            let review_date = *hire_date + Duration::days(y * 365);
            if review_date <= observation_end {
                review_assignments.push((emp_id, review_date, reviewer_id));
            }
        }
    }
    let npr = review_assignments.len().max(1);
    crate::generate_table_parallel(
        pool,
        "performance_reviews",
        npr,
        &pb,
        "Generating performance reviews...",
        |i| {
            let mut rng = SmallRng::seed_from_u64(i as u64);
            let (emp_id, rev_date, reviewer_id) = review_assignments[i - 1];
            let score = round_to(rng.gen_range(1.0..5.0), 2);
            let cat = weighted_choice(&mut rng, &review_categories, &review_category_weights);
            let notes = format!("Review notes for review {}", i);
            (i as i32, emp_id, rev_date, reviewer_id, score, cat, notes)
        },
    )?;

    // 5. Leave Requests (popularity-weighted across employees)
    crate::generate_table_parallel(
        pool,
        "leave_requests",
        nlr,
        &pb,
        "Generating leave requests...",
        |i| {
            let mut rng = SmallRng::seed_from_u64(i as u64);
            let emp_id = leave_popularity.sample(&mut rng) as i32;
            let ltype = weighted_choice(&mut rng, &leave_types, &leave_type_weights);
            let start = base_date + Duration::days(rng.gen_range(0..3001));
            let end = start + Duration::days(rng.gen_range(1..31));
            let approved = rng.gen_bool(0.9);
            (i as i32, emp_id, ltype, start, end, approved)
        },
    )?;

    pb.finish_with_message("p05_hr complete");

    Ok(())
}
