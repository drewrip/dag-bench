use chrono::{Duration, NaiveDate};
use duckdb::Connection;
use indicatif::{ProgressBar, ProgressStyle};
use rand::prelude::*;
use rand::rngs::SmallRng;

pub fn run(sf: f64, con: &mut Connection) -> duckdb::Result<()> {
    let sf_adj = sf * 8989.0;
    let nd = (30.0 * sf_adj).max(5.0) as usize;
    let ne = (800.0 * sf_adj).max(20.0) as usize;
    let ns = (2000.0 * sf_adj).max(30.0) as usize;
    let npr = (1600.0 * sf_adj).max(20.0) as usize;
    let nlr = (1200.0 * sf_adj).max(10.0) as usize;

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
    let divs = [
        "Engineering",
        "Sales",
        "Operations",
        "Finance",
        "Marketing",
        "HR",
    ];
    let locs = ["NYC", "London", "Berlin", "Tokyo", "Sydney", "Toronto"];
    let ttitles = [
        "Engineer",
        "Manager",
        "Director",
        "Analyst",
        "Specialist",
        "Lead",
        "VP",
        "C-Suite",
    ];
    let etypes = ["full_time", "part_time", "contractor"];
    let ltypes = ["annual", "sick", "parental", "unpaid", "bereavement"];
    let cats = [
        "technical",
        "leadership",
        "communication",
        "teamwork",
        "delivery",
    ];
    let genders = ["M", "F", "NB"];

    let mgr_id_limit = (ne / 10).max(2);

    let pb = ProgressBar::new(5);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("[{elapsed_precise}] {bar:40.cyan/blue} {pos}/{len} {msg}")
            .unwrap(),
    );

    // 1. Departments
    crate::generate_table_parallel(
        con,
        "departments",
        nd,
        &pb,
        "Generating departments...",
        |i| {
            let mut rng = SmallRng::seed_from_u64(i as u64);
            let name = format!("Dept-{}", i);
            let div = divs[rng.gen_range(0..divs.len())];
            let loc = locs[rng.gen_range(0..locs.len())];
            let budget = ((rng.gen_range(100000.0..10000000.0) * 100.0) as f64).round() / 100.0;
            let headcount = rng.gen_range(5..=101);
            (i as i32, name, div, loc, budget, headcount)
        },
    )?;

    // 2. Employees
    crate::generate_table_parallel(con, "employees", ne, &pb, "Generating employees...", |i| {
        let mut rng = SmallRng::seed_from_u64(i as u64);
        let dept_id = rng.gen_range(1..=nd) as i32;
        let manager_id = if i <= mgr_id_limit {
            None
        } else {
            Some(rng.gen_range(1..=mgr_id_limit) as i32)
        };
        let first_name = format!("First{}", i);
        let last_name = format!("Last{}", i);
        let gender = genders[rng.gen_range(0..genders.len())];
        let hire_date = base_date + Duration::days(rng.gen_range(0..3001));
        let title = ttitles[rng.gen_range(0..ttitles.len())];
        let etype = etypes[rng.gen_range(0..etypes.len())];
        let active = rng.gen_bool(0.93);
        (
            i as i32, dept_id, manager_id, first_name, last_name, gender, hire_date, title, etype,
            active,
        )
    })?;

    // 3. Salaries
    crate::generate_table_parallel(con, "salaries", ns, &pb, "Generating salaries...", |i| {
        let mut rng = SmallRng::seed_from_u64(i as u64);
        let emp_id = rng.gen_range(1..=ne) as i32;
        let eff_date = base_date + Duration::days(rng.gen_range(0..3001));
        let base = ((rng.gen_range(30000.0..300000.0) * 100.0) as f64).round() / 100.0;
        let bonus = ((rng.gen_range(0.0..50000.0) * 100.0) as f64).round() / 100.0;
        (i as i32, emp_id, eff_date, base, bonus, "USD")
    })?;

    // 4. Performance Reviews
    crate::generate_table_parallel(
        con,
        "performance_reviews",
        npr,
        &pb,
        "Generating performance reviews...",
        |i| {
            let mut rng = SmallRng::seed_from_u64(i as u64);
            let emp_id = rng.gen_range(1..=ne) as i32;
            let rev_date = base_date + Duration::days(rng.gen_range(365..3651));
            let reviewer_id = rng.gen_range(1..=ne) as i32;
            let score = ((rng.gen_range(1.0..5.0) * 100.0) as f64).round() / 100.0;
            let cat = cats[rng.gen_range(0..cats.len())];
            let notes = format!("Review notes for review {}", i);
            (i as i32, emp_id, rev_date, reviewer_id, score, cat, notes)
        },
    )?;

    // 5. Leave Requests
    crate::generate_table_parallel(
        con,
        "leave_requests",
        nlr,
        &pb,
        "Generating leave requests...",
        |i| {
            let mut rng = SmallRng::seed_from_u64(i as u64);
            let emp_id = rng.gen_range(1..=ne) as i32;
            let ltype = ltypes[rng.gen_range(0..ltypes.len())];
            let start = base_date + Duration::days(rng.gen_range(0..3001));
            let end = start + Duration::days(rng.gen_range(1..31));
            let approved = rng.gen_bool(0.9);
            (i as i32, emp_id, ltype, start, end, approved)
        },
    )?;

    pb.finish_with_message("p04_hr complete");

    Ok(())
}
