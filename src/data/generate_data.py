# fmt: off
import random

import duckdb
import pandas as pd
from faker import Faker

fake = Faker()

# Define projects and activity ids for linking data
projects = ['PJ01', 'PJ02', 'PJ03']
activity_ids = ['CN.C1.0010','CN.C1.0020','CN.C1.0030','CN.C1.0040','CN.C1.0050','CN.C1.0060','CN.C1.0070','CN.C1.0080','CN.C1.0090','CN.C1.0100','CN.C1.0110','CN.C1.0120','CN.C2.0010','CN.C2.0020','CN.C2.0030','CN.C2.0040','CN.C2.0050','CN.C2.0060','CN.C2.0070','CN.C2.0080','CN.C2.0090','CN.C2.0100','CN.C2.0110','CN.C2.0120','CN.C3.0010','CN.C3.0020','CN.C3.0030','CN.C3.0040','CN.C3.0050','CN.C3.0060','CN.C3.0070','CN.C3.0080','CN.C3.0090','CN.C3.0100','CN.C3.0110','CN.C3.0120','CN.C4.0010','CN.C4.0020','CN.C4.0030','CN.C4.0040','CN.C4.0050','CN.C4.0060','CN.C4.0070','CN.C4.0080','CN.C4.0090','CN.C4.0100','CN.C4.0110','CN.C4.0120','EN.C1.0010','EN.C1.0020','EN.C1.0030','EN.C1.0040','EN.C1.0050','EN.C1.0060','EN.C1.0070','EN.C1.0080','EN.C2.0010','EN.C2.0020','EN.C2.0030','EN.C2.0040','EN.C2.0050','EN.C2.0060','EN.C2.0070','EN.C2.0080','EN.C3.0010','EN.C3.0020','EN.C3.0030','EN.C3.0040','EN.C3.0050','EN.C3.0060','EN.C3.0070','EN.C3.0080','EN.C4.0010','EN.C4.0020','EN.C4.0030','EN.C4.0040','EN.C4.0050','EN.C4.0060','EN.C4.0070','EN.C4.0080','EN.GE.0010','EN.GE.0020','EN.GE.0030','MS.C1.0030','MS.C2.0040','MS.C3.0050','MS.C4.0060','MS.GE.0010','MS.GE.0020','PC.GE.0010','PC.GE.0020','PC.GE.0030','PC.GE.0040','PC.GE.0050','PC.GE.0060','PC.GE.0070','PC.GE.0080','PC.GE.0090','TS.C1.0010','TS.C1.0020','TS.C1.0030','TS.C1.0040','TS.C1.0050','TS.C2.0010','TS.C2.0020','TS.C2.0030','TS.C2.0040','TS.C2.0050','TS.C3.0010','TS.C3.0020','TS.C3.0030','TS.C3.0040','TS.C3.0050','TS.C4.0010','TS.C4.0020','TS.C4.0030','TS.C4.0040','TS.C4.0050']
wbs_codes = ['CASA.UD.[AUG23].CN.C1.C&P','CASA.UD.[AUG23].CN.C1.CIV','CASA.UD.[AUG23].CN.C1.ELC','CASA.UD.[AUG23].CN.C1.MEC','CASA.UD.[AUG23].CN.C1.MEP','CASA.UD.[AUG23].CN.C2.C&P','CASA.UD.[AUG23].CN.C2.CIV','CASA.UD.[AUG23].CN.C2.ELC','CASA.UD.[AUG23].CN.C2.MEC','CASA.UD.[AUG23].CN.C2.MEP','CASA.UD.[AUG23].CN.C3.C&P','CASA.UD.[AUG23].CN.C3.CIV','CASA.UD.[AUG23].CN.C3.ELC','CASA.UD.[AUG23].CN.C3.MEC','CASA.UD.[AUG23].CN.C3.MEP','CASA.UD.[AUG23].CN.C4.C&P','CASA.UD.[AUG23].CN.C4.CIV','CASA.UD.[AUG23].CN.C4.ELC','CASA.UD.[AUG23].CN.C4.MEC','CASA.UD.[AUG23].CN.C4.MEP','CASA.UD.[AUG23].EN.C1.CIV','CASA.UD.[AUG23].EN.C1.ELC','CASA.UD.[AUG23].EN.C1.MEC','CASA.UD.[AUG23].EN.C1.MEP','CASA.UD.[AUG23].EN.C2.CIV','CASA.UD.[AUG23].EN.C2.ELC','CASA.UD.[AUG23].EN.C2.MEC','CASA.UD.[AUG23].EN.C2.MEP','CASA.UD.[AUG23].EN.C3.CIV','CASA.UD.[AUG23].EN.C3.ELC','CASA.UD.[AUG23].EN.C3.MEC','CASA.UD.[AUG23].EN.C3.MEP','CASA.UD.[AUG23].EN.C4.CIV','CASA.UD.[AUG23].EN.C4.ELC','CASA.UD.[AUG23].EN.C4.MEC','CASA.UD.[AUG23].EN.C4.MEP','CASA.UD.[AUG23].EN.GE','CASA.UD.[AUG23].MS.OA','CASA.UD.[AUG23].MS.ST','CASA.UD.[AUG23].PC','CASA.UD.[AUG23].TS.C1.C&P','CASA.UD.[AUG23].TS.C1.ELC','CASA.UD.[AUG23].TS.C1.MEC','CASA.UD.[AUG23].TS.C1.MEP','CASA.UD.[AUG23].TS.C2.C&P','CASA.UD.[AUG23].TS.C2.ELC','CASA.UD.[AUG23].TS.C2.MEC','CASA.UD.[AUG23].TS.C2.MEP','CASA.UD.[AUG23].TS.C3.C&P','CASA.UD.[AUG23].TS.C3.ELC','CASA.UD.[AUG23].TS.C3.MEC','CASA.UD.[AUG23].TS.C3.MEP','CASA.UD.[AUG23].TS.C4.C&P','CASA.UD.[AUG23].TS.C4.ELC','CASA.UD.[AUG23].TS.C4.MEC','CASA.UD.[AUG23].TS.C4.MEP','OMEGA.UD.[AUG23].CN.C1.C&P','OMEGA.UD.[AUG23].CN.C1.CIV','OMEGA.UD.[AUG23].CN.C1.ELC','OMEGA.UD.[AUG23].CN.C1.MEC','OMEGA.UD.[AUG23].CN.C1.MEP','OMEGA.UD.[AUG23].CN.C2.C&P','OMEGA.UD.[AUG23].CN.C2.CIV','OMEGA.UD.[AUG23].CN.C2.ELC','OMEGA.UD.[AUG23].CN.C2.MEC','OMEGA.UD.[AUG23].CN.C2.MEP','OMEGA.UD.[AUG23].CN.C3.C&P','OMEGA.UD.[AUG23].CN.C3.CIV','OMEGA.UD.[AUG23].CN.C3.ELC','OMEGA.UD.[AUG23].CN.C3.MEC','OMEGA.UD.[AUG23].CN.C3.MEP','OMEGA.UD.[AUG23].CN.C4.C&P','OMEGA.UD.[AUG23].CN.C4.CIV','OMEGA.UD.[AUG23].CN.C4.ELC','OMEGA.UD.[AUG23].CN.C4.MEC','OMEGA.UD.[AUG23].CN.C4.MEP','OMEGA.UD.[AUG23].EN.C1.CIV','OMEGA.UD.[AUG23].EN.C1.ELC','OMEGA.UD.[AUG23].EN.C1.MEC','OMEGA.UD.[AUG23].EN.C1.MEP','OMEGA.UD.[AUG23].EN.C2.CIV','OMEGA.UD.[AUG23].EN.C2.ELC','OMEGA.UD.[AUG23].EN.C2.MEC','OMEGA.UD.[AUG23].EN.C2.MEP','OMEGA.UD.[AUG23].EN.C3.CIV','OMEGA.UD.[AUG23].EN.C3.ELC','OMEGA.UD.[AUG23].EN.C3.MEC','OMEGA.UD.[AUG23].EN.C3.MEP','OMEGA.UD.[AUG23].EN.C4.CIV','OMEGA.UD.[AUG23].EN.C4.ELC','OMEGA.UD.[AUG23].EN.C4.MEC','OMEGA.UD.[AUG23].EN.C4.MEP','OMEGA.UD.[AUG23].EN.GE','OMEGA.UD.[AUG23].MS.OA','OMEGA.UD.[AUG23].MS.ST','OMEGA.UD.[AUG23].PC','OMEGA.UD.[AUG23].TS.C1.C&P','OMEGA.UD.[AUG23].TS.C1.ELC','OMEGA.UD.[AUG23].TS.C1.MEC','OMEGA.UD.[AUG23].TS.C1.MEP','OMEGA.UD.[AUG23].TS.C2.C&P','OMEGA.UD.[AUG23].TS.C2.ELC','OMEGA.UD.[AUG23].TS.C2.MEC','OMEGA.UD.[AUG23].TS.C2.MEP','OMEGA.UD.[AUG23].TS.C3.C&P','OMEGA.UD.[AUG23].TS.C3.ELC','OMEGA.UD.[AUG23].TS.C3.MEC','OMEGA.UD.[AUG23].TS.C3.MEP','OMEGA.UD.[AUG23].TS.C4.C&P','OMEGA.UD.[AUG23].TS.C4.ELC','OMEGA.UD.[AUG23].TS.C4.MEC','OMEGA.UD.[AUG23].TS.C4.MEP','RIO.UD.[AUG23].CN.C1.C&P','RIO.UD.[AUG23].CN.C1.CIV','RIO.UD.[AUG23].CN.C1.ELC','RIO.UD.[AUG23].CN.C1.MEC','RIO.UD.[AUG23].CN.C1.MEP','RIO.UD.[AUG23].CN.C2.C&P','RIO.UD.[AUG23].CN.C2.CIV','RIO.UD.[AUG23].CN.C2.ELC','RIO.UD.[AUG23].CN.C2.MEC','RIO.UD.[AUG23].CN.C2.MEP','RIO.UD.[AUG23].CN.C3.C&P','RIO.UD.[AUG23].CN.C3.CIV','RIO.UD.[AUG23].CN.C3.ELC','RIO.UD.[AUG23].CN.C3.MEC','RIO.UD.[AUG23].CN.C3.MEP','RIO.UD.[AUG23].CN.C4.C&P','RIO.UD.[AUG23].CN.C4.CIV','RIO.UD.[AUG23].CN.C4.ELC','RIO.UD.[AUG23].CN.C4.MEC','RIO.UD.[AUG23].CN.C4.MEP','RIO.UD.[AUG23].EN.C1.CIV','RIO.UD.[AUG23].EN.C1.ELC','RIO.UD.[AUG23].EN.C1.MEC','RIO.UD.[AUG23].EN.C1.MEP','RIO.UD.[AUG23].EN.C2.CIV','RIO.UD.[AUG23].EN.C2.ELC','RIO.UD.[AUG23].EN.C2.MEC','RIO.UD.[AUG23].EN.C2.MEP','RIO.UD.[AUG23].EN.C3.CIV','RIO.UD.[AUG23].EN.C3.ELC','RIO.UD.[AUG23].EN.C3.MEC','RIO.UD.[AUG23].EN.C3.MEP','RIO.UD.[AUG23].EN.C4.CIV','RIO.UD.[AUG23].EN.C4.ELC','RIO.UD.[AUG23].EN.C4.MEC','RIO.UD.[AUG23].EN.C4.MEP','RIO.UD.[AUG23].EN.GE','RIO.UD.[AUG23].MS.OA','RIO.UD.[AUG23].MS.ST','RIO.UD.[AUG23].PC','RIO.UD.[AUG23].TS.C1.C&P','RIO.UD.[AUG23].TS.C1.ELC','RIO.UD.[AUG23].TS.C1.MEC','RIO.UD.[AUG23].TS.C1.MEP','RIO.UD.[AUG23].TS.C2.C&P','RIO.UD.[AUG23].TS.C2.ELC','RIO.UD.[AUG23].TS.C2.MEC','RIO.UD.[AUG23].TS.C2.MEP','RIO.UD.[AUG23].TS.C3.C&P','RIO.UD.[AUG23].TS.C3.ELC','RIO.UD.[AUG23].TS.C3.MEC','RIO.UD.[AUG23].TS.C3.MEP','RIO.UD.[AUG23].TS.C4.C&P','RIO.UD.[AUG23].TS.C4.ELC','RIO.UD.[AUG23].TS.C4.MEC','RIO.UD.[AUG23].TS.C4.MEP']
jobs = [
    'Accounts Payable Clerk',
    'Architect',
    'Bid Manager',
    'Bricklayer',
    'Building Inspector',
    'Carpenter',
    'Civil Engineer',
    'Concrete Finisher',
    'Concrete Pump Operator',
    'Construction Manager',
    'Contracts Manager',
    'Crane Operator',
    'Demolition Worker',
    'Document Controller',
    'Driller',
    'Drywall Installer',
    'Electrician',
    'Estimator',
    'Finance Manager',
    'Foreman',
    'Geotechnical Engineer',
    'Glazier',
    'Heavy Equipment Operator',
    'Health and Safety Officer',
    'HR Manager',
    'HVAC Technician',
    'Joiner',
    'Landscape Architect',
    'Mason',
    'Mechanical Engineer',
    'Office Administrator',
    'Office Manager',
    'Painter and Decorator',
    'Piling Operative',
    'Planning Engineer',
    'Plasterer',
    'Plumber',
    'Procurement Officer',
    'Project Coordinator',
    'Project Manager',
    'Quantity Surveyor',
    'Roofer',
    'Roofing Labourer',
    'Safety Officer',
    'Scaffolder',
    'Site Engineer',
    'Site Supervisor',
    'Steel Erector',
    'Structural Engineer',
    'Surveyor',
    'Tiler',
    'Traffic Marshal',
    'Welder',
]

def generate_cost_accounts_data(num_records=100000):
    data = []
    for _ in range(num_records):
        project_id = random.choice(projects)
        wbs_code = random.choice(wbs_codes)
        activity_id = random.choice(activity_ids)
        cost_account_id = fake.bothify(text="CA-????-####")
        cost_account_name = random.choice(jobs)  # Random cost account names like job descriptions
        budget = round(random.uniform(1000, 10000), 2)
        actual_cost = round(random.uniform(5000, budget), 2)
        committed_cost = round(random.uniform(0, budget), 2)
        earned_value = round(random.uniform(0, budget), 2)
        planned_value = round(random.uniform(0, budget), 2)
        cost_date = fake.date_between(start_date="-2y", end_date="today")

        data.append({
            "Project ID": project_id,
            "WBS Code": wbs_code,
            "Activity ID": activity_id,
            "Cost Account ID": cost_account_id,
            "Cost Account Name": cost_account_name,
            "Budget": budget,
            "Actual Cost": actual_cost,
            "Committed Cost": committed_cost,
            "Earned Value": earned_value,
            "Planned Value": planned_value,
            "Cost Date": cost_date,
        })

    df = pd.DataFrame(data)
    return df

def generate_expenses_data(num_records=50000):
    data = []
    for _ in range(num_records):
        project_id = random.choice(projects)
        activity_id = random.choice(activity_ids)
        expense_id = fake.bothify(text="EXP-####-???")
        expense_type = fake.random_element(elements=("Travel", "Consulting", "Facilities", "Training"))
        expense_description = f"{expense_type} - {fake.catch_phrase()}"
        budget = round(random.uniform(100, 5000), 2)
        actual_expense = round(random.uniform(0, budget), 2)
        remaining_cost = round(random.uniform(0, budget - actual_expense), 2)
        expense_date = fake.date_between(start_date="-2y", end_date="today")

        data.append({
            "Project ID": project_id,
            "Activity ID": activity_id,
            "Expense ID": expense_id,
            "Expense Type": expense_type,
            "Expense Description": expense_description,
            "Budget": budget,
            "Actual Expense": actual_expense,
            "Remaining Cost": remaining_cost,
            "Expense Date": expense_date,
        })

    df = pd.DataFrame(data)
    return df

def add_duplicates(df: pd.DataFrame, number_of_rows: int = 100) -> pd.DataFrame:
    """Add duplicate rows to DataFrame."""
    return duckdb.sql(f"""
                      with df_limit as (
                        select * from df
                        limit {number_of_rows}
                      )
                      select * from df
                      union all
                      select * from df_limit
                      """).df()


# Generate datasets
_cost_accounts_df = generate_cost_accounts_data(num_records=100000)
_expenses_df = generate_expenses_data(num_records=50000)

cost_accounts_df = add_duplicates(_cost_accounts_df)
expenses_df = add_duplicates(_expenses_df)

# Save the datasets as CSV for review
cost_accounts_df.to_csv("src/data/p6/cost_accounts.csv", index=False)
expenses_df.to_csv("src/data/p6/expenses.csv", index=False)

print("Cost Accounts and Expenses datasets generated successfully.")
