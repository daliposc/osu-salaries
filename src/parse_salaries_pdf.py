import argparse
import json
import re
from pathlib import Path

import pandas as pd
from pdfminer.high_level import extract_text


def parse_salaries_data(
    in_data: str,
    out_txt: str = None,
) -> list[dict]:
    """
    Parse salaries data from input PDF/text, returning a list of faculty data.

    Args:
        in_data (str, optional): Input data. Can be a .pdf or .txt.
        out_txt(str, optional): Output text file path. Defaults to None.

    Returns:
        list: List of dictionaries with faculty data for each employee.

    Raises:
        ValueError: If neither pdf in_data and out_txt nor txt in_data is provided.
    """
    in_data = Path(in_data)
    if in_data.suffix == ".pdf" and out_txt:
        salaries_txt = extract_text(in_data)
        Path(out_txt).write_text(salaries_txt)
    elif in_data.suffix == ".txt":
        salaries_txt = Path(in_data).read_text()
    else:
        raise ValueError(
            "Either an input .pdf and output .txt, or an input .txt must be provided"
        )

    clean_salaries_txt = "\n".join(
        [
            line
            for line in salaries_txt.splitlines()
            if line.strip() and "Unclassified Personnel List" not in line
        ]
    )
    employee_blocks = re.split(r"-{80,}", clean_salaries_txt)

    return [parse_employee_block(block) for block in employee_blocks]


def parse_employee_block(block: str) -> dict:
    """
    Parse a block of text to extract employee information using regex patterns.

    Args:
        block (str): String block representing employee data.

    Returns:
        dict: Map of employee attributes (e.g., name, job title) to regex
        extracted values.
    """
    patterns = {
        "name": "Name: (.+?)\\s{2,}",
        "department": "Home Orgn: (.+?)\\s{2,}",
        "job_department": "Job Orgn: (.+?)\\s{2,}",
        "job_type": "Job Type: (.+?)\\n",
        "job_title": "Job Title: (.+?)\\s{2,}",
        "job_rank": "Rank: (.+?)\\s{2,}",
        "annual_salary": "Annual Salary Rate:\\s+(.+?)\\s{2,}",
        "first_hired_date": "First Hired: (.+?)\\n",
        "adj_service_date": "Adj Service Date: (.+?)\\n",
        "rank_start_date": "Rank Effective Date: (.+?)\\n",
        "appt_start_date": "Appt Begin Date: (.+?)\\s{2,}",
        "appt_end_date": "Appt End Date: (.+?)\\s{2,}",
        "appt_percent": "Appt Percent: (.+?)\\n",
    }

    employee_data = {}
    for key, pattern in patterns.items():
        match = re.search(pattern, block, re.DOTALL)
        employee_data[key] = match.group(1).strip() if match else None

    return employee_data


def make_pd_dataframe(
    faculty_data: list,
    estimated_gender_csv: str = None,
    department_class_map_json: str = None,
) -> pd.DataFrame:
    """
    Generate a pandas DataFrame from the given faculty data list, using
    specified data types for each column. And join `estimated_gender` data.

    Args:
        faculty_data (list): List of faculty data.
        estimated_gender_csv (str, optional): Path to estimated gender CSV file.

    Returns:
        pd.DataFrame: Resulting pandas DataFrame.
    """
    dtypes = {
        "name": "string",
        "department": "string",
        "job_department": "string",
        "job_type": "string",
        "job_title": "string",
        "job_rank": "string",
        "annual_salary": "float64",
        "first_hired_date": "datetime",
        "adj_service_date": "datetime",
        "rank_start_date": "datetime",
        "appt_start_date": "datetime",
        "appt_end_date": "datetime",
        "appt_percent": "float64",
    }

    faculty_df = pd.DataFrame(faculty_data).dropna()

    for col, dtype in dtypes.items():
        if dtype == "datetime":
            faculty_df[col] = pd.to_datetime(
                faculty_df[col], format="%d-%b-%Y", errors="coerce"
            )
        else:
            faculty_df[col] = faculty_df[col].astype(dtype)

    if estimated_gender_csv:
        gender_df = pd.read_csv(estimated_gender_csv)
        gender_df.rename(
            columns={"name": "first_name", "gender": "estimated_gender"},
            inplace=True,
        )
        faculty_df["first_name"] = (
            faculty_df["name"].str.split(", ").str[1].str.split(" ").str[0]
        )
        faculty_df = faculty_df.merge(
            gender_df[["first_name", "estimated_gender"]],
            how="left",
            on=["first_name"],
        )
        faculty_df.drop(["first_name"], axis=1, inplace=True)

    if department_class_map_json:
        with open(department_class_map_json, "r") as dept_classes:
            dept_class_map = json.load(dept_classes)
        faculty_df["dept_class"] = faculty_df["job_department"].map(dept_class_map)
        faculty_df["dept_class"] = faculty_df["dept_class"].str.upper()

    return faculty_df


def parse_args():
    parser = argparse.ArgumentParser(description="Process faculty salary data")

    parser.add_argument(
        "--input_data",
        type=str,
        help="Input file path (PDF or text)",
    )
    parser.add_argument(
        "--output_txt",
        type=str,
        help="Output text file path (if processing PDF)",
        default=None,
    )
    parser.add_argument(
        "--gender_csv",
        type=str,
        help="Path to estimated gender CSV file",
        default=None,
    )
    parser.add_argument(
        "--dept_class_json",
        type=str,
        help="Path to department class map JSON file",
        default=None,
    )

    return parser.parse_args()


def main():
    args = parse_args()
    print(args.input_data)
    if args.input_data:
        in_data = args.input_data
        out_txt = args.output_txt
        estimated_gender_csv = args.gender_csv
        department_class_map_json = args.dept_class_json
    else:
        in_data = "../data/salaries.txt"
        out_txt = "../data/salaries.txt"
        estimated_gender_csv = "../data/names_genders_USA.csv"
        department_class_map_json = "../data/depts.json"

    faculty_data = parse_salaries_data(in_data, out_txt)
    faculty_df = make_pd_dataframe(
        faculty_data, estimated_gender_csv, department_class_map_json
    )

    in_data_name = Path(in_data).stem
    faculty_df.to_csv(f"../data/{in_data_name}.csv", index=False)
    faculty_df.to_feather(f"../data/{in_data_name}.feather")
    print(faculty_df.head())
    print(faculty_df.info())


if __name__ == "__main__":
    main()
