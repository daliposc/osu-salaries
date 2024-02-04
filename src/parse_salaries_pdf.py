import re
from pathlib import Path

import pandas as pd
from pdfminer.high_level import extract_text


def parse_salaries_data(
    in_pdf: str = None, out_txt: str = None, in_txt: str = None
) -> list:
    """
    Parse salaries data from input PDF/text, returning a list of faculty data.

    Args:
        in_pdf (str, optional): Input PDF file path. Defaults to None.
        out_txt(str, optional): Output text file path. Defaults to None.
        in_txt (str,optional): Input text file path. Defaults to None.

    Returns:
        list: List of dictionaries with faculty data for each employee.

    Raises:
        ValueError: If neither in_pdf and out_txt nor in_txt is provided.
    """
    if in_pdf and out_txt:
        salaries_txt = extract_text(in_pdf)
        Path(out_txt).write_text(salaries_txt)
    elif in_txt:
        salaries_txt = Path(in_txt).read_text()
    else:
        raise ValueError("Either in_pdf and out_txt or in_txt must be provided")

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
    faculty_data: list, estimated_gender_csv: str = None
) -> pd.DataFrame:
    """
    Generate a pandas DataFrame from the given faculty data list, using
    specified data types for each column. And join `estimated_gender` data.

    Args:
        faculty_data (list): List of faculty data. estimated_gender_csv (str,
        optional): Path to estimated gender CSV file.

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

    return faculty_df


def main():
    # faculty_data = salaries_pdf_to_txt(
    #     in_pdf="../data/salaries.pdf",
    #     out_txt="../data/salaries.txt",
    # )
    faculty_data = parse_salaries_data(in_txt="../data/salaries.txt")
    faculty_df = make_pd_dataframe(
        faculty_data, estimated_gender_csv="../data/names_genders_USA.csv"
    )

    faculty_df.to_csv("../data/salaries.csv", index=False)
    faculty_df.to_feather("../data/salaries.feather")

    print(faculty_df.head())
    print(faculty_df.info())


if __name__ == "__main__":
    main()
