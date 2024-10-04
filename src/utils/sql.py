import re

import inflection
import pyperclip


def process_header(header: str) -> str:
    # Split the string by either tabs or 2/4 consecutive spaces
    fields = re.split(r"\t|\s{4}|\s{2}", header)

    processed_columns = []

    for field in fields:
        # Step 1: Remove special characters and convert to lowercase
        # Replace '&' with 'percent', remove parentheses, and convert spaces to underscores
        clean_field = field.replace("&", "percent")
        clean_field = re.sub(
            r"[\(\)\[\]]", "", clean_field
        )  # Remove brackets and parentheses
        clean_field = re.sub(
            r"\s+", "_", clean_field
        )  # Replace spaces with underscores
        clean_field = clean_field.lower()

        # Step 2: Convert the cleaned field name to snake_case using inflection
        snake_case_field = inflection.underscore(clean_field)

        # Step 3: Add SQL alias formatting
        processed_columns.append(f'"{field}" as {snake_case_field}')

    # Join the processed columns with newlines and leading commas for SQL formatting
    result = "\n, ".join(processed_columns)

    # Copy the result to the clipboard
    pyperclip.copy(result)

    # Return the result for optional printing or further use
    return result


# Example usage
# header_string = "Project ID	Trade	Life Cycle	Concern Description	Responsibility	Task Owner	Notice Date	Concern Impact	Required Action	Concern Status"
# sql_friendly_columns = process_header(header_string)

# # Print result for verification
# print(sql_friendly_columns)
