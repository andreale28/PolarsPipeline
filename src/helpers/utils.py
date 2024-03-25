import os
from datetime import datetime, timedelta
from itertools import product


def create_date_directories(base_path, start_date, end_date, format="%Y%m%d"):
    """
    Creates a list of directories for each date between start_date and end_date, inclusive.

    Args:
        base_path: The base path where directories will be created.
        start_date: The start date as a string in the specified format.
        end_date: The end date as a string in the specified format.
        format: The date format string to use (default is YYYY-MM-DD).

    Returns:
        A list of directory paths created.
    """

    # Convert dates to datetime objects
    try:
        start_date = datetime.strptime(start_date, format).date()
        end_date = datetime.strptime(end_date, format).date()
    except ValueError:
        raise ValueError(f"Invalid date format. Please use '{format}'")

    # Check if start date is before end date
    if start_date > end_date:
        raise ValueError("Start date must be before end date.")

    # Create list to store directory paths
    directory_paths = []

    # Iterate through dates and create directories
    current_date = start_date
    while current_date <= end_date:
        # Format date string
        date_str = current_date.strftime(format)
        # Create directory path
        directory_path = os.path.join(base_path, f"{date_str}.json")
        # Add path to list
        directory_paths.append(directory_path)
        # Increment date
        current_date += timedelta(days=1)

    return directory_paths


def all_combinations_with_replacement_iterative(numbers):
    """
    This function generates all possible combinations of elements in a list,
    including replacements, using an iterative approach.

    Args:
        numbers (List[int]): A list of numbers.

    Returns:
        List[List[int]]: A list of lists, where each inner list represents
                         a combination of elements from the input list.
    """
    combinations = []
    for length in range(1, len(numbers) + 1):
        if length != len(numbers):
            pass
        else:
            combinations.extend(
                product(numbers, repeat=length)
            )  # Use product for efficient generation
    return combinations
