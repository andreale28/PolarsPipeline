import os
from datetime import datetime, timedelta


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


# # Example usage
# base_path = "data/log_content/"
# start_date = "20220401"
# end_date = "20220430"
# directory_paths = create_date_directories(base_path, start_date, end_date)
#
# print("Created directories:")
# for path in directory_paths:
#     print(path)
