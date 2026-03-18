import csv
from collections import defaultdict


def count_unique_companies_by_section(csv_file='regdocs_full.csv'):
    """
    Read regdocs CSV and count unique Company_URL values for each Root_Section.

    Args:
        csv_file (str): Path to the CSV file
    """
    # Dictionary to store unique Company_URLs for each Root_Section
    section_companies = defaultdict(set)

    try:
        with open(csv_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)

            for row in reader:
                root_section = row.get('Root_Section', '').strip()
                company_url = row.get('Company_URL', '').strip()

                # Only count non-empty values
                if root_section and company_url:
                    section_companies[root_section].add(company_url)

        # Print results
        print("=" * 80)
        print("Unique Company_URL counts by Root_Section")
        print("=" * 80)
        print()

        # Sort by Root_Section for better readability
        for root_section in sorted(section_companies.keys()):
            unique_count = len(section_companies[root_section])
            print(f"{root_section}: {unique_count} unique Company_URL(s)")

        print()
        print("=" * 80)
        print(f"Total Root_Sections: {len(section_companies)}")
        print("=" * 80)

    except FileNotFoundError:
        print(f"Error: CSV file '{csv_file}' not found.")
    except Exception as e:
        print(f"Error processing CSV file: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    count_unique_companies_by_section('regdocs_full.csv')