import pandas as pd
import numpy as np
import re
import os
from google.colab import files
import io # Import io for handling file handling
from datetime import datetime # Import datetime

# --- Configuration ---
# Strictly use the explicit country column
country_col = 'What Country are you located in ?'
# Updated preference column name (ensure this matches your exact header, including the colon)
preference_col = 'Grow with Google Career Preference:'
original_sheet_output_name = 'Original Data' # Name for the sheet containing original data
unknown_sheet_output_name = 'Unknown Region' # Name for the sheet containing unknown region data

# --- Define Comprehensive Regional Grouping Logic with Country Lists ---

# Define sets of countries for each region for faster lookup
# These lists are compiled from general geographical and business groupings and may vary slightly
# depending on the specific definition used by different organizations.
# Includes standard names and some common alternative forms or associated territories.

EMEAI_COUNTRIES = set([
    # Europe
    'Albania', 'Andorra', 'Armenia', 'Austria', 'Azerbaijan', 'Belarus', 'Belgium', 'Bosnia and Herzegovina',
    'Bulgaria', 'Croatia', 'Cyprus', 'Czech Republic', 'Czechia', 'Denmark', 'Estonia', 'Faroe Islands', 'Finland',
    'France', 'Georgia', 'Germany', 'Gibraltar', 'Greece', 'Guernsey', 'Hungary', 'Iceland', 'Ireland',
    'Isle of Man', 'Italy', 'Jersey', 'Kazakhstan', 'Kosovo', 'Kyrgyzstan', 'Latvia', 'Liechtenstein',
    'Lithuania', 'Luxembourg', 'Malta', 'Moldova', 'Republic of Moldova', 'Monaco', 'Montenegro', 'Netherlands',
    'North Macedonia', 'Macedonia', 'Norway', 'Poland', 'Portugal', 'Romania', 'Russia', 'Russian Federation',
    'San Marino', 'Serbia', 'Slovakia', 'Slovenia', 'Spain', 'Sweden', 'Switzerland', 'Tajikistan', 'Turkey',
    'Türkiye', 'Turkmenistan', 'Ukraine', 'United Kingdom', 'UK', 'Great Britain', 'Uzbekistan', 'Vatican City',

    # Middle East
    'Bahrain', 'Iran', 'Islamic Republic of Iran', 'Iraq', 'Israel', 'Jordan', 'Hashemite Kingdom of Jordan',
    'Kuwait', 'Lebanon', 'Oman', 'Palestine', 'State of Palestine', 'Palestinian Territories', 'Qatar',
    'Saudi Arabia', 'Syria', 'Syrian Arab Republic', 'United Arab Emirates', 'UAE', 'Yemen',

    # Africa
    'Algeria', 'Angola', 'Benin', 'Botswana', 'Burkina Faso', 'Burundi', 'Cabo Verde', 'Cape Verde', 'Cameroon',
    'Central African Republic', 'Chad', 'Comoros', 'Congo (Brazzaville)', 'Republic of the Congo',
    'Congo (Kinshasa)', 'Democratic Republic of the Congo', 'DR Congo', "Cote d'Ivoire", 'Ivory Coast',
    'Djibouti', 'Egypt', 'Equatorial Guinea', 'Eritrea', 'Eswatini', 'Swaziland', 'Ethiopia', 'Gabon', 'Gambia',
    'Ghana', 'Guinea', 'Guinea-Bissau', 'Kenya', 'Lesotho', 'Liberia', 'Libya', 'Madagascar', 'Malawi',
    'Mali', 'Mauritania', 'Mauritius', 'Mayotte', 'Morocco', 'Mozambique', 'Namibia', 'Niger', 'Nigeria',
    'Rwanda', 'Sao Tome and Principe', 'Senegal', 'Seychelles', 'Sierra Leone', 'Somalia', 'South Africa',
    'South Sudan', 'Sudan', 'Tanzania', 'United Republic of Tanzania', 'Togo', 'Tunisia', 'Uganda',
    'Western Sahara', 'Zambia', 'Zimbabwe',

    # India
    'India',

    # Added British Indian Ocean Territory to EMEAI
    'British Indian Ocean Territory'
])

AMER_COUNTRIES = set([
   # North America
    'Canada', 'Mexico', 'United States', 'USA', 'U.S.',

    # Central America
    'Belize', 'Costa Rica', 'El Salvador', 'Guatemala', 'Honduras', 'Nicaragua', 'Panama',

    # Caribbean
    'Anguilla', 'Antigua and Barbuda', 'Aruba', 'Bahamas', 'Barbados', 'Bermuda', 'Bonaire, Sint Eustatius and Saba',
    'British Virgin Islands', 'BVI', 'Cayman Islands', 'Cuba', 'Curaçao', 'Dominica', 'Dominican Republic', 'Grenada',
    'Guadeloupe', 'Haiti', 'Jamaica', 'Martinique', 'Montserrat', 'Puerto Rico', 'Saint Barthelemy', 'St. Barts',
    'St. Barth', 'Saint Kitts and Nevis', 'Saint Lucia', 'Saint Martin (French part)', 'Saint Pierre and Miquelon',
    'Saint Vincent and the Grenadines', 'Sint Maarten (Dutch part)', 'Trinidad and Tobago', 'Turks and Caicos Islands',
    'United States Virgin Islands', 'USVI',

    # South America
    'Argentina', 'Bolivia', 'Plurinational State of Bolivia', 'Brazil', 'Chile', 'Colombia', 'Ecuador',
    'Falkland Islands', 'French Guiana', 'Guyana', 'Paraguay', 'Peru', 'Suriname', 'Uruguay',
    'Venezuela', 'Bolivarian Republic of Venezuela'
])

APAC_COUNTRIES = set([
     # East Asia
    'China', 'People\'s Republic of China', 'PRC', 'Hong Kong', 'Japan', 'Macao', 'Macau', 'Mongolia',
    'North Korea', 'Democratic People\'s Republic of Korea', 'DPRK', 'South Korea', 'Republic of Korea', 'ROK',
    'Taiwan', 'Republic of China',

    # Southeast Asia
    'Brunei', 'Brunei Darussalam', 'Cambodia', 'Indonesia', 'Laos', 'Lao People\'s Democratic Republic',
    'Malaysia', 'Myanmar', 'Burma', 'Philippines', 'Singapore', 'Thailand', 'Timor-Leste', 'East Timor',
    'Vietnam', 'Viet Nam',

    # Australasia
    'Australia', 'New Zealand',

    # Pacific Islands (Oceania)
    'American Samoa', 'Cook Islands', 'Federated States of Micronesia', 'Fiji', 'French Polynesia',
    'Guam', 'Kiribati', 'Marshall Islands', 'Nauru', 'New Caledonia', 'Niue', 'Northern Mariana Islands',
    'Palau', 'Papua New Guinea', 'Pitcairn Islands', 'Samoa', 'Solomon Islands', 'Tokelau', 'Tonga',
    'Tuvalu', 'Vanuatu', 'Wallis and Futuna',

    # South Asia (excluding India, which is in EMEAI in this definition)
    'Afghanistan', 'Bangladesh', 'Bhutan', 'Maldives', 'Nepal', 'Pakistan', 'Sri Lanka',

    # North Asia (often included, primarily the Asian part of Russia)
    # Note: Russia is also listed in EMEAI. Depending on the specific regional
    # definition, Russia might be considered entirely in EMEAI, entirely in APAC,
    # or split. For consistency with the EMEAI list above, we will not duplicate
    # Russia here, assuming it's covered in EMEAI. If your definition requires
    # the Asian part of Russia in APAC, you would add 'Russia' here as well.
])


def assign_region_by_country(country):
    if pd.isna(country):
        return 'Unknown'
    # Remove leading/trailing spaces and convert to lowercase for robust matching
    country_lower = str(country).strip().lower()

    if country_lower in (c.lower() for c in EMEAI_COUNTRIES):
        return 'EMEAI'
    elif country_lower in (c.lower() for c in AMER_COUNTRIES):
        return 'AMER'
    elif country_lower in (c.lower() for c in APAC_COUNTRIES):
        return 'APAC'
    else:
        return 'Unknown'

print("Please upload your Excel file (e.g., applicants.xlsx) using the button below:")
uploaded = files.upload()

if not uploaded:
    print("\nERROR: No file selected. Please run the cell again and select a file.")
    raise SystemExit("File upload cancelled.")

input_excel_file = list(uploaded.keys())[0]
print(f'\nSuccessfully uploaded file: "{input_excel_file}" ({len(uploaded[input_excel_file])} bytes)')

# --- Generate filenames with timestamp ---
current_date = datetime.now().strftime("%Y%m%d_%H%M%S") # Added time for uniqueness
base_name = os.path.splitext(input_excel_file)[0]
output_excel_file = f"{base_name}_{current_date}_sorted.xlsx" # Use base_name and timestamp
# Define the Google Sheet name based on the base name and timestamp
GOOGLE_SHEET_NAME = f"{base_name}_{current_date}_evaluated" # Added evaluated suffix

print(f"Output Excel file will be saved as: '{output_excel_file}'")
print(f"Suggested Google Sheet name upon upload: '{GOOGLE_SHEET_NAME}'")


# --- Detect available sheets ---
print("\nChecking available sheets in the Excel file...")
xls = pd.ExcelFile(input_excel_file)
sheet_names = xls.sheet_names
print("Available sheets:", sheet_names)

if not sheet_names:
    print("ERROR: No sheets found in the Excel file.")
    raise SystemExit
else:
    original_sheet_name = sheet_names[0]
    print(f"\nUsing first sheet: '{original_sheet_name}'")

# --- Load the Data ---
print(f"\nLoading data from sheet '{original_sheet_name}'...")
try:
    # Load the data, ensuring country and preference columns are treated as strings
    # Load the original data into a separate DataFrame
    df_original = pd.read_excel(input_excel_file, sheet_name=original_sheet_name, dtype=str)
    print("Original data loaded successfully.")
    print("\nFirst few rows of original data:")
    print(df_original.head())
    print(f"\nTotal rows loaded: {len(df_original)}") # Print total rows

    # Create a copy for processing to add the Region column
    df_processed = df_original.copy()

    # --- Check if the required columns exist in the processed DataFrame ---
    if country_col not in df_processed.columns:
        print(f"\nERROR: Required country column '{country_col}' not found in the Excel file.")
        print("Please ensure the column header matches exactly.")
        # Print available columns to help the user identify the correct one
        print("Available columns are:", df_processed.columns.tolist())
        raise SystemExit("Country column missing.")

    if preference_col not in df_processed.columns:
        print(f"\nERROR: Required preference column '{preference_col}' not found in the Excel file.")
        print("Please ensure the column header matches exactly.")
        # Print available columns to help the user identify the correct one
        print("Available columns are:", df_processed.columns.tolist())
        raise SystemExit("Preference column missing.")

except Exception as e:
    print(f"\nERROR loading data: {e}")
    raise SystemExit


# --- Assign Regions using the Detailed Country List on the Processed DataFrame ---
print(f"\nAssigning regions based on column '{country_col}'...")

# Apply the country-based region assignment
df_processed['Region'] = df_processed[country_col].apply(assign_region_by_country)

# --- Debugging Step: Show unknown countries from the processed data ---
unknown_countries_df = df_processed[df_processed['Region'] == 'Unknown']
if not unknown_countries_df.empty:
    print("\n--- Countries not mapped to a known region (Unknown) ---")
    # Get unique country names that resulted in 'Unknown'
    unknown_country_names = unknown_countries_df[country_col].unique()
    print("The following country names from your data were not recognized:")
    # Print them clearly with indicators
    for country_name in unknown_country_names:
         print(f"- '{country_name}'") # Print with quotes to highlight any spaces/typos
    print("------------------------------------------------------------")
    print(f"Warning: {len(unknown_countries_df)} locations were not mapped to a known region ('Unknown').")
    print(f"These rows will be included in the '{unknown_sheet_output_name}' sheet.")


# --- Clean Preference Column on the Processed DataFrame ---
df_processed[preference_col] = df_processed[preference_col].astype(str).str.strip()

# --- Group Data from the Processed DataFrame (excluding Unknown for grouping) ---
print("\nGrouping data by Region and Career Preference (excluding Unknown region)...")
df_filtered_known_regions = df_processed[df_processed['Region'] != 'Unknown'].copy()
print(f"Number of rows after filtering for known regions: {len(df_filtered_known_regions)}") # Print count after filtering


# --- Create New Excel File with Original, Unknown, and Sorted Sheets ---
print(f"\nCreating output file '{output_excel_file}'...")

# Write the Excel file directly to the Colab file system
try:
    with pd.ExcelWriter(output_excel_file, engine='openpyxl') as writer:
        used_sheet_names = set()

        # Write the original data to the first sheet
        print(f"  Writing sheet: '{original_sheet_output_name}' ({len(df_original)} rows - Original Data)")
        df_original.to_excel(writer, sheet_name=original_sheet_output_name, index=False)
        used_sheet_names.add(original_sheet_output_name)

        # Write the unknown region data if there are any
        if not unknown_countries_df.empty:
            print(f"  Writing sheet: '{unknown_sheet_output_name}' ({len(unknown_countries_df)} rows - Unknown Region)")
            # Drop the temporary 'Region' column for the unknown sheet as well
            unknown_output_df = unknown_countries_df.drop(columns=['Region']).copy()
            unknown_output_df.to_excel(writer, sheet_name=unknown_sheet_output_name, index=False)
            used_sheet_names.add(unknown_sheet_output_name)
        else:
            print(f"\nNo applicants in the '{unknown_sheet_output_name}' sheet.")


        # Write the grouped data for known regions if there are any
        if not df_filtered_known_regions.empty:
            grouped = df_filtered_known_regions.groupby(['Region', preference_col])
            for (region, preference), group_df in grouped:
                # Sanitize and shorten sheet name if necessary
                sheet_name = f"{region} - {preference}"
                # Ensure sheet names are valid and not too long
                sheet_name = sheet_name[:31] # Truncate to max length for sheet names
                sheet_name = re.sub(r'[/\\?*\[\]:]', '_', sheet_name) # Replace invalid characters

                original_sheet_name_for_check = sheet_name
                counter = 1
                # Ensure sheet name is unique and doesn't conflict with other sheet names
                while sheet_name in used_sheet_names:
                    suffix = f"_{counter}"
                    max_len = 31 - len(suffix)
                    sheet_name = original_sheet_name_for_check[:max_len] + suffix
                    counter += 1
                used_sheet_names.add(sheet_name)

                print(f"  Writing sheet: '{sheet_name}' ({len(group_df)} rows)")
                # Drop the temporary 'Region' column before writing to excel
                output_df = group_df.drop(columns=['Region']).copy()
                output_df.to_excel(writer, sheet_name=sheet_name, index=False)
        else:
            print("\nNo data to group for known regions.")

    print(f"\nProcessing complete. The file '{output_excel_file}' has been saved to the Colab file system.")
    print(f"You can download it manually from the file pane on the left sidebar.")

except Exception as e:
    print(f"\nERROR writing the Excel file: {e}")


print(f"You can upload the file '{output_excel_file}' to Google Sheets to view the data with separate sheets.")
print(f"A suggested name for the Google Sheet is '{GOOGLE_SHEET_NAME}'.")
