# Lincoln Adams, Dakota Dowd, Caleb Caten, Isaac Pratte, and Josh Knight
# IS 303 - Section 004
# P4 - Retails Sales: Pandas and PostgreSQL
# Description: Transfering data to a postgres database and reading it back from the database

import pandas as pd # Importing pandas dataframe
import shutil # For the border function
from sqlalchemy import create_engine # Imports the create engine function of sqlalchemy
import matplotlib.pyplot as plot # Imports the plot maker from matplotlib

def border(): # Border function based on the size of the terminal window for readability
    iTerminalWidth = shutil.get_terminal_size().columns
    return '-' * iTerminalWidth

def import_file(): # For readability since I included a lot of error handling. The function gets the file type and file name from the user and imports into a pandas dataframe
    bContinue = True
    while bContinue:
        try:
            print(f"\nPlease select an option:\n    1. Excel file\n    2. CSV File\n\n") # Asking for which file to read into the dataframe and exception handling
            iFileType = int(input("Selection: "))
            if iFileType not in [1, 2]: # Looping until either 1 or 2 is selected
                print(f"\n    !ERROR! --> Please enter either 1 or 2 to ensure the correct file type is read.\n")
            else:
                bContinue = False
        except ValueError:
            print(f"\n    !ERROR! --> Please enter either 1 or 2 to ensure the correct file type is read.\n")

    if iFileType == 1:
        bContinue = True # Error handling to ensure a valid file is imported
        while bContinue:
            try:
                sFileName = input(f"\nPlease enter the name of the Excel file you would like to import, including the .xlsx extension: ")
                dfImportedFile = pd.read_excel(sFileName, index_col=0) # If option 1 is selected, user types in file name to import
                bContinue = False
            except:
                print(f'\n    !ERROR! --> Invalid file name. Please enter a valid file name with the correct extension (ex: .xlsx).\n')
    else:
        bContinue = True # Error handling to ensure a valid file is imported
        while bContinue:
            try:
                sFileName = input(f"\nPlease enter the name of the CSV file you would like to import, including the .csv extension: ")
                dfImportedFile = pd.read_csv(sFileName, index_col=0) # If option 1 is selected, user types in file name to import
                bContinue = False
            except:
                print(f'\n    !ERROR! --> Invalid file name. Please enter a valid file name with the correct extension (ex: .csv).\n')
    
    return dfImportedFile

def menu():
    print(f"\nEnter your menu selection as a whole integer\n") # Menu selection saves as integer
    iSelection = int( input(f"\nMenu:\n    1. Import data\n    2. See summaries of stored data\n\n    Enter any other value to exit the program.\n\nSelection: "))
    print()

    print(border()) # Printing a border line

    return iSelection

def database_connection():
    # Database connection details
    sUsername = 'testuser' 
    sPassword = 'test'
    sHost = 'localhost'
    sPort = '5432'
    sDatabaseName = 'is303'

    sConnectionString = f'postgresql+psycopg2://{sUsername}:{sPassword}@{sHost}:{sPort}/{sDatabaseName}' # Connection string
    engine = create_engine(sConnectionString) # Creates the engine

    return engine

def main():

    iSelection = menu() # Prints the menu and returns the selection

    while iSelection == 1 or iSelection == 2: # Loops while a valid selection has been entered

        if iSelection == 1: 

            dfImportedFile = import_file() # Calls function to import the correct file and saves as a pandas dataframe

            lstFirstNames = [] # Empty lists to store the data from the next loop, to then assign it to the new column
            lstLastNames = [] 

            for name in dfImportedFile['name']: # Loops through each value in the name column
                sFirstName, sLastName = name.split('_') # Splits it into first and last at the underscore
                lstFirstNames.append(sFirstName) # Appends the new values to their corresponding lists
                lstLastNames.append(sLastName)
            
            dfImportedFile.drop(columns=['name'], inplace=True) # Drops the old column now that the new values are saved in their corresponding lists

            dfImportedFile.insert(loc=0, column='first_name', value=lstFirstNames) # Creating a new column "first_name" at the 1 index
            dfImportedFile.insert(loc=1, column='last_name', value=lstLastNames) # Creating a new column "last_name" at the 2 index

            dctProdCategories = { # Dictionary with correct product categories
            'Camera': 'Technology',
            'Laptop': 'Technology',
            'Gloves': 'Apparel',
            'Smartphone': 'Technology',
            'Watch': 'Accessories',
            'Backpack': 'Accessories',
            'Water Bottle': 'Household Items',
            'T-shirt': 'Apparel',
            'Notebook': 'Stationery',
            'Sneakers': 'Apparel',
            'Dress': 'Apparel',
            'Scarf': 'Apparel',
            'Pen': 'Stationery',
            'Jeans': 'Apparel',
            'Desk Lamp': 'Household Items',
            'Umbrella': 'Accessories',
            'Sunglasses': 'Accessories',
            'Hat': 'Apparel',
            'Headphones': 'Technology',
            'Charger': 'Technology'
            }

            dfImportedFile['category'] = dfImportedFile['product'].map(dctProdCategories) # Assigns the product values using the dictionary key

            engine = database_connection()

            dfImportedFile.to_sql("sale", engine, if_exists='replace', index = False) # Imports the file into the postgresql database

            print(f"\nYou've imported the file into your postgres database.\n") 
            print(border())

        else:
            engine = database_connection() # Getting engine from the function
            dfCategories = pd.read_sql_query("SELECT DISTINCT category FROM sale", engine) # Assigning unique categories to a new database
            dfData = pd.read_sql_query("SELECT * FROM sale", engine)

            dctCategories = {}
            print(f"\nThe following are all the categories that have been sold:\n") # Printing menu with unique categories
            for iCount, sCategory in enumerate(dfCategories['category'], start=1):
                print(f"    {iCount}: {sCategory}")
                dctCategories[iCount] = sCategory
            

            # Error handling to only get valid inputs
            bContinue = True
            while bContinue:

                try:
                    iCategorySelection = int(input(f"\nPlease enter the number of the category you want to see summarized: ")) # Category selection
                    if iCategorySelection not in dctCategories: 
                        print(f"\n    !ERROR! --> Please enter a valid integer selection.\n")
                    else:
                        bContinue = False

                except ValueError:
                    print(f"\n    !ERROR! --> Please enter a valid integer selection.\n")
            
            dfQuery = dfData.query(f'category == "{dctCategories[iCategorySelection]}"') # Grabs all the data only from the selected category
            
            iTotalSales = dfQuery['total_price'].sum() # Gets the statistics from the particular category
            iAveragePrice = dfQuery['total_price'].mean()
            iQuantitySold = dfQuery['quantity_sold'].sum()

            print(f"\nStats:\n    Total Sales: ${iTotalSales:.2f}\n    Average Price: ${iAveragePrice:.2f}\n    Total Quantity Sold: {iQuantitySold} units\n\n") # Prints out the statistics
            print(border())

            dfProductSales = dfQuery.groupby('product')['total_price'].sum() # Groups all of the products in that category and gets the sum of their total prices
            dfProductSales.plot(kind='bar') # Creates a bar chart
            plot.title(f'Total Sales in {dctCategories[iCategorySelection]}') # Sets up chart
            plot.xlabel('Product') 
            plot.ylabel('Total Sales')
            plot.show() # Shows chart
        
        print(f"\nEnter your menu selection as a whole integer\n") # Menu selection saves as integer
        iSelection = int( input(f"\nMenu:\n    1. Import data\n    2. See summaries of stored data\n\n    Enter any other value to exit the program.\n\nSelection: "))
        print()

        print(border()) # Printing a border line

main() # Calls main function