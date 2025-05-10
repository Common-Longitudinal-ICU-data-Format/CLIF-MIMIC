import pandas as pd # utilizing pandas to create a table for this data 

data = {'Variable Name': ["hospitalization_id","start_dttm","code_status_name","code_status_category"], # Table set up that includes the draft table data
        'Data Type':["VARCHAR", "DATETIME", "VARCHAR", "VARCHAR"],
         'Definition': ["Unique identifier linking the code status event to a specific hospitalization in the CLIF database.", "The date and time when the specific code status was initiated.","The name/description of the code status.", "Categorical variable specifying the code status during the hospitalization."],
          'Permissible VAlues': ["Unique identifier, e.g., 123456", "Example: 2024-12-03 08:30:00+00:00", "Free text to describe the code status.", "E.g., DNR, UDNR, DNR/DNI, Full, Presume Full, Other"]}

df = pd.DataFrame(data, index=["1", "2", "3", "4"]) # inserting integers to number columns and setting up data frame 

print(df) # printing display tabel 

df.to_csv("CLIFF_MIMIC_Tabel.csv") #saving the file as a csv 