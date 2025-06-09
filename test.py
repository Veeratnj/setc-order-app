import requests
import pandas as pd

# Step 1: API URL
url = "http://127.0.0.1:8000/portfolios/trade-history/3"

# Step 2: Send GET request
response = requests.get(url, headers={"accept": "application/json"})

# Step 3: Check for successful response
if response.status_code == 200:
    data = response.json()
    
    # Step 4: Convert to DataFrame
    df = pd.DataFrame(data)
    
    # Step 5: Save to Excel
    df.to_excel("trade_history.xlsx", index=False)
    
    print("✅ Excel file 'trade_history.xlsx' created successfully!")
else:
    print(f"❌ Failed to fetch data. Status code: {response.status_code}")
