import sys
import os
import pandas as pd
from datetime import datetime

# Add src to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../src')))

from infra.adapters.storage.google_drive_adapter import GoogleDriveAdapter

def verify_drive_integration():
    print("--- Google Drive Integration Verification ---")
    
    client_secret_file = "secrets/client_secret.json"
    service_account_file = "secrets/service-account.json"
    
    if not os.path.exists(client_secret_file) and not os.path.exists(service_account_file):
        print(f"🚨 No credential files found in secrets/")
        return

    try:
        # 1. Initialize Adapter
        # Prefer Client Secret (OAuth)
        if os.path.exists(client_secret_file):
            print(f"Using OAuth 2.0 ({client_secret_file})")
            adapter = GoogleDriveAdapter(client_secret_file=client_secret_file, root_folder_name="KRX_Test_Folder")
        else:
            print(f"Using Service Account ({service_account_file})")
            adapter = GoogleDriveAdapter(service_account_file=service_account_file, root_folder_name="KRX_Test_Folder")
        
        # 2. Create Dummy Data
        df = pd.DataFrame({
            'Name': ['Test1', 'Test2', 'Test3'],
            'Value': [100, 200, 300],
            'Date': [datetime.now(), datetime.now(), datetime.now()]
        })
        
        filename = f"test_upload_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        path = f"verification/{filename}"
        
        # 3. Test Save (Upload)
        print(f"\n[Test 1] Uploading {filename}...")
        success = adapter.save_dataframe_excel(df, path, index=False)
        if success:
            print("✅ Upload Successful")
        else:
            print("🚨 Upload Failed")
            return

        # 4. Test Load (Download)
        print(f"\n[Test 2] Downloading {filename}...")
        loaded_df = adapter.load_dataframe(path)
        
        if not loaded_df.empty:
            print("✅ Download & Load Successful")
            print("Loaded Data:")
            print(loaded_df)
            
            # Verify Content
            if len(loaded_df) == 3 and loaded_df.iloc[0]['Name'] == 'Test1':
                 print("✅ Content Verification Passed")
            else:
                 print("🚨 Content Verification Failed")
        else:
            print("🚨 Download Failed or Empty Data")

    except Exception as e:
        print(f"🚨 Verification Failed with Exception: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    verify_drive_integration()
