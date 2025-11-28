import os
import sys
import shutil
from pathlib import Path
import yaml
import sqlite3

# Add project root to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from mcp_database_manager.config import ConfigManager
from mcp_database_manager.db_manager import DatabaseManager

def test_verification():
    print("Starting verification...")
    
    # Setup test environment
    test_dir = Path("./test_env")
    if test_dir.exists():
        shutil.rmtree(test_dir)
    test_dir.mkdir()
    
    # Create test database
    db_path = test_dir / "test.db"
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
    cursor.execute("INSERT INTO users (name) VALUES ('Alice')")
    cursor.execute("INSERT INTO users (name) VALUES ('Bob')")
    conn.commit()
    conn.close()
    
    # Create config
    config_data = {
        "connections": [
            {
                "name": "test_read",
                "url": f"sqlite:///{db_path.absolute()}",
                "readonly": True
            },
            {
                "name": "test_write",
                "url": f"sqlite:///{db_path.absolute()}",
                "readonly": False
            }
        ]
    }
    
    config_dir = test_dir / "config"
    config_dir.mkdir()
    with open(config_dir / "config.yaml", "w") as f:
        yaml.dump(config_data, f)
        
    # Initialize managers
    config_manager = ConfigManager(config_dir=config_dir)
    db_manager = DatabaseManager(config_manager)
    
    # Test 1: List connections
    print("\nTest 1: List connections")
    conns = config_manager.list_connections()
    print(f"Found {len(conns)} connections")
    assert len(conns) == 2
    
    # Test 2: Read SQL (Allowed)
    print("\nTest 2: Read SQL (Allowed)")
    results = db_manager.execute_read("test_read", "SELECT * FROM users")
    print(f"Results: {results}")
    assert len(results) == 2
    assert results[0]['name'] == 'Alice'
    
    # Test 3: Write SQL on Readonly (Should Fail)
    print("\nTest 3: Write SQL on Readonly (Should Fail)")
    try:
        db_manager.execute_write("test_read", "INSERT INTO users (name) VALUES ('Charlie')")
        print("ERROR: Write succeeded on readonly connection!")
    except PermissionError as e:
        print(f"Success: Caught expected error: {e}")
        
    # Test 4: Write SQL on ReadWrite (Should Succeed)
    print("\nTest 4: Write SQL on ReadWrite (Should Succeed)")
    result = db_manager.execute_write("test_write", "INSERT INTO users (name) VALUES ('Charlie')")
    print(f"Write result: {result}")
    
    # Verify write
    results = db_manager.execute_read("test_read", "SELECT * FROM users")
    assert len(results) == 3
    
    # Test 5: Get Schema
    print("\nTest 5: Get Schema")
    schema = db_manager.get_schema("test_read")
    print(schema)
    assert "users" in schema
    assert "name" in schema
    
    print("\nVerification Complete!")
    
    # Cleanup
    # shutil.rmtree(test_dir)

if __name__ == "__main__":
    with open("verify_output.txt", "w") as log_file:
        sys.stdout = log_file
        sys.stderr = log_file
        try:
            test_verification()
        except Exception as e:
            print(f"FAILED with error: {e}")
            import traceback
            traceback.print_exc()
