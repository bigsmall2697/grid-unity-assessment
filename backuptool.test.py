import os
import sqlite3
import hashlib
import shutil
import json
import time
import unittest
from pathlib import Path
from backuptool import BackupTool

class TestBackupTool(unittest.TestCase):
    TEST_DIR = "./test_data"
    RESTORE_DIR = "./restore_data"
    TEST_DB = "test_backup.db"
    
    @classmethod
    def setUpClass(cls):
        os.makedirs(cls.TEST_DIR, exist_ok=True)
        cls.tool = BackupTool(db_path=cls.TEST_DB)
    
    @classmethod
    def tearDownClass(cls):
        del cls.tool        
        shutil.rmtree(cls.TEST_DIR, ignore_errors=True)
        shutil.rmtree(cls.RESTORE_DIR, ignore_errors=True)

    def setUp(self):
        if os.path.exists(self.RESTORE_DIR):
            shutil.rmtree(self.RESTORE_DIR)
        os.makedirs(self.RESTORE_DIR, exist_ok=True)

    def test_snapshot_and_restore(self):
        file1 = Path(self.TEST_DIR) / "file1.txt"
        file2 = Path(self.TEST_DIR) / "file2.txt"
        file1.write_text("Hello World")
        file2.write_text("Backup Tool Test")
        
        snapshot_id = self.tool.snapshot(self.TEST_DIR)
        
        os.makedirs(self.RESTORE_DIR, exist_ok=True)
        self.tool.restore(snapshot_id, self.RESTORE_DIR)
        
        self.assertTrue((Path(self.RESTORE_DIR) / "file1.txt").exists(), "file1.txt was not restored")
        self.assertEqual((Path(self.RESTORE_DIR) / "file1.txt").read_text(), "Hello World")
        self.assertTrue((Path(self.RESTORE_DIR) / "file2.txt").exists(), "file2.txt was not restored")
        self.assertEqual((Path(self.RESTORE_DIR) / "file2.txt").read_text(), "Backup Tool Test")

    def test_prune_snapshot(self):
        file1 = Path(self.TEST_DIR) / "file1.txt"
        file1.write_text("To be deleted")
        
        snapshot_id = self.tool.snapshot(self.TEST_DIR)
        self.tool.prune(snapshot_id)

        self.tool.list_snapshots()
        
        with sqlite3.connect(self.TEST_DB) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM snapshots WHERE id = ?", (snapshot_id,))
            snapshot_count = cursor.fetchone()[0]
        
        self.assertEqual(snapshot_count, 0, "Snapshot should be removed")
    
    def test_binary_file_handling(self):
        file_path = Path(self.TEST_DIR) / "image.bin"
        with open(file_path, "wb") as f:
            f.write(os.urandom(1024))
        
        snapshot_id = self.tool.snapshot(self.TEST_DIR)
        self.tool.restore(snapshot_id, self.RESTORE_DIR)
        
        restored_path = Path(self.RESTORE_DIR) / "image.bin"
        self.assertTrue(restored_path.exists(), "Binary file was not restored")
        
        original = file_path.read_bytes()
        restored = restored_path.read_bytes()
        self.assertEqual(original, restored, "Binary files should be restored identically")
        
    def test_check_integrity(self):
        self.tool.snapshot(self.TEST_DIR)
        self.tool.check()
        
if __name__ == "__main__":
    unittest.main()
