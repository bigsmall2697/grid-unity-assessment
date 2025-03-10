import os
import sqlite3
import hashlib
import shutil
import json
import time
from pathlib import Path

DB_FILE = "backup.db"
SNAPSHOT_DIR = "snapshots"

class BackupTool:
    def __init__(self, db_path=DB_FILE):
        self.db_path = db_path
        self._init_db()
        
    def _connect(self):
        """Helper function to create a connection and enforce foreign keys."""
        conn = sqlite3.connect(self.db_path)
        conn.execute("PRAGMA foreign_keys = ON;")
        return conn
    
    def _init_db(self):
        os.makedirs(SNAPSHOT_DIR, exist_ok=True)
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute("PRAGMA foreign_keys = ON;")  # Enable foreign key constraints
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS snapshots (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT NOT NULL
                )
            """
            )
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS files (
                    snapshot_id INTEGER,
                    path TEXT,
                    hash TEXT,
                    size INTEGER,
                    FOREIGN KEY(snapshot_id) REFERENCES snapshots(id) ON DELETE CASCADE
                )
            """
            )
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS file_data (
                    hash TEXT PRIMARY KEY,
                    content BLOB,
                    size INTEGER
                )
            """
            )
            conn.commit()

    def _hash_file(self, file_path):
        hasher = hashlib.sha256()
        size = os.path.getsize(file_path)
        with open(file_path, 'rb') as f:
            while chunk := f.read(8192):
                hasher.update(chunk)
        return hasher.hexdigest(), size

    def snapshot(self, target_directory):
        target_directory = Path(target_directory).resolve()
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute("INSERT INTO snapshots (timestamp) VALUES (?)", (time.strftime("%Y-%m-%d %H:%M:%S"),))
            snapshot_id = cursor.lastrowid
            total_size = 0
            
            for file in target_directory.rglob("*"):
                if file.is_file():
                    file_hash, file_size = self._hash_file(file)
                    total_size += file_size
                    cursor.execute("INSERT INTO files (snapshot_id, path, hash, size) VALUES (?, ?, ?, ?)",
                                   (snapshot_id, str(file.relative_to(target_directory)), file_hash, file_size))
                    
                    cursor.execute("SELECT hash FROM file_data WHERE hash = ?", (file_hash,))
                    if not cursor.fetchone():
                        with open(file, 'rb') as f:
                            cursor.execute("INSERT INTO file_data (hash, content, size) VALUES (?, ?, ?)",
                                           (file_hash, f.read(), file_size))
            
            conn.commit()
        print(f"Snapshot {snapshot_id} created. Total size: {total_size} bytes")
        return snapshot_id
    
    def list_snapshots(self):
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT s.id, s.timestamp, COALESCE(SUM(f.size), 0) FROM snapshots s LEFT JOIN files f ON s.id = f.snapshot_id GROUP BY s.id")
            snapshots = cursor.fetchall()
            print("SNAPSHOT  TIMESTAMP           SIZE (bytes)")
            for snap in snapshots:
                print(f"{snap[0]}        {snap[1]}    {snap[2]}")
    
    def restore(self, snapshot_number, output_directory):
        output_directory = Path(output_directory).resolve()
        os.makedirs(output_directory, exist_ok=True)
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT path, hash FROM files WHERE snapshot_id = ?", (snapshot_number,))
            files = cursor.fetchall()
            
            for rel_path, file_hash in files:
                target_path = output_directory / rel_path
                os.makedirs(target_path.parent, exist_ok=True)
                cursor.execute("SELECT content FROM file_data WHERE hash = ?", (file_hash,))
                content = cursor.fetchone()
                if content:
                    with open(target_path, 'wb') as f:
                        f.write(content[0])
        print(f"Snapshot {snapshot_number} restored to {output_directory}.")
    
    def prune(self, snapshot_number):
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute("PRAGMA foreign_keys = ON;")
            cursor.execute("DELETE FROM snapshots WHERE id = ?", (snapshot_number,))
            cursor.execute("DELETE FROM files WHERE snapshot_id = ?", (snapshot_number,))
            conn.commit()
        print(f"Snapshot {snapshot_number} pruned.")
    
    def check(self):
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT hash, content FROM file_data")
            files = cursor.fetchall()
            for file_hash, content in files:
                if hashlib.sha256(content).hexdigest() != file_hash:
                    print(f"Corruption detected for hash: {file_hash}")
                    return
        print("All files verified successfully.")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Backup Tool")
    subparsers = parser.add_subparsers(dest="command")
    
    snap_parser = subparsers.add_parser("snapshot")
    snap_parser.add_argument("--target-directory", required=True)
    
    list_parser = subparsers.add_parser("list")
    
    restore_parser = subparsers.add_parser("restore")
    restore_parser.add_argument("--snapshot-number", required=True, type=int)
    restore_parser.add_argument("--output-directory", required=True)
    
    prune_parser = subparsers.add_parser("prune")
    prune_parser.add_argument("--snapshot", required=True, type=int)
    
    check_parser = subparsers.add_parser("check")
    
    args = parser.parse_args()
    tool = BackupTool()
    
    if args.command == "snapshot":
        tool.snapshot(args.target_directory)
    elif args.command == "list":
        tool.list_snapshots()
    elif args.command == "restore":
        tool.restore(args.snapshot_number, args.output_directory)
    elif args.command == "prune":
        tool.prune(args.snapshot)
    elif args.command == "check":
        tool.check()
    else:
        parser.print_help()
