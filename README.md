# Coding Challenge: Backup Tool

## Objective

Build a command line **file backup tool** that can take snapshots of a directory, storing its contents in a database and
supporting incremental backups.
Each snapshot should represent the state of the directory at a given point in time.
The tool should allow the user to restore a copy of the directory from a snapshot.
The tool should also support pruning of old snapshots without losing data.

## Requirements

- Demonstrate your ability to write robust, professional-grade code to the best of your ability.
- Provide *automated* tests that prove the correctness of your implementation.
- Provide an easy and repeatable way to build, test, and execute your
  implementation on a unix-like system.

## Guidelines

- For the purpose of this challenge, focus on the core functionality rather than window dressing such as argument
  parsing or output formatting. The mechanics of the operations and the quality of the code are the priority, so you
  don't need to worry about making the command-line interface polished or user-friendly.
- Time box your effort to fit the amount of time you are willing and able to commit. It's OK to leave pieces of this
  incomplete. It is more preferable to have a few complete pieces than many incomplete pieces.

## Operations

### `snapshot`

Takes a snapshot of all files in the specified directory and stores their content and filenames in a database.

- Only the file contents and filenames are stored as part of the snapshot; metadata like permissions, ownership, or
  timestamps should be ignored.
- Snapshots should store only incremental differences in order to minimize the size of the database. That is, the
  minimal amount of data necessary to express the new state of the directory by referencing already-stored data.
- The tool should not store any duplicate file or directory content. Use content hashes (such as SHA-256) to detect
  changes and avoid storing duplicate content.
- The database can be a database of any kind, not necessarily involving a database management system.
- Snapshots are given a number in sequence based on the order in which they were created.

Illustrative example: `$ backuptool snapshot --target-directory=~/my_important_files`

### `list`

Lists snapshots that are stored in the database.

- Snapshots are listed in a table on stdout with the following columns: snapshot number, timestamp

Illustrative example:

```
$ backuptool list
SNAPSHOT  TIMESTAMP
1         2024-09-01 14:35:22
2         2024-09-02 09:10:45
3         2024-09-03 16:22:10
```

### `restore`

Restores the directory state from any previous snapshot into a new directory.

- The tool should recreate the entire directory structure and contents
  exactly as they were at the time of the snapshot.
- Only the files present in the snapshot should be restored.
- All files that were originally shapshotted should be restored.
- The restored files should be bit-for-bit identical to the originally
  snapshotted files.

Illustrative example: `$ backuptool restore --snapshot-number=42 --output-directory=./out`

### `prune`

Removes old snapshots from the database and deletes any unreferenced data.

- The tool should allow the user to prune older snapshots while ensuring no data loss from the remaining snapshots.
- After pruning, all remaining snapshots should still be fully restorable.

Illustrative example: `$ backuptool prune --snapshot=42`

## Sanity Checks

Before submitting, double-check that all of these are true of your implementation.
It is recommended to create integration tests to verify these where possible.

- All files that were originally included in a snapshot are restored when that
  snapshot is restored.
- Restored files are bit-for-bit identical to the originally snapshotted files.
- Pruning a snapshot should not affect the ability to restore a different
  snapshot, even if the pruned snapshot shared data with the other snapshot. An
  unpruned snapshot should always be restorable.
- Your implementation can handle files that contain arbitrary binary content.
- Your implementation can handle relative and absolute file paths robustly.
- When snapshotting a directory twice without changes, the second snapshot
  should only cause storage of metadata, and not result in storage of duplicate
  file content.
- Try getting your project set up and running on a clean system by following
  your own provided instructions. You can use a container or VM to try this.
  The project should be trivial to get running.

## Stretch goals

- Enhance `list` operation to include additional disk-usage metrics, such as:
    - How much disk space the directory consumed at the time of snapshotting (i.e. how much space the restored snapshot would require).
    - How much disk space the snapshot actually requires for itself.  (i.e. how much space is required to store the files that are unique to the snapshot)
    - Total size of the database as a 'summary' line
- Implement a `check` operation that scans the database for any corrupted file content.
- Use chunking to de-duplicate storage at a more fine-grained level.
- Come up with your own idea and implement it!

## Licensing

Copyright 2025 GridUnity. All rights reserved.

This document is copyrighted by GridUnity and is provided for reference purposes only.

This document may not be copied, shared, or redistributed without prior
permission from GridUnity.

This copyright applies solely to this document and does not extend to any
solutions, code, or responses provided by the candidate.
