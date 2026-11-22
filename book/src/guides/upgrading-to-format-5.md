# Upgrading to Format 5

This release introduces file format 5. This page is what that means for the
databases you already have, and what to do if you want them in the new format.

The short version: **you do not have to do anything.** A database created by
0.18 or earlier opens in this release as it is, and is read and written in the
layout it already has. Upgrading it is optional, and one command.

## What changed

A database keeps its nodes, its relationships and its search documents in
B+Trees keyed by id. Up to 0.18 those ids were written little-endian, lowest
byte first. The tree compares keys byte by byte, so ids handed out in order
were not stored in order: id 256 sorted between 0 and 1, every new id landed in
a different page, and the trees ended about 70% full.

Format 5 writes the same ids big-endian. New ids go to the right edge of the
tree and the pages fill. A load of 1,000,000 nodes and 2,000,000 relationships
got about a quarter faster from this change alone, and its two largest trees
between a quarter and a third smaller.
[Id Key Order](../architecture/graph-storage.md#id-key-order) has the details.

The order is fixed per file. A file created by this release is format 5. A file
created earlier keeps its little-endian keys until you upgrade it.

## Which release opens which file

| File | 0.17 and 0.18 | This release |
|------|---------------|--------------|
| Created by 0.18 or earlier, never written by this release | read and write | read and write |
| Created by 0.18 or earlier, since written by this release | read only | read and write |
| Created by 0.18 or earlier, then upgraded | refused | read and write |
| Created by this release | refused | read and write |

"Refused" is a clean error, `LATTICE_ERROR_VERSION_MISMATCH` through the C API
and the bindings, and the file is left untouched.

The second row is the one to know about. Once this release has written to an
old file, 0.17 and 0.18 can still read it but no longer write it
(`LATTICE_ERROR_NEEDS_NEWER_WRITER`). That includes recovery: if the database
was not closed cleanly, open it with this release once before an older one
reads it.

0.16.0 is as before: it opens an old file until the file holds something it
would misread, such as changefeed events
([Opening with 0.16.0](./changefeed.md#opening-with-0160)), and it refuses a
format-5 file.

If an older release has to keep **writing** a database, do not open that
database for writing with this release. If it only has to read it, carry on.

## Check what you have

```bash
lattice info social.lattice
```

```text
Format:       v4
Id keys:      little-endian (created before format 5)
              'lattice compact --upgrade-id-keys' converts them (one-way: only format-5 readers open the result)
Min reader:   v4, so 0.16.0 cannot open it (changefeed events)
```

`Id keys` is the line that says which layout the file has: `little-endian` for
an older file, `big-endian` for a format-5 one. `Min reader` is what keeps older
releases out, and stays where it was until you upgrade. `Format` is only the
newest release that has written to the file. It becomes `v5` the first time
this release writes to an old file, and that changes nothing about who can read
it.

After the upgrade the same lines read:

```text
Format:       v5
Id keys:      big-endian (format 5: scans return ascending ids)
Min reader:   v5, so 0.18 and earlier cannot open it (changefeed events, ordered id keys)
```

## Should you upgrade?

Upgrade if the database is still growing and only this release, or later ones,
will open it. You get full id trees, inserts that stay at the right edge, and
scans in ascending id order.

Leave it if:

- any program that still uses 0.18 or earlier has to open the file;
- the database is small or rarely written, where the difference does not show;
- you cannot stop the programs that use it for the length of the upgrade.

There is no deadline. An old file is not deprecated, and this release will keep
reading and writing it.

## Upgrading

Stop the programs that use the database. Then run the command without `--apply`
first. It changes nothing and tells you what you are agreeing to:

```bash
lattice compact social.lattice --upgrade-id-keys
```

```text
social.lattice has not been changed. Upgrading it to format 5 needs --apply.

Before you go ahead:
  - It cannot be undone. LatticeDB 0.18 and earlier cannot open the
    upgraded file.
  - Replication starts over. The upgraded file gets a new id, so a
    directory 'lattice replicate' already writes to will refuse it.
    Replicate to a new, empty directory afterwards. The old directory
    still restores the database as it was before the upgrade.
  - It needs room on disk for a second copy of the database (about
    2 MB), and no other program may have the database open.

Are you sure? To continue, run the command again with --apply:
  lattice compact social.lattice --upgrade-id-keys --apply
```

Then run it with `--apply`:

```bash
lattice compact social.lattice --upgrade-id-keys --apply
```

The command never rewrites your database in place. It writes the upgraded
database to a copy beside the original, compares the copy with the original
(every node, relationship, full-text and vector entry, and byte for byte every
page the upgrade had no reason to change), and only then renames the copy over
the original. If anything fails, the copy is deleted and your database is as it
was. If the command is interrupted, the path holds either the whole original or
the whole upgraded database, and running it again finishes the job.
[The CLI page](../getting-started/cli.md#upgrading-an-older-file-to-format-5)
goes through each step, and what the command needs in memory and disk.

A database of 1,000,000 nodes and 2,000,000 relationships took 13 to 30 seconds
on a busy laptop.

## After the upgrade

**Replication needs a new directory.** The upgraded file has a new id, and a
replication directory only accepts the database it was started for. Point
`lattice replicate`, and whatever runs it on a schedule, at a new, empty
directory. Keep the old directory for as long as you might want the database as
it was before the upgrade: `lattice restore` on it still works.
[After upgrading a database to format 5](./backup-and-replication.md#after-upgrading-a-database-to-format-5)
has more.

**The file may be a little larger.** The upgraded trees are written into new
pages, and the old trees' pages become free space in the middle of the file,
which is reused as the database grows. Two measured files grew 6% and 10%; one
of them then took 20,000 more nodes, 40,000 more relationships and 10,000 more
vectors without growing at all.

**Some rows come back in a different order.** A pattern that starts from a
relationship type alone, `MATCH ()-[r:KNOWS]->()`, now returns relationships in
ascending id order. No order was ever promised there; use `ORDER BY` where the
order matters.

## Going back

There is no command that turns a format-5 file back. If an older release has to
open the data again, you have three ways:

- Keep a copy from before the upgrade (`lattice backup`), or the old
  replication directory, and restore from it. You get the database as it was
  then.
- Export the upgraded database and import it with the older release
  ([Data Export](./data-export.md)).
- From Zig, create a new database with `new_database_id_keys = .little` and
  load into that.

## From an application

The upgrade runs from the `lattice` command. From Zig,
`Database.upgradeIdKeysTo` writes the upgraded, verified copy to a path you
choose, and leaves putting it in place to you. The C API and the Python, TypeScript, Go and Java
bindings have no call for it. An application that manages its users' files can
run the command, or go on using the files as they are.
