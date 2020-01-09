package filedb

import (
	"fmt"

	"github.com/gwenn/gosqlite"
)

func Create(db *sqlite.Conn, path string) error {
	return db.Exec(fmt.Sprintf(`
		create virtual table found_files using file_search(%s);

		create table if not exists file_index (
			path string primary key,
			size int,
			modtime datetime,
			proved_unique int default 0,
			first_bytes_hash string,
			hash string
		);

		delete from file_index
		where path not in (
			select file_index.path from file_index
			inner join found_files as found
			on file_index.path = found.path
			and file_index.size = found.size
			and file_index.modtime = found.modtime
		);

		update file_index
		set proved_unique = 0;

		insert or ignore into file_index (path, size, modtime)
		select path, size, modtime from found_files;

		drop table found_files;
	`, path))
}

func ExcludeUniqueSize(db *sqlite.Conn) error {
	return db.Exec(`
		create index if not exists
		idx_file_index_size on file_index(size);

		update file_index
		set proved_unique = 1
		where size in (
			select size
			from file_index
			group by size
			having count(size) = 1
		)
	`)
}

func ExcludeUniqueFirstBytes(db *sqlite.Conn) error {
	return db.Exec(`
		update file_index
		set first_bytes_hash = hash_first_bytes(path)
		where proved_unique = 0 and first_bytes_hash is null;

		create index if not exists
		idx_file_index_first_bytes_hash on file_index(first_bytes_hash);

		update file_index
		set proved_unique = 1
		where first_bytes_hash is not null and first_bytes_hash in (
			select first_bytes_hash
			from file_index
			group by first_bytes_hash
			having count(first_bytes_hash) = 1
		);

		create index if not exists
		idx_file_index_proved_unique on file_index(proved_unique);
	`)
}

func HashFiles(db *sqlite.Conn) error {
	return db.Exec(`
		update file_index
		set hash = hash(path)
		where proved_unique = 0 and hash is null;

		create index if not exists
		idx_file_index_hash on file_index(hash);
	`)
}

func PrintDuplicates(db *sqlite.Conn) error {
	err := db.Select(`
		select group_concat(path, char(13) || char(10))
		from file_index
		where hash is not null
		group by hash
		having count(hash) > 1
	`, func(s *sqlite.Stmt) error {
		var paths string
		if e := s.Scan(&paths); e != nil {
			return e
		}

		fmt.Println(paths)
		fmt.Println()

		return nil
	})

	return err
}
