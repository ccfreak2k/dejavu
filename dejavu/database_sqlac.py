from __future__ import absolute_import

from itertools import zip_longest

try:
    import sqlite3
except ImportError:
    pass

from sqlalchemy import Table, Column, MetaData, Binary, Integer, Text, Boolean, ForeignKey, UniqueConstraint, create_engine
from sqlalchemy.sql import select, func

from dejavu.database import Database


class SQLACDatabase(Database):
    """
    An implementation of the Database backend using the SQLAlchemy Core system.
    """
    type = "sqlalchemy_core"

    FINGERPRINTS_TABLENAME = "fingerprints"
    SONGS_TABLENAME = "songs"

    FIELD_FINGERPRINTED = "fingerprinted"

    metadata = MetaData()

    class HexedBinary(Binary):
        """
        A Binary column that automatically hexes/unhexes when read/written.

        SQLite doesn't normally allow non-Unicode strings, even for values
        destined to BLOB columns, so we have to put those in a buffer
        first and cast them back as str when retrieving.
        """
        @staticmethod
        def _hextobin(val):
            return bytes.fromhex(val)

        @staticmethod
        def _hextobin_sqlite(val):
            return sqlite3.Binary(bytes.fromhex(val))

        @staticmethod
        def _bintohex(val):
            return val.hex().upper()

        def bind_processor(self, dialect):
            if dialect.name == "sqlite":
                return self._hextobin_sqlite
            else:
                return self._hextobin

        def result_processor(self, dialect, coltype):
            return self._bintohex

    songs = Table(SONGS_TABLENAME, metadata,
                  Column(Database.FIELD_SONG_ID, Integer, primary_key=True),
                  Column(Database.FIELD_SONGNAME, Text, nullable=False),
                  Column(FIELD_FINGERPRINTED, Boolean, default=False),
                  Column(Database.FIELD_FILE_SHA1, HexedBinary(20), nullable=False),
                  UniqueConstraint(Database.FIELD_SONG_ID, name=Database.FIELD_SONG_ID)
                  )

    fingerprints = Table(FINGERPRINTS_TABLENAME, metadata,
                         Column(Database.FIELD_HASH, HexedBinary(8), nullable=False),
                         Column(Database.FIELD_SONG_ID, None, ForeignKey(songs.c[Database.FIELD_SONG_ID], ondelete="CASCADE"), nullable=False),
                         Column(Database.FIELD_OFFSET, Integer, nullable=False),
                         UniqueConstraint(Database.FIELD_HASH, Database.FIELD_SONG_ID, Database.FIELD_OFFSET, name='unique_constraint')
                         )

    Engine = None

    def __init__(self, **options):
        super(SQLACDatabase, self).__init__()
        connection_string = ""
        echo = False
        if "connection_string" in options and options["connection_string"] != "":
            connection_string = options["connection_string"]
        else:
            connection_string = "{}://".format(options["driver"])
            if "user" in options and options["user"] != "":
                connection_string += "{}:{}".format(options["user"],options["passwd"])
            if "host" in options and options["host"] != "":
                connection_string += "@{}".format(options['host'])
            if "db" in options and options["db"] != "":
                connection_string += "/{}".format(options["db"])
        if "echo" in options:
            echo = options['echo']
        self.Engine = create_engine(connection_string, echo=echo)
        self.metadata.bind = self.Engine

    @staticmethod
    def _grouper(iterable, n, fillvalue=None):
        args = [iter(iterable)] * n
        return (filter(None, values) for values
                in zip_longest(fillvalue=fillvalue, *args))

    def setup(self):
        """
        Creates any non-existing tables required for dejavu to function.
        """
        self.metadata.create_all()

    def empty(self):
        """
        Called when the database should be cleared of all data.
        """
        self.metadata.drop_all()
        self.setup()

    def delete_unfingerprinted_songs(self):
        """
        Called to remove any song entries that do not have any fingerprints
        associated with them.
        """
        d = self.songs.delete().where(self.songs.c[SQLACDatabase.FIELD_FINGERPRINTED] == False)
        with self.Engine.connect() as c:
            c.execute(d)

    def get_num_songs(self):
        """
        Returns the amount of songs in the database.
        """
        s = select([func.count()]).select_from(self.songs).where(self.songs.c[SQLACDatabase.FIELD_FINGERPRINTED] == True)
        with self.Engine.connect() as c:
            r = c.execute(s).scalar()
            return r

    def get_num_fingerprints(self):
        """
        Returns the number of fingerprints in the database.
        """
        s = select([func.count()]).select_from(self.fingerprints)
        with self.Engine.connect() as c:
            r = c.execute(s).scalar()
            return r

    def set_song_fingerprinted(self, sid):
        """
        Sets a specific song as having all fingerprints in the database.

        sid: Song identifier
        """
        u = self.songs.update().where(self.songs.c[SQLACDatabase.FIELD_SONG_ID] == sid).values(**{SQLACDatabase.FIELD_FINGERPRINTED: True})
        with self.Engine.connect() as c:
            c.execute(u)

    def get_songs(self):
        """
        Returns all fully fingerprinted songs in the database.
        """
        s = self.songs.select().where(self.songs.c[SQLACDatabase.FIELD_FINGERPRINTED] == True)
        with self.Engine.connect() as c:
            l = [dict(zip(r.keys(), r)) for r in c.execute(s).fetchall()]
            for r in l:
                yield r

    def get_song_by_id(self, sid):
        """
        Return a song by its identifier

        sid: Song identifier
        """
        if sid is -1:
            return None
        s = select([self.songs]).where(self.songs.c[SQLACDatabase.FIELD_SONG_ID] == sid)
        with self.Engine.connect() as c:
            r = c.execute(s)
            return dict(zip(r.keys(), r.fetchone()))

    def insert(self, hash, sid, offset):
        """
        Inserts a single fingerprint into the database.

          hash: Part of a sha1 hash, in hexadecimal format
           sid: Song identifier this fingerprint is off
        offset: The offset this hash is from
        """
        i = self.fingerprints.insert().values(**{SQLACDatabase.FIELD_HASH: hash, SQLACDatabase.FIELD_SONG_ID: sid, SQLACDatabase.FIELD_OFFSET: int(offset)})
        with self.Engine.connect() as c:
            c.execute(i)

    def insert_song(self, song_name, file_hash):
        """
        Inserts a song name into the database, returns the new
        identifier of the song.

        song_name: The name of the song.
        """
        i = self.songs.insert().values(**{SQLACDatabase.FIELD_SONGNAME: song_name, Database.FIELD_FILE_SHA1: file_hash})
        with self.Engine.connect() as c:
            return c.execute(i).inserted_primary_key[0]

    def query(self, hash):
        """
        Return all tuples associated with hash.

        If hash is None, returns all entries in the
        database (be careful with that one!).
        """
        s = select([self.fingerprints.c[Database.FIELD_SONG_ID], self.fingerprints.c[Database.FIELD_OFFSET]])
        if hash is not None:
            s = s.where(self.fingerprints.c[Database.FIELD_HASH] == hash)
        with self.Engine.connect() as c:
            for r in c.execute(s).fetchall():
                yield (r[0], r[1])

    def get_iterable_kv_pairs(self):
        """
        Returns all tuples in database.
        """
        return self.query(None)

    def insert_hashes(self, sid, hashes):
        """
        Insert series of hash => song_id, offset
        values into the database.
        """
        with self.Engine.connect() as c:
            i = self.fingerprints.insert()
            c.execute(i, [{SQLACDatabase.FIELD_HASH: h[0], Database.FIELD_SONG_ID: sid,Database.FIELD_OFFSET:int(h[1])} for h in hashes])

    def return_matches(self, hashes):
        """
        Return the (song_id, offset_diff) tuples associated with
        a list of (sha1, sample_offset) values.
        """
        mapper = {}
        for hash, offset in hashes:
            mapper[hash.upper()] = offset
        s = [select([self.fingerprints.c[Database.FIELD_SONG_ID], self.fingerprints.c[Database.FIELD_OFFSET], self.fingerprints.c[Database.FIELD_HASH]], self.fingerprints.c[Database.FIELD_HASH].in_(split)) for split in self._grouper(mapper.keys(), 999)]
        with self.Engine.connect() as c:
            for s in s:
                for f in c.execute(s).fetchall():
                    yield(f[0], f[1]-mapper[f[2].upper()])
