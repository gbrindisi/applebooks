#
# Copyright (c) 2023 Gianluca Brindisi
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
#

import os.path
import re
import sqlite3
import datetime
import glob

__version__ = '0.0.20230418'

def timestamp_to_datetime(s):
    '''
    Convert a Core Data timestamp to a datetime. They're all a float of seconds
    since 1 Jan 2001. We calculate the seconds in offset.
    '''
    if not s:
        return None

    OFFSET = (datetime.datetime(2001, 1, 1, 0, 0, 0)
        - datetime.datetime.fromtimestamp(0)).total_seconds()
    return datetime.datetime.fromtimestamp(s + OFFSET)


class EpubCFI(object):
    def __init__(self, cfi_string):
        self.cfi_string = cfi_string
        self.parsed_steps = self.parse_epub_cfi(cfi_string)

    def __repr__(self):
        return self.cfi_string
    
    def __lt__(self, other):
        return self.compare(other) < 0

    def __le__(self, other):
        return self.compare(other) <= 0

    def __gt__(self, other):
        return self.compare(other) > 0

    def __ge__(self, other):
        return self.compare(other) >= 0

    def compare(self, other):
        if not isinstance(other, EpubCFI):
            raise TypeError("Comparing EpubCFI with a non-EpubCFI object")

        min_length = min(len(self.parsed_steps), len(other.parsed_steps))

        for i in range(min_length):
            step_self = self.parsed_steps[i]
            step_other = other.parsed_steps[i]

            if step_self['step'] != step_other['step']:
                return step_self['step'] - step_other['step']

            if step_self['type'] != step_other['type']:
                return (step_self['type'] or '').__lt__(step_other['type'] or '')

            if step_self['text_offset'] != step_other['text_offset']:
                # Handle None text_offset values
                if step_self['text_offset'] is None:
                    return -1
                if step_other['text_offset'] is None:
                    return 1
                return step_self['text_offset'] - step_other['text_offset']

        return len(self.parsed_steps) - len(other.parsed_steps)


    def parse_epub_cfi(self, cfi_string):
        path_regex = re.compile(r'/(?P<step>\d+)(?P<type>\[\w+\])?(?P<text_offset>:\d+)?')

        def parse_step(match):
            step = int(match.group('step'))
            step_type = match.group('type')[1:-1] if match.group('type') else None
            text_offset = int(match.group('text_offset')[1:]) if match.group('text_offset') else None
            return {'step': step, 'type': step_type, 'text_offset': text_offset}

        if not cfi_string.startswith('epubcfi(') or not cfi_string.endswith(')'):
            raise ValueError('Invalid EPUB CFI string format')

        cfi_string = cfi_string[8:-1]
        parsed_steps = [parse_step(match) for match in path_regex.finditer(cfi_string)]

        return parsed_steps


class Annotation(object):
    def __init__(self, id, asset_id, creator_identifier, location, note, representative_text,
                 selected_text, type, style, deleted, is_underline, absolute_physical_location,
                 location_range_end, location_range_start, creation_date, modification_date, storage_uuid, user_data):
        self.id = id
        self.asset_id = asset_id
        self.creator_identifier = creator_identifier
        self.location = location
        self.note = note
        self.representative_text = representative_text
        self.selected_text = selected_text
        self.type = type
        self.style = style
        self.deleted = deleted
        self.is_underline = is_underline
        self.absolute_physical_location = absolute_physical_location
        self.location_range_end = location_range_end
        self.location_range_start = location_range_start
        self.creation_date = timestamp_to_datetime(creation_date)
        self.modification_date = timestamp_to_datetime(modification_date)
        self.storage_uuid = storage_uuid
        self.user_data = user_data

    def __repr__(self):
        return f"Annotation(id={self.id}, asset_id={self.asset_id}, type={self.type}, note={self.note})"


class Book(object):
    def __init__(self, applebooks, int_id, id, title, author, genre, language, page_count, year, creation_date,
                 modification_date, release_date, purchase_date, is_finished, is_hidden, is_locked, is_new,
                 is_sample, is_store_audio_book, is_explicit, is_ephemeral, is_development, is_proof,
                 rating, computed_rating, content_type, description, cover_url, path, store_id, store_playlist_id):
        self.applebooks = applebooks
        self.int_id= int_id
        self.id = id
        self.title = title
        self.author = author
        self.genre = genre
        self.language = language
        self.page_count = page_count
        self.year = year
        self.creation_date = timestamp_to_datetime(creation_date)
        self.modification_date = timestamp_to_datetime(modification_date)
        self.release_date = timestamp_to_datetime(release_date)
        self.purchase_date = timestamp_to_datetime(purchase_date)
        self.is_finished = is_finished
        self.is_hidden = is_hidden
        self.is_locked = is_locked
        self.is_new = is_new
        self.is_sample = is_sample
        self.is_store_audio_book = is_store_audio_book
        self.is_explicit = is_explicit
        self.is_ephemeral = is_ephemeral
        self.is_development = is_development
        self.is_proof = is_proof
        self.rating = rating
        self.computed_rating = computed_rating
        self.content_type = content_type
        self.description = description
        self.cover_url = cover_url
        self.path = path
        self.store_id = store_id
        self.store_playlist_id = store_playlist_id

    def annotations(self):
        """Return all annotations for this book."""
        cursor = self.applebooks._annotations_db.cursor()
        cursor.execute('SELECT * FROM ZAEANNOTATION WHERE ZANNOTATIONASSETID = ?', [self.id])

        for annotation in cursor.fetchall():
            yield self.applebooks._row_to_annotation(annotation)


    def __repr__(self):
        return f"Book(id={self.id}, title={self.title}, author={self.author}, genre={self.genre})"


class AppleBooks(object):
    def __init__(self, books_path=None, annotations_path=None, *, connect=True):
        if books_path:
            self._books_path = books_path
        else:
            pattern = os.path.expanduser(
                '~/Library/Containers/com.apple.iBooksX/Data/Documents/BKLibrary/*.sqlite'
            )
            matching_files = glob.glob(pattern)
            
            if matching_files:
                self._books_path = matching_files[0]  # Choose the first matching file
            else:
                raise FileNotFoundError("No books .sqlite file found in the specified path.")

        if annotations_path:
            self._annotations_path = annotations_path
        else:
            pattern = os.path.expanduser(
                '~/Library/Containers/com.apple.iBooksX/Data/Documents/AEAnnotation/*.sqlite'
            )
            matching_files = glob.glob(pattern)
            print(matching_files)
            if matching_files:
                self._annotations_path = matching_files[0]  # Choose the first matching file
            else:
                raise FileNotFoundError("No annotation .sqlite file found in the specified path.")

        if connect:
            self.connect()

    def connect(self, **kwds):
        self._books_db = sqlite3.connect(self._books_path, **kwds)
        self._books_db.row_factory = sqlite3.Row

        self._annotations_db = sqlite3.connect(self._annotations_path, **kwds)
        self._annotations_db.row_factory = sqlite3.Row

    def books(self):
        '''
        .. code-block:: sql

            CREATE TABLE ZBKLIBRARYASSET (
                Z_PK INTEGER PRIMARY KEY,
                Z_ENT INTEGER,
                Z_OPT INTEGER,
                ZCANREDOWNLOAD INTEGER,
                ZCOMBINEDSTATE INTEGER,
                ZCOMPUTEDRATING INTEGER,
                ZCONTENTTYPE INTEGER,
                ZDESKTOPSUPPORTLEVEL INTEGER,
                ZDIDRUNFORYOUENDOFBOOKEXPERIENCE INTEGER,
                ZDIDWARNABOUTDESKTOPSUPPORT INTEGER,
                ZFILESIZE INTEGER,
                ZGENERATION INTEGER,
                ZHASRACSUPPORT INTEGER,
                ZISDEVELOPMENT INTEGER,
                ZISDOWNLOADINGSUPPLEMENTALCONTENT INTEGER,
                ZISEPHEMERAL INTEGER,
                ZISEXPLICIT INTEGER,
                ZISFINISHED INTEGER,
                ZISHIDDEN INTEGER,
                ZISLOCKED INTEGER,
                ZISNEW INTEGER,
                ZISPROOF INTEGER,
                ZISSAMPLE INTEGER,
                ZISSTOREAUDIOBOOK INTEGER,
                ZISSUPPLEMENTALCONTENT INTEGER,
                ZISTRACKEDASRECENT INTEGER,
                ZMETADATAMIGRATIONVERSION INTEGER,
                ZNOTFINISHED INTEGER,
                ZPAGECOUNT INTEGER,
                ZRATING INTEGER,
                ZSERIESISCLOUDONLY INTEGER,
                ZSERIESISHIDDEN INTEGER,
                ZSERIESNEXTFLAG INTEGER,
                ZSERIESSORTKEY INTEGER,
                ZSORTKEY INTEGER,
                ZSTATE INTEGER,
                ZTASTE INTEGER,
                ZTASTESYNCEDTOSTORE INTEGER,
                ZLOCALONLYSERIESITEMSPARENT INTEGER,
                ZPURCHASEDANDLOCALPARENT INTEGER,
                ZSERIESCONTAINER INTEGER,
                ZSUPPLEMENTALCONTENTPARENT INTEGER,
                ZASSETDETAILSMODIFICATIONDATE TIMESTAMP,
                ZBOOKHIGHWATERMARKPROGRESS FLOAT,
                ZBOOKMARKSSERVERMAXMODIFICATIONDATE TIMESTAMP,
                ZCREATIONDATE TIMESTAMP,
                ZDATEFINISHED TIMESTAMP,
                ZDURATION FLOAT,
                ZEXPECTEDDATE TIMESTAMP,
                ZFILEONDISKLASTTOUCHDATE TIMESTAMP,
                ZLASTENGAGEDDATE TIMESTAMP,
                ZLASTOPENDATE TIMESTAMP,
                ZLOCATIONSERVERMAXMODIFICATIONDATE TIMESTAMP,
                ZMODIFICATIONDATE TIMESTAMP,
                ZPURCHASEDATE TIMESTAMP,
                ZREADINGPROGRESS FLOAT,
                ZRELEASEDATE TIMESTAMP,
                ZUPDATEDATE TIMESTAMP,
                ZVERSIONNUMBER FLOAT,
                ZACCOUNTID VARCHAR,
                ZASSETGUID VARCHAR,
                ZASSETID VARCHAR,
                ZAUTHOR VARCHAR,
                ZBOOKDESCRIPTION VARCHAR,
                ZBOOKMARKSSERVERVERSION VARCHAR,
                ZCOMMENTS VARCHAR,
                ZCOVERURL VARCHAR,
                ZCOVERWRITINGMODE VARCHAR,
                ZDATASOURCEIDENTIFIER VARCHAR,
                ZDOWNLOADEDDSID VARCHAR,
                ZEPUBID VARCHAR,
                ZFAMILYID VARCHAR,
                ZGENRE VARCHAR,
                ZGROUPING VARCHAR,
                ZKIND VARCHAR,
                ZLANGUAGE VARCHAR,
                ZLOCATIONSERVERVERSION VARCHAR,
                ZPAGEPROGRESSIONDIRECTION VARCHAR,
                ZPATH VARCHAR,
                ZPERMLINK VARCHAR,
                ZPURCHASEDDSID VARCHAR,
                ZSEQUENCEDISPLAYNAME VARCHAR,
                ZSERIESID VARCHAR,
                ZSERIESSTACKIDS VARCHAR,
                ZSORTAUTHOR VARCHAR,
                ZSORTTITLE VARCHAR,
                ZSTOREID VARCHAR,
                ZSTOREPLAYLISTID VARCHAR,
                ZTEMPORARYASSETID VARCHAR,
                ZTITLE VARCHAR,
                ZVERSIONNUMBERHUMANREADABLE VARCHAR,
                ZYEAR VARCHAR
            );
        '''

        cursor = self._books_db.cursor()
        cursor.execute("SELECT * FROM ZBKLIBRARYASSET")

        for book in cursor.fetchall():
            yield self._row_to_book(book)

    def _row_to_book(self, row):
        return Book(
            applebooks=self,
            int_id = row['Z_PK'],
            id=row['ZASSETID'],
            title=row['ZTITLE'],
            author=row['ZAUTHOR'],
            genre=row['ZGENRE'],
            language=row['ZLANGUAGE'],
            page_count=row['ZPAGECOUNT'],
            year=row['ZYEAR'],
            creation_date=row['ZCREATIONDATE'],
            modification_date=row['ZMODIFICATIONDATE'],
            release_date=row['ZRELEASEDATE'],
            purchase_date=row['ZPURCHASEDATE'],
            is_finished=bool(row['ZISFINISHED']),
            is_hidden=bool(row['ZISHIDDEN']),
            is_locked=bool(row['ZISLOCKED']),
            is_new=bool(row['ZISNEW']),
            is_sample=bool(row['ZISSAMPLE']),
            is_store_audio_book=bool(row['ZISSTOREAUDIOBOOK']),
            is_explicit=bool(row['ZISEXPLICIT']),
            is_ephemeral=bool(row['ZISEPHEMERAL']),
            is_development=bool(row['ZISDEVELOPMENT']),
            is_proof=bool(row['ZISPROOF']),
            rating=row['ZRATING'],
            computed_rating=row['ZCOMPUTEDRATING'],
            content_type=row['ZCONTENTTYPE'],
            description=row['ZBOOKDESCRIPTION'],
            cover_url=row['ZCOVERURL'],
            path=row['ZPATH'],
            store_id=row['ZSTOREID'],
            store_playlist_id=row['ZSTOREPLAYLISTID'],
            # Add other columns as needed
        )

    def get_book(self, id):
        cursor = self._book_db.cursor()
        cursor.execute(
            'SELECT * FROM ZBKLIBRARYASSET WHERE ZASSETID = ?', [id])
        row = cursor.fetchone()
        if not row:
            return None
        return self._row_to_book(row)

    def annotations(self):
        '''
        .. code-block:: sql

            CREATE TABLE ZAEANNOTATION (
                Z_PK INTEGER PRIMARY KEY,
                Z_ENT INTEGER,
                Z_OPT INTEGER,
                ZANNOTATIONDELETED INTEGER,
                ZANNOTATIONISUNDERLINE INTEGER,
                ZANNOTATIONSTYLE INTEGER,
                ZANNOTATIONTYPE INTEGER,
                ZPLABSOLUTEPHYSICALLOCATION INTEGER,
                ZPLLOCATIONRANGEEND INTEGER,
                ZPLLOCATIONRANGESTART INTEGER,
                ZANNOTATIONCREATIONDATE TIMESTAMP,
                ZANNOTATIONMODIFICATIONDATE TIMESTAMP,
                ZANNOTATIONASSETID VARCHAR,
                ZANNOTATIONCREATORIDENTIFIER VARCHAR,
                ZANNOTATIONLOCATION VARCHAR,
                ZANNOTATIONNOTE VARCHAR,
                ZANNOTATIONREPRESENTATIVETEXT VARCHAR,
                ZANNOTATIONSELECTEDTEXT VARCHAR,
                ZANNOTATIONUUID VARCHAR,
                ZFUTUREPROOFING1 VARCHAR,
                ZFUTUREPROOFING10 VARCHAR,
                ZFUTUREPROOFING11 VARCHAR,
                ZFUTUREPROOFING12 VARCHAR,
                ZFUTUREPROOFING2 VARCHAR,
                ZFUTUREPROOFING3 VARCHAR,
                ZFUTUREPROOFING4 VARCHAR,
                ZFUTUREPROOFING5 VARCHAR,
                ZFUTUREPROOFING6 VARCHAR,
                ZFUTUREPROOFING7 VARCHAR,
                ZFUTUREPROOFING8 VARCHAR,
                ZFUTUREPROOFING9 VARCHAR,
                ZPLSTORAGEUUID VARCHAR,
                ZPLUSERDATA BLOB
            );
        '''

        cursor = self._annotations_db.cursor()
        cursor.execute("select * from ZAEANNOTATION")

        for annotation in cursor.fetchall():
            yield self._row_to_annotation(annotation)

    def _row_to_annotation(self, row):
        return Annotation(
            id=row['ZANNOTATIONUUID'],
            asset_id=row['ZANNOTATIONASSETID'],
            creator_identifier=row['ZANNOTATIONCREATORIDENTIFIER'],
            location=EpubCFI(row['ZANNOTATIONLOCATION']),
            note=row['ZANNOTATIONNOTE'],
            representative_text=row['ZANNOTATIONREPRESENTATIVETEXT'],
            selected_text=row['ZANNOTATIONSELECTEDTEXT'],
            type=row['ZANNOTATIONTYPE'],
            style=row['ZANNOTATIONSTYLE'],
            deleted=bool(row['ZANNOTATIONDELETED']),
            is_underline=bool(row['ZANNOTATIONISUNDERLINE']),
            absolute_physical_location=row['ZPLABSOLUTEPHYSICALLOCATION'],
            location_range_end=row['ZPLLOCATIONRANGEEND'],
            location_range_start=row['ZPLLOCATIONRANGESTART'],
            creation_date=row['ZANNOTATIONCREATIONDATE'],
            modification_date=row['ZANNOTATIONMODIFICATIONDATE'],
            storage_uuid=row['ZPLSTORAGEUUID'],
            user_data=row['ZPLUSERDATA']
        )
