from flask import Flask, request, jsonify, send_file
from flask_cors import CORS
from datetime import datetime
import os
import zipfile
import sqlite3
import shutil
import uuid
import time


app = Flask(__name__)
app.config['PROPAGATE_EXCEPTIONS'] = True
CORS(app, origins=["https://jwmerge.netlify.app"])

UPLOAD_FOLDER = "uploads"
EXTRACT_FOLDER = "extracted"

os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(EXTRACT_FOLDER, exist_ok=True)


def get_current_local_iso8601():
    now_local = datetime.datetime.now()
    return now_local.strftime("%Y-%m-%dT%H:%M:%S")


def checkpoint_db(db_path):
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("PRAGMA wal_checkpoint(FULL)")
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"Erreur lors du checkpoint de {db_path}: {e}")


def list_tables(db_path):
    """
    Retourne une liste des noms de tables présentes dans la base de données
    spécifiée par 'db_path', en excluant les tables système (commençant par 'sqlite_').
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("""
        SELECT name
        FROM sqlite_master
        WHERE type='table'
          AND name NOT LIKE 'sqlite_%'
    """)
    tables = [row[0] for row in cursor.fetchall()]
    conn.close()
    return tables


def merge_independent_media(merged_db_path, file1_db, file2_db):
    """
    Fusionne la table IndependentMedia des deux bases sources dans la base fusionnée.
    Deux lignes sont considérées identiques si (OriginalFilename, FilePath, Hash) sont identiques.
    Si une ligne existe déjà mais avec un MimeType différent, le MimeType est mis à jour.
    Retourne un mapping : {(db_source, ancien_ID) : nouveau_ID, ...}
    """
    print("\n[FUSION INDEPENDENTMEDIA]")
    mapping = {}
    merged_conn = sqlite3.connect(merged_db_path)
    merged_cursor = merged_conn.cursor()

    for db_path in [file1_db, file2_db]:
        print(f"Traitement de {db_path}")
        with sqlite3.connect(db_path) as src_conn:
            src_cursor = src_conn.cursor()
            src_cursor.execute("""
                SELECT IndependentMediaId, OriginalFilename, FilePath, MimeType, Hash
                FROM IndependentMedia
            """)
            rows = src_cursor.fetchall()

            for row in rows:
                old_id, orig_fn, file_path, mime, hash_val = row
                print(f"  - Média : {orig_fn}, Hash={hash_val}")

                merged_cursor.execute("""
                    SELECT IndependentMediaId, MimeType
                    FROM IndependentMedia
                    WHERE OriginalFilename = ? AND FilePath = ? AND Hash = ?
                """, (orig_fn, file_path, hash_val))
                result = merged_cursor.fetchone()

                if result:
                    new_id, existing_mime = result
                    if existing_mime != mime:
                        print(f"    > Mise à jour MimeType pour ID {new_id}")
                        merged_cursor.execute("""
                            UPDATE IndependentMedia
                            SET MimeType = ?
                            WHERE IndependentMediaId = ?
                        """, (mime, new_id))
                else:
                    merged_cursor.execute("""
                        INSERT INTO IndependentMedia (OriginalFilename, FilePath, MimeType, Hash)
                        VALUES (?, ?, ?, ?)
                    """, (orig_fn, file_path, mime, hash_val))
                    new_id = merged_cursor.lastrowid
                    print(f"    > Insertion nouvelle ligne ID {new_id}")

                mapping[(db_path, old_id)] = new_id

    merged_conn.commit()
    merged_conn.close()
    print("Fusion IndependentMedia terminée.")
    return mapping


def read_notes_and_highlights(db_path):
    if not os.path.exists(db_path):
        return {"error": f"Base de données introuvable : {db_path}"}
    checkpoint_db(db_path)
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute("""
        SELECT n.NoteId, n.Guid, n.Title, n.Content, n.LocationId, um.UserMarkGuid,
               n.LastModified, n.Created, n.BlockType, n.BlockIdentifier
        FROM Note n
        LEFT JOIN UserMark um ON n.UserMarkId = um.UserMarkId
    """)
    notes = cursor.fetchall()

    cursor.execute("""
        SELECT UserMarkId, ColorIndex, LocationId, StyleIndex, UserMarkGuid, Version
        FROM UserMark
    """)
    highlights = cursor.fetchall()

    conn.close()
    return {"notes": notes, "highlights": highlights}


def extract_file(file_path, extract_folder):
    zip_path = file_path.replace(".jwlibrary", ".zip")
    if os.path.exists(zip_path):
        os.remove(zip_path)
    os.rename(file_path, zip_path)
    extract_full_path = os.path.join(EXTRACT_FOLDER, extract_folder)
    os.makedirs(extract_full_path, exist_ok=True)
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(extract_full_path)
    return extract_full_path


def create_merged_schema(merged_db_path, base_db_path):
    checkpoint_db(base_db_path)
    src_conn = sqlite3.connect(base_db_path)
    src_cursor = src_conn.cursor()
    src_cursor.execute(
        "SELECT type, name, sql FROM sqlite_master "
        "WHERE type IN ('table', 'index', 'trigger', 'view') "
        "AND name NOT LIKE 'sqlite_%'"
    )
    schema_items = src_cursor.fetchall()
    src_conn.close()

    merged_conn = sqlite3.connect(merged_db_path)
    merged_cursor = merged_conn.cursor()
    for obj_type, name, sql in schema_items:
        # On exclut la table (et triggers associés) LastModified
        if (obj_type == 'table' and name == "LastModified") or (obj_type == 'trigger' and "LastModified" in sql):
            continue
        if sql:
            try:
                merged_cursor.execute(sql)
            except Exception as e:
                print(f"Erreur lors de la création de {obj_type} '{name}': {e}")
    merged_conn.commit()

    try:
        merged_cursor.execute("DROP TABLE IF EXISTS LastModified")
        merged_cursor.execute("CREATE TABLE LastModified (LastModified TEXT NOT NULL)")
    except Exception as e:
        print(f"Erreur lors de la création de la table LastModified: {e}")
    merged_conn.commit()

    # Vérification et création de PlaylistItemMap si elle n'existe pas
    merged_cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='PlaylistItemMap'")
    if not merged_cursor.fetchone():
        try:
            merged_cursor.execute("""
                CREATE TABLE PlaylistItemMap (
                    PlaylistItemMapId INTEGER PRIMARY KEY AUTOINCREMENT,
                    PlaylistItemId INTEGER NOT NULL,
                    AutreColonne TEXT,  -- Remplace ou complète ces colonnes selon tes besoins
                    UNIQUE(PlaylistItemId)
                )
            """)
            print("PlaylistItemMap créée dans la base fusionnée.")
        except Exception as e:
            print(f"Erreur lors de la création de PlaylistItemMap: {e}")
    merged_conn.commit()
    merged_conn.close()


def create_table_if_missing(merged_conn, source_db_paths, table):
    cursor = merged_conn.cursor()
    cursor.execute(f"PRAGMA table_info({table})")
    if cursor.fetchone() is None:
        create_sql = None
        for db_path in source_db_paths:
            checkpoint_db(db_path)
            src_conn = sqlite3.connect(db_path)
            src_cursor = src_conn.cursor()
            src_cursor.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name=?", (table,))
            row = src_cursor.fetchone()
            src_conn.close()
            if row and row[0]:
                create_sql = row[0]
                break
        if create_sql:
            try:
                merged_conn.execute(create_sql)
                print(f"Table {table} créée dans la base fusionnée.")
            except Exception as e:
                print(f"Erreur lors de la création de {table}: {e}")
        else:
            print(f"Aucun schéma trouvé pour la table {table} dans les bases sources.")


def merge_other_tables(merged_db_path, db1_path, db2_path, exclude_tables=None):
    if exclude_tables is None:
        exclude_tables = ["Note", "UserMark"]
    checkpoint_db(db1_path)
    checkpoint_db(db2_path)
    conn_db1 = sqlite3.connect(db1_path)
    cursor_db1 = conn_db1.cursor()
    cursor_db1.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
    tables1 = {row[0] for row in cursor_db1.fetchall()}
    conn_db1.close()
    conn_db2 = sqlite3.connect(db2_path)
    cursor_db2 = conn_db2.cursor()
    cursor_db2.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
    tables2 = {row[0] for row in cursor_db2.fetchall()}
    conn_db2.close()
    all_tables = (tables1 | tables2) - set(exclude_tables)
    merged_conn = sqlite3.connect(merged_db_path)
    merged_cursor = merged_conn.cursor()
    source_db_paths = [db1_path, db2_path]
    for table in all_tables:
        create_table_if_missing(merged_conn, source_db_paths, table)
        merged_cursor.execute(f"PRAGMA table_info({table})")
        columns_info = merged_cursor.fetchall()
        if not columns_info:
            print(f"Table {table} n'a pas pu être créée dans la base fusionnée, on passe.")
            continue
        columns = [col[1] for col in columns_info]
        columns_joined = ", ".join(columns)
        placeholders = ", ".join(["?"] * len(columns))
        conn_db1 = sqlite3.connect(db1_path)
        cursor_db1 = conn_db1.cursor()
        try:
            cursor_db1.execute(f"SELECT * FROM {table}")
            rows1 = cursor_db1.fetchall()
        except Exception as e:
            rows1 = []
            print(f"Erreur lors de la lecture de {table} dans db1: {e}")
        conn_db1.close()
        conn_db2 = sqlite3.connect(db2_path)
        cursor_db2 = conn_db2.cursor()
        try:
            cursor_db2.execute(f"SELECT * FROM {table}")
            rows2 = cursor_db2.fetchall()
        except Exception as e:
            rows2 = []
            print(f"Erreur lors de la lecture de {table} dans db2: {e}")
        conn_db2.close()
        merged_rows = list(rows1)
        for row in rows2:
            if row not in merged_rows:
                merged_rows.append(row)
        for row in merged_rows:
            try:
                if table == "BlockRange":
                    existing = merged_cursor.execute("SELECT * FROM BlockRange WHERE BlockRangeId = ?", (row[0],)).fetchone()
                    if not existing:
                        merged_cursor.execute(f"INSERT INTO {table} ({columns_joined}) VALUES ({placeholders})", row)
                    else:
                        if (existing[2] != row[2] or existing[3] != row[3] or existing[4] != row[4] or existing[5] != row[5]):
                            cur_max = merged_cursor.execute("SELECT MAX(BlockRangeId) FROM BlockRange").fetchone()[0] or 0
                            new_id = cur_max + 1
                            new_row = (new_id,) + row[1:]
                            merged_cursor.execute(f"INSERT INTO {table} ({columns_joined}) VALUES ({placeholders})", new_row)
                else:
                    merged_cursor.execute(f"INSERT INTO {table} ({columns_joined}) VALUES ({placeholders})", row)
            except Exception as e:
                print(f"Erreur lors de l'insertion dans la table {table}: {e}")
    merged_conn.commit()
    merged_conn.close()


def merge_bookmarks(merged_db_path, file1_db, file2_db, location_id_map):
    """
    Fusionne les bookmarks tout en respectant les doublons sur (PublicationLocationId, Slot).
    Retourne un mapping { (db_path, old_id) : new_id } si tu veux l'étendre plus tard.
    """
    conn = sqlite3.connect(merged_db_path)
    cursor = conn.cursor()
    mapping = {}

    for db_path in [file1_db, file2_db]:
        with sqlite3.connect(db_path) as src_conn:
            src_cursor = src_conn.cursor()

            # DEBUG pour repérer si Playlist existe
            print(f"\nDEBUG: Contenu de {db_path} avant fusion:")
            src_cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = [t[0] for t in src_cursor.fetchall()]
            print(f"Tables disponibles: {tables}")

            if 'Bookmark' not in tables:
                print(f"Aucune table Bookmark dans {db_path}")
                continue

            src_cursor.execute("""
                SELECT BookmarkId, LocationId, PublicationLocationId, Slot, Title, 
                       Snippet, BlockType, BlockIdentifier
                FROM Bookmark
            """)
            for row in src_cursor.fetchall():
                old_id, loc_id, pub_loc_id, slot, title, snippet, block_type, block_id = row
                new_loc_id = location_id_map.get((db_path, loc_id), loc_id)
                new_pub_loc_id = location_id_map.get((db_path, pub_loc_id), pub_loc_id)

                cursor.execute("SELECT 1 FROM Location WHERE LocationId IN (?, ?)", (new_loc_id, new_pub_loc_id))
                if len(cursor.fetchall()) != 2:
                    print(f"⚠️  Location {new_loc_id} ou {new_pub_loc_id} manquante - Bookmark ignoré")
                    continue

                cursor.execute("""
                    SELECT 1 FROM Bookmark 
                    WHERE PublicationLocationId = ? AND Slot = ?
                """, (new_pub_loc_id, slot))
                if cursor.fetchone():
                    print(f"Bookmark ignoré : doublon PublicationLocationId={new_pub_loc_id}, Slot={slot}")
                    continue

                cursor.execute("""
                    INSERT INTO Bookmark
                    (LocationId, PublicationLocationId, Slot, Title,
                     Snippet, BlockType, BlockIdentifier)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (new_loc_id, new_pub_loc_id, slot, title, snippet, block_type, block_id))
                # Pas besoin d'indiquer BookmarkId, il sera auto-généré
                mapping[(db_path, old_id)] = cursor.lastrowid

    conn.commit()
    conn.close()
    return mapping


def merge_usermark_with_id_relabeling(merged_db_path, source_db_path):
    conn_merged = sqlite3.connect(merged_db_path)
    cur_merged = conn_merged.cursor()
    cur_merged.execute("SELECT UserMarkId FROM UserMark")
    existing_ids = set(row[0] for row in cur_merged.fetchall())
    current_max_id = max(existing_ids) if existing_ids else 0
    conn_source = sqlite3.connect(source_db_path)
    cur_source = conn_source.cursor()
    cur_source.execute("SELECT UserMarkId, ColorIndex, LocationId, StyleIndex, UserMarkGuid, Version FROM UserMark")
    source_rows = cur_source.fetchall()
    conn_source.close()
    replacements = {}
    for row in source_rows:
        old_id = row[0]
        if old_id in existing_ids:
            current_max_id += 1
            new_id = current_max_id
            replacements[old_id] = new_id
        else:
            replacements[old_id] = old_id
            existing_ids.add(old_id)
    for row in source_rows:
        old_id = row[0]
        new_id = replacements[old_id]
        ColorIndex = row[1]
        LocationId = row[2]
        StyleIndex = row[3]
        UserMarkGuid = row[4]
        Version = row[5]
        try:
            cur_merged.execute("""
                INSERT INTO UserMark (UserMarkId, ColorIndex, LocationId, StyleIndex, UserMarkGuid, Version)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (new_id, ColorIndex, LocationId, StyleIndex, UserMarkGuid, Version))
        except Exception as e:
            print(f"Erreur insertion UserMark old_id={old_id}, new_id={new_id}: {e}")
    conn_merged.commit()
    conn_merged.close()
    return replacements


def merge_blockrange_from_two_sources(merged_db_path, file1_db, file2_db):
    # 1) Récupère le maximum des BlockRangeId déjà présents
    conn_merged = sqlite3.connect(merged_db_path)
    cur_merged = conn_merged.cursor()
    cur_merged.execute("SELECT MAX(BlockRangeId) FROM BlockRange")
    row = cur_merged.fetchone()
    current_max_id = row[0] if row and row[0] is not None else 0
    conn_merged.close()

    # 2) Fonction interne pour lire la table BlockRange d'une source
    def read_blockrange(db_path):
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()
        cur.execute("SELECT BlockRangeId, BlockType, Identifier, StartToken, EndToken, UserMarkId FROM BlockRange")
        rows = cur.fetchall()
        conn.close()
        return rows

    # 3) Lecture des deux sources
    rows1 = read_blockrange(file1_db)
    rows2 = read_blockrange(file2_db)
    combined_rows = rows1 + rows2

    # 4) Fusion : clé = (old_id, Identifier, StartToken, EndToken, UserMarkId)
    blockrange_replacements = {}
    existing_ids = set()

    # On recharge les IDs existants (pour détecter si old_id existe déjà)
    conn_merged = sqlite3.connect(merged_db_path)
    cur_merged = conn_merged.cursor()
    cur_merged.execute("SELECT BlockRangeId FROM BlockRange")
    for r in cur_merged.fetchall():
        existing_ids.add(r[0])
    conn_merged.close()

    # 5) Parcours de toutes les lignes
    for row in combined_rows:
        old_id, BlockType, Identifier, StartToken, EndToken, user_mark_id = row
        key = (old_id, Identifier, StartToken, EndToken, user_mark_id)

        # Si la ligne est déjà traitée, on saute
        if key in blockrange_replacements:
            continue

        # Ouvre la connexion à chaque insertion pour éviter conflits
        conn_merged = sqlite3.connect(merged_db_path)
        cur_merged = conn_merged.cursor()

        if old_id in existing_ids:
            # Cet ID est déjà utilisé => on génère un nouvel ID
            current_max_id += 1
            new_id = current_max_id
            # On supprime la ligne existante avec old_id pour éviter les doublons
            cur_merged.execute("DELETE FROM BlockRange WHERE BlockRangeId = ?", (old_id,))
        else:
            # Sinon, on garde old_id
            new_id = old_id
            existing_ids.add(old_id)
            current_max_id = max(current_max_id, new_id)

        # On utilise le même new_id pour la colonne UserMarkId
        blockrange_replacements[key] = new_id
        new_row = (new_id, BlockType, Identifier, StartToken, EndToken, new_id)

        try:
            cur_merged.execute("""
                INSERT INTO BlockRange (BlockRangeId, BlockType, Identifier, StartToken, EndToken, UserMarkId)
                VALUES (?, ?, ?, ?, ?, ?)
            """, new_row)
        except Exception as e:
            print(f"Erreur insertion BlockRange, row {new_row}: {e}")

        conn_merged.commit()
        conn_merged.close()

    return blockrange_replacements


def update_location_references(merged_db_path, location_replacements):
    conn = sqlite3.connect(merged_db_path)
    cursor = conn.cursor()
    tables_with_single_location = ["InputField", "Note", "PlaylistItemLocationMap", "TagMap", "UserMark"]
    for table in tables_with_single_location:
        for old_loc, new_loc in location_replacements.items():
            try:
                cursor.execute(f"UPDATE {table} SET LocationId = ? WHERE LocationId = ?", (new_loc, old_loc))
                print(f"{table} mis à jour: LocationId {old_loc} -> {new_loc}")
            except Exception as e:
                print(f"Erreur mise à jour {table} pour LocationId {old_loc}: {e}")
    # Pour Bookmark, mettre à jour LocationId et PublicationLocationId
    for old_loc, new_loc in location_replacements.items():
        try:
            cursor.execute("UPDATE Bookmark SET LocationId = ? WHERE LocationId = ?", (new_loc, old_loc))
            cursor.execute("UPDATE Bookmark SET PublicationLocationId = ? WHERE PublicationLocationId = ?", (new_loc, old_loc))
            print(f"Bookmark mis à jour: LocationId {old_loc} -> {new_loc} et PublicationLocationId {old_loc} -> {new_loc}")
        except Exception as e:
            print(f"Erreur mise à jour Bookmark pour LocationId {old_loc}: {e}")
    conn.commit()
    conn.close()


def merge_usermark_from_sources(merged_db_path, file1_db, file2_db, location_id_map):
    def read_usermarks(db_path):
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()
        cur.execute("""
            SELECT UserMarkId, ColorIndex, LocationId, StyleIndex, UserMarkGuid, Version
            FROM UserMark
        """)
        rows = cur.fetchall()
        conn.close()
        return rows

    # Lire les usermarks des deux bases
    usermarks1 = read_usermarks(file1_db)
    usermarks2 = read_usermarks(file2_db)
    all_usermarks = usermarks1 + usermarks2

    conn = sqlite3.connect(merged_db_path)
    cur = conn.cursor()

    # Récupérer le maximum des UserMarkId existants
    cur.execute("SELECT MAX(UserMarkId) FROM UserMark")
    max_id = cur.fetchone()[0] or 0

    # Dictionnaire pour mapper les anciens IDs aux nouveaux
    usermark_id_map = {}

    for um in all_usermarks:
        um_id = um[0]
        # Mapper le LocationId si nécessaire
        mapped_loc_id = location_id_map.get(um[2], um[2])

        # Vérifier si ce UserMark existe déjà (par GUID)
        cur.execute("SELECT UserMarkId FROM UserMark WHERE UserMarkGuid=?", (um[4],))
        existing = cur.fetchone()

        if existing:
            usermark_id_map[um_id] = existing[0]
        else:
            max_id += 1
            new_um = (max_id, um[1], mapped_loc_id, um[3], um[4], um[5])
            try:
                cur.execute("""
                    INSERT INTO UserMark 
                    (UserMarkId, ColorIndex, LocationId, StyleIndex, UserMarkGuid, Version)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, new_um)
                usermark_id_map[um_id] = max_id
            except sqlite3.IntegrityError:
                # En cas de conflit, on incrémente à nouveau et on réessaye
                max_id += 1
                new_um = (max_id, um[1], mapped_loc_id, um[3], um[4], um[5])
                cur.execute("""
                    INSERT INTO UserMark 
                    (UserMarkId, ColorIndex, LocationId, StyleIndex, UserMarkGuid, Version)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, new_um)
                usermark_id_map[um_id] = max_id

    conn.commit()
    conn.close()
    return usermark_id_map


def insert_usermark_if_needed(conn, usermark_tuple):
    """
    usermark_tuple = (UserMarkId, ColorIndex, LocationId, StyleIndex, UserMarkGuid, Version)
    """
    (um_id, color, loc, style, guid, version) = usermark_tuple

    cur = conn.cursor()
    # Vérifie si un usermark avec ce UserMarkGuid existe déjà
    existing = cur.execute("SELECT UserMarkId, ColorIndex, LocationId, StyleIndex, Version FROM UserMark WHERE UserMarkGuid=?", (guid,)).fetchone()
    if existing:
        # Si la ligne est identique, on skip. Sinon, on peut décider de faire un UPDATE
        (ex_id, ex_color, ex_loc, ex_style, ex_version) = existing
        if ex_color == color and ex_loc == loc and ex_style == style and ex_version == version:
            print(f"UserMarkGuid={guid} déjà présent, identique, on skip l'insertion.")
            return
        else:
            print(f"UserMarkGuid={guid} existe déjà avec des différences, on peut soit faire un UPDATE, soit générer un nouveau guid.")
            # Par exemple, on fait un UPDATE:
            cur.execute("""
                UPDATE UserMark
                SET ColorIndex=?, LocationId=?, StyleIndex=?, Version=?
                WHERE UserMarkGuid=?
            """, (color, loc, style, version, guid))
            return
    else:
        # Si pas trouvé, on insère
        try:
            cur.execute("""
                INSERT INTO UserMark (UserMarkId, ColorIndex, LocationId, StyleIndex, UserMarkGuid, Version)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (um_id, color, loc, style, guid, version))
        except Exception as e:
            print(f"Erreur insertion usermark {um_id} guid={guid}: {e}")


def merge_location_from_sources(merged_db_path, file1_db, file2_db):
    """
    Fusionne la table Location des deux bases sources dans la base fusionnée.
    Deux lignes sont considérées identiques si toutes les colonnes critiques sont égales :
    (BookNumber, ChapterNumber, DocumentId, Track, IssueTagNumber, KeySymbol, MepsLanguage, Type, Title).
    Retourne un mapping : {(db_source, ancien_LocationId) : nouveau_LocationId, ...}
    """
    def read_locations(db_path):
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()
        cur.execute("""
            SELECT LocationId, BookNumber, ChapterNumber, DocumentId, Track, IssueTagNumber, KeySymbol, MepsLanguage, Type, Title
            FROM Location
        """)
        rows = cur.fetchall()
        conn.close()
        # On garde l'info du chemin source pour le mapping
        return [(db_path, ) + row for row in rows]

    # Lecture des données des deux sources
    locations = read_locations(file1_db) + read_locations(file2_db)

    conn = sqlite3.connect(merged_db_path)
    cur = conn.cursor()

    # Optionnel : on vide la table Location dans la DB fusionnée pour repartir de zéro
    cur.execute("DELETE FROM Location")

    location_id_map = {}    # Mapping final : {(db_source, old_LocationId) : new_LocationId}
    existing_keys = {}      # Pour garder la trace des lignes déjà insérées
    current_max_id = 0

    def safe_str(val):
        return str(val) if val is not None else "NULL"

    for entry in locations:
        db_source = entry[0]
        old_loc_id, book_num, chap_num, doc_id, track, issue, key_sym, meps_lang, loc_type, title = entry[1:]

        # Construction de la clé de matching sur toutes les colonnes critiques
        key = (
            safe_str(book_num),
            safe_str(chap_num),
            safe_str(doc_id),
            safe_str(track),
            safe_str(issue),
            safe_str(key_sym),
            safe_str(meps_lang),
            safe_str(loc_type),
            safe_str(title)
        )

        if key in existing_keys:
            new_loc_id = existing_keys[key]
        else:
            current_max_id += 1
            new_loc_id = current_max_id
            existing_keys[key] = new_loc_id
            new_row = (new_loc_id, book_num, chap_num, doc_id, track, issue, key_sym, meps_lang, loc_type, title)
            try:
                cur.execute("""
                    INSERT INTO Location
                    (LocationId, BookNumber, ChapterNumber, DocumentId, Track, IssueTagNumber, KeySymbol, MepsLanguage, Type, Title)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, new_row)
            except sqlite3.IntegrityError as e:
                print(f"Erreur insertion Location pour {new_row}: {e}")
        # Enregistre le mapping pour cette source et cet ancien ID
        location_id_map[(db_source, old_loc_id)] = new_loc_id

    conn.commit()
    conn.close()
    return location_id_map


@app.route('/upload', methods=['GET', 'POST'])
def upload_files():
    if request.method == 'GET':
        response = jsonify({"message": "Route /upload fonctionne (GET) !"})
        response.headers.add("Access-Control-Allow-Origin", "*")
        return response, 200

    if 'file1' not in request.files or 'file2' not in request.files:
        return jsonify({"error": "Veuillez envoyer deux fichiers userData.db"}), 400

    file1 = request.files['file1']
    file2 = request.files['file2']

    # Définir les dossiers d'extraction (où sera placé chaque fichier userData.db)
    extracted1 = os.path.join("extracted", "file1_extracted")
    extracted2 = os.path.join("extracted", "file2_extracted")

    os.makedirs(extracted1, exist_ok=True)
    os.makedirs(extracted2, exist_ok=True)

    # Supprimer les anciens fichiers s'ils existent
    file1_path = os.path.join(extracted1, "userData.db")
    file2_path = os.path.join(extracted2, "userData.db")

    if os.path.exists(file1_path):
        os.remove(file1_path)
    if os.path.exists(file2_path):
        os.remove(file2_path)

    # Sauvegarde des fichiers userData.db
    file1.save(file1_path)
    file2.save(file2_path)

    response = jsonify({"message": "Fichiers userData.db reçus et enregistrés avec succès !"})
    response.headers.add("Access-Control-Allow-Origin", "*")
    return response, 200


@app.route('/analyze', methods=['GET'])
def validate_db_path(db_path):
    if not os.path.exists(db_path):
        raise FileNotFoundError(f"Database not found: {db_path}")


def analyze_files():
    try:
        file1_db = os.path.join(EXTRACT_FOLDER, "file1_extracted", "userData.db")
        file2_db = os.path.join(EXTRACT_FOLDER, "file2_extracted", "userData.db")

        validate_db_path(file1_db)  # Validation ajoutée
        validate_db_path(file2_db)  # Validation ajoutée
        data1 = read_notes_and_highlights(file1_db)
        data2 = read_notes_and_highlights(file2_db)
        response = jsonify({"file1": data1, "file2": data2})
        response.headers.add("Access-Control-Allow-Origin", "*")

    except FileNotFoundError as e:
        return jsonify({"error": str(e)}), 404
    except Exception as e:
        app.logger.error(f"Analyze error: {str(e)}")
        return jsonify({"error": "Internal server error"}), 500


@app.route('/compare', methods=['GET'])
def compare_data():
    file1_db = os.path.join(EXTRACT_FOLDER, "file1_extracted", "userData.db")
    file2_db = os.path.join(EXTRACT_FOLDER, "file2_extracted", "userData.db")
    data1 = read_notes_and_highlights(file1_db)
    data2 = read_notes_and_highlights(file2_db)
    notes1 = {note[1]: note[2] for note in data1.get("notes", [])}
    notes2 = {note[1]: note[2] for note in data2.get("notes", [])}
    identical_notes = {}
    conflicts_notes = {}
    unique_notes_file1 = {}
    unique_notes_file2 = {}
    for title in set(notes1.keys()).intersection(set(notes2.keys())):
        if notes1[title] == notes2[title]:
            identical_notes[title] = notes1[title]
        else:
            conflicts_notes[title] = {"file1": notes1[title], "file2": notes2[title]}
    for title in set(notes1.keys()).difference(set(notes2.keys())):
        unique_notes_file1[title] = notes1[title]
    for title in set(notes2.keys()).difference(set(notes1.keys())):
        unique_notes_file2[title] = notes2[title]
    highlights1 = {h[1]: h[2] for h in data1.get("highlights", [])}
    highlights2 = {h[1]: h[2] for h in data2.get("highlights", [])}
    identical_highlights = {}
    conflicts_highlights = {}
    unique_highlights_file1 = {}
    unique_highlights_file2 = {}
    for loc in set(highlights1.keys()).intersection(set(highlights2.keys())):
        if highlights1[loc] == highlights2[loc]:
            identical_highlights[loc] = highlights1[loc]
        else:
            conflicts_highlights[loc] = {"file1": highlights1[loc], "file2": highlights2[loc]}
    for loc in set(highlights1.keys()).difference(set(highlights2.keys())):
        unique_highlights_file1[loc] = highlights1[loc]
    for loc in set(highlights2.keys()).difference(set(highlights1.keys())):
        unique_highlights_file2[loc] = highlights2[loc]
    result = {
        "notes": {
            "identical": identical_notes,
            "conflicts": conflicts_notes,
            "unique_file1": unique_notes_file1,
            "unique_file2": unique_notes_file2
        },
        "highlights": {
            "identical": identical_highlights,
            "conflicts": conflicts_highlights,
            "unique_file1": unique_highlights_file1,
            "unique_file2": unique_highlights_file2
        }
    }
    return jsonify(result), 200


def merge_tags_and_tagmap(merged_db_path, file1_db, file2_db, note_mapping, location_id_map, playlist_item_id_map):
    """
    Fusionne les Tags et la table TagMap.
    Deux entrées de TagMap sont considérées identiques si, après mise à jour via les mappings,
    la référence pertinente (NoteId, LocationId ou PlaylistItemId), le TagId et la Position sont identiques.
    """
    print("\n[FUSION TAGS ET TAGMAP]")
    conn = sqlite3.connect(merged_db_path)
    cursor = conn.cursor()

    # === 1. Fusion des Tags ===
    cursor.execute("SELECT COALESCE(MAX(TagId), 0) FROM Tag")
    max_tag_id = cursor.fetchone()[0]
    tag_id_map = {}

    for db_path in [file1_db, file2_db]:
        with sqlite3.connect(db_path) as src_conn:
            src_cursor = src_conn.cursor()
            src_cursor.execute("SELECT TagId, Type, Name FROM Tag")
            for tag_id, tag_type, tag_name in src_cursor.fetchall():
                cursor.execute("SELECT TagId FROM Tag WHERE Type=? AND Name=?", (tag_type, tag_name))
                existing_tag = cursor.fetchone()
                if existing_tag:
                    tag_id_map[(db_path, tag_id)] = existing_tag[0]
                else:
                    max_tag_id += 1
                    cursor.execute("INSERT INTO Tag (TagId, Type, Name) VALUES (?, ?, ?)",
                                   (max_tag_id, tag_type, tag_name))
                    tag_id_map[(db_path, tag_id)] = max_tag_id

    # === 2. Fusion des TagMap ===
    cursor.execute("SELECT COALESCE(MAX(TagMapId), 0) FROM TagMap")
    max_tagmap_id = cursor.fetchone()[0]
    tagmap_id_map = {}

    for db_path in [file1_db, file2_db]:
        with sqlite3.connect(db_path) as src_conn:
            src_cursor = src_conn.cursor()
            src_cursor.execute("""
                SELECT TagMapId, PlaylistItemId, LocationId, NoteId, TagId, Position
                FROM TagMap
            """)
            for row in src_cursor.fetchall():
                old_tagmap_id, playlist_item_id, location_id, note_id, tag_id, position = row

                # Mapping des IDs
                new_note_id = note_mapping.get((db_path, note_id)) if note_id else None
                normalized_key = (os.path.normpath(db_path), location_id)
                normalized_map = {(os.path.normpath(k[0]), k[1]): v for k, v in location_id_map.items()}
                new_location_id = normalized_map.get(normalized_key) if location_id else None
                new_playlist_item_id = playlist_item_id_map.get((db_path, playlist_item_id)) if playlist_item_id else None
                new_tag_id = tag_id_map.get((db_path, tag_id))

                print(f"[DEBUG] TagMap {old_tagmap_id} ({db_path}) : "
                      f"NoteId={note_id}->{new_note_id}, "
                      f"LocId={location_id}->{new_location_id}, "
                      f"ItemId={playlist_item_id}->{new_playlist_item_id}")

                # Refuser seulement si toutes les références sont nulles
                if all(v is None for v in [new_note_id, new_location_id, new_playlist_item_id]):
                    print(f"❌ Aucune référence valide pour TagMap {old_tagmap_id} (db: {db_path})")
                    continue

                # Vérifie si une ligne identique existe déjà
                cursor.execute("""
                    SELECT 1 FROM TagMap 
                    WHERE TagId = ? AND Position = ? AND
                          ((NoteId IS ? OR NoteId = ?) AND
                           (LocationId IS ? OR LocationId = ?) AND
                           (PlaylistItemId IS ? OR PlaylistItemId = ?))
                """, (
                    new_tag_id, position,
                    new_note_id, new_note_id,
                    new_location_id, new_location_id,
                    new_playlist_item_id, new_playlist_item_id
                ))
                if cursor.fetchone():
                    print(f"❌ Doublon exact détecté — TagMap ignoré (TagId={new_tag_id}, Pos={position})")
                    continue

                # Insertion
                max_tagmap_id += 1
                try:
                    cursor.execute("""
                        INSERT INTO TagMap (TagMapId, PlaylistItemId, LocationId, NoteId, TagId, Position)
                        VALUES (?, ?, ?, ?, ?, ?)
                    """, (
                        max_tagmap_id,
                        new_playlist_item_id,
                        new_location_id,
                        new_note_id,
                        new_tag_id,
                        position
                    ))
                    tagmap_id_map[(db_path, old_tagmap_id)] = max_tagmap_id
                    print(f"✅ Insertion TagMap {old_tagmap_id} OK")
                except sqlite3.IntegrityError as e:
                    print(f"❌ Erreur insertion TagMap {old_tagmap_id}: {e}")

    conn.commit()
    conn.close()
    return tag_id_map, tagmap_id_map


def merge_playlist_items(merged_db_path, file1_db, file2_db, im_mapping=None):
    """
    Fusionne les PlaylistItem des deux bases sources dans la DB fusionnée.
    Deux lignes sont considérées identiques si (Label, StartTrimOffsetTicks, EndTrimOffsetTicks, Accuracy, EndAction, ThumbnailFilePath)
    sont strictement identiques (après normalisation).
    Si im_mapping est fourni, il peut être utilisé pour mettre à jour le ThumbnailFilePath.
    Retourne un mapping : {(db_source, ancien_PlaylistItemId) : nouveau_PlaylistItemId, ...}
    """
    print("\n[FUSION PLAYLISTITEMS]")
    # Fonctions de normalisation

    def safe_text(val):
        return val if val is not None else ""

    def safe_number(val):
        return val if val is not None else 0

    with sqlite3.connect(merged_db_path, timeout=30) as conn:
        conn.execute("PRAGMA busy_timeout = 10000")
        cursor = conn.cursor()
        # Vérifier que la table PlaylistItem existe
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='PlaylistItem'")
        if not cursor.fetchone():
            print("[ERREUR] La table PlaylistItem n'existe pas dans la DB fusionnée.")
            return {}
        cursor.execute("SELECT COALESCE(MAX(PlaylistItemId), 0) FROM PlaylistItem")
        max_id = cursor.fetchone()[0]
        print(f"ID max initial: {max_id}")

        item_id_map = {}
        existing_items = {}  # clé de matching -> nouvel ID

        def read_playlist_items(db_path):
            with sqlite3.connect(db_path) as src_conn:
                cur_source = src_conn.cursor()
                cur_source.execute("""
                    SELECT PlaylistItemId, Label, StartTrimOffsetTicks, EndTrimOffsetTicks, Accuracy, EndAction, ThumbnailFilePath
                    FROM PlaylistItem
                """)
                rows = cur_source.fetchall()
            # Ajoute le chemin de la source pour différencier
            return [(db_path,) + row for row in rows]

        playlist_items = read_playlist_items(file1_db) + read_playlist_items(file2_db)
        print(f"Total playlist items lus : {len(playlist_items)}")

        for item in playlist_items:
            db_source = item[0]
            old_id, label, start_trim, end_trim, accuracy, end_action, thumb_path = item[1:]
            # Normalisation des valeurs pour la comparaison
            norm_label = safe_text(label)
            norm_start = safe_number(start_trim)
            norm_end = safe_number(end_trim)
            norm_thumb = safe_text(thumb_path)
            # Si im_mapping doit transformer thumb_path, appliquez ici (pour l'instant on laisse inchangé)
            new_thumb_path = norm_thumb

            key = (norm_label, norm_start, norm_end, accuracy, end_action, new_thumb_path)
            if key in existing_items:
                new_playlist_item_id = existing_items[key]
                print(f"Doublon détecté pour key {key}. Réutilisation de l'ID {new_playlist_item_id}")
            else:
                max_id += 1
                new_playlist_item_id = max_id
                existing_items[key] = new_playlist_item_id
                new_row = (new_playlist_item_id, label, start_trim, end_trim, accuracy, end_action, thumb_path)
                try:
                    # Utiliser INSERT OR IGNORE pour éviter les erreurs et vérifier ensuite l'insertion
                    cursor.execute("""
                        INSERT OR IGNORE INTO PlaylistItem 
                        (PlaylistItemId, Label, StartTrimOffsetTicks, EndTrimOffsetTicks, Accuracy, EndAction, ThumbnailFilePath)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                    """, new_row)
                    # Vérifier si l'insertion a bien eu lieu (si la ligne existe maintenant)
                    cursor.execute("SELECT PlaylistItemId FROM PlaylistItem WHERE PlaylistItemId=?", (new_playlist_item_id,))
                    if cursor.fetchone():
                        print(f"Insertion réussie : {new_row}")
                    else:
                        print(f"Aucune insertion pour {new_row} (peut-être déjà existant)")
                except sqlite3.IntegrityError as e:
                    print(f"Erreur insertion PlaylistItem {old_id}: {e}")
                    continue
            item_id_map[(db_source, old_id)] = new_playlist_item_id

        conn.commit()
        print(f"ID max final: {max_id}")
        print(f"Total items mappés: {len(item_id_map)}")
    return item_id_map


def merge_playlists(merged_db_path, file1_db, file2_db, location_id_map, independent_media_map):
    """Fusionne toutes les tables liées aux playlists en respectant les contraintes."""
    print("\n=== DÉBUT FUSION PLAYLISTS ===")

    max_media_id = 0  # 🔧 Ajout essentiel
    max_playlist_id = 0  # 🔧 pour éviter 'not associated with a value'
    conn = None  # 🧷 Pour pouvoir le fermer plus tard

    try:
        conn = sqlite3.connect(merged_db_path, timeout=30)
        conn.execute("PRAGMA busy_timeout = 10000")
        cursor = conn.cursor()

        print("\n[INITIALISATION]")
        print(f"Base de fusion: {merged_db_path}")
        print(f"Source 1: {file1_db}")
        print(f"Source 2: {file2_db}")
        print(f"Location IDs mappés: {len(location_id_map)}")

        # Appel immédiat à merge_playlist_items pour avoir item_id_map dispo dès le début
        item_id_map = merge_playlist_items(merged_db_path, file1_db, file2_db, independent_media_map)
        print(f"Mapping PlaylistItems: {item_id_map}")

        # ... (la suite continue normalement)

        marker_id_map = {}

        # 1. Fusion PlaylistItemAccuracy
        print("\n[FUSION PLAYLISTITEMACCURACY]")
        cursor.execute("SELECT MAX(PlaylistItemAccuracyId) FROM PlaylistItemAccuracy")
        max_acc_id = cursor.fetchone()[0] or 0
        print(f"ID max initial: {max_acc_id}")

        for db_path in [file1_db, file2_db]:
            with sqlite3.connect(db_path) as src_conn:
                src_cur = src_conn.cursor()
                src_cur.execute("SELECT * FROM PlaylistItemAccuracy")
                records = src_cur.fetchall()
                print(f"{len(records)} records trouvés dans {os.path.basename(db_path)}")
                for acc_id, desc in records:
                    try:
                        cursor.execute("INSERT OR IGNORE INTO PlaylistItemAccuracy VALUES (?, ?)", (acc_id, desc))
                        max_acc_id = max(max_acc_id, acc_id)
                    except sqlite3.IntegrityError:
                        print(f"Duplicate PlaylistItemAccuracy: {acc_id} (ignoré)")
        print(f"ID max final: {max_acc_id}")

        # 2. Fusion PlaylistItemLocationMap
        print("\n[FUSION PLAYLISTITEMLOCATIONMAP]")
        for db_path in [file1_db, file2_db]:
            with sqlite3.connect(db_path) as src_conn:
                src_cur = src_conn.cursor()
                src_cur.execute("""
                    SELECT PlaylistItemId, LocationId, MajorMultimediaType, BaseDurationTicks
                    FROM PlaylistItemLocationMap
                """)
                mappings = src_cur.fetchall()
                print(f"{len(mappings)} mappings trouvés dans {os.path.basename(db_path)}")
                for old_item_id, old_loc_id, mm_type, duration in mappings:
                    new_item_id = item_id_map.get((db_path, old_item_id))
                    new_loc_id = location_id_map.get((db_path, old_loc_id))
                    if new_item_id and new_loc_id:
                        try:
                            cursor.execute("""
                                INSERT OR IGNORE INTO PlaylistItemLocationMap
                                VALUES (?, ?, ?, ?)
                            """, (new_item_id, new_loc_id, mm_type, duration))
                        except sqlite3.IntegrityError as e:
                            print(f"Erreur PlaylistItemLocationMap: {e}")
                    else:
                        print(f"  ⚠️ Mapping manquant pour Item={old_item_id}, Location={old_loc_id} (db: {db_path})")

        # 3. Fusion PlaylistItemMarker
        print("\n[FUSION PLAYLISTITEMMARKER]")
        cursor.execute("SELECT MAX(PlaylistItemMarkerId) FROM PlaylistItemMarker")
        max_marker_id = cursor.fetchone()[0] or 0
        print(f"ID max initial: {max_marker_id}")

        for db_path in [file1_db, file2_db]:
            with sqlite3.connect(db_path) as src_conn:
                src_cur = src_conn.cursor()
                src_cur.execute("""
                    SELECT PlaylistItemMarkerId, PlaylistItemId, Label, 
                           StartTimeTicks, DurationTicks, EndTransitionDurationTicks
                    FROM PlaylistItemMarker
                """)
                markers = src_cur.fetchall()
                print(f"{len(markers)} markers trouvés dans {os.path.basename(db_path)}")
                for row in markers:
                    old_marker_id = row[0]
                    old_item_id = row[1]
                    new_item_id = item_id_map.get((db_path, old_item_id))

                    if new_item_id is None:
                        print(f"    > ID item introuvable pour marker {old_marker_id} — ignoré")
                        continue

                    max_marker_id += 1
                    new_row = (max_marker_id, new_item_id) + row[2:]

                    try:
                        cursor.execute("""
                            INSERT INTO PlaylistItemMarker
                            VALUES (?, ?, ?, ?, ?, ?)
                        """, new_row)
                        marker_id_map[(db_path, old_marker_id)] = max_marker_id
                    except sqlite3.IntegrityError as e:
                        print(f"Erreur insertion PlaylistItemMarker: {e}")

        print(f"ID max final: {max_marker_id}")
        print(f"Total markers mappés: {len(marker_id_map)}")

        # 4. Fusion des PlaylistItemMarker*Map (BibleVerse/Paragraph)
        print("\n[FUSION MARKER MAPS]")
        for map_type in ['BibleVerse', 'Paragraph']:
            table_name = f'PlaylistItemMarker{map_type}Map'
            if not cursor.execute(
                    f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'"
            ).fetchone():
                print(f"Table {table_name} non trouvée - ignorée")
                continue
            print(f"\nFusion {table_name}")
            for db_path in [file1_db, file2_db]:
                with sqlite3.connect(db_path) as source_conn:
                    source_cursor = source_conn.cursor()
                    source_cursor.execute(f"SELECT * FROM {table_name}")
                    maps = source_cursor.fetchall()
                    print(f"{len(maps)} entries dans {os.path.basename(db_path)}")
                    for row in maps:
                        old_marker_id = row[0]
                        # Si marker_id_map n'est disponible pour ce marker
                        new_marker_id = marker_id_map.get((db_path, old_marker_id))
                        if new_marker_id:
                            new_row = (new_marker_id,) + row[1:]
                            try:
                                placeholders = ",".join(["?"] * len(new_row))
                                cursor.execute(f"INSERT OR IGNORE INTO {table_name} VALUES ({placeholders})", new_row)
                            except sqlite3.IntegrityError as e:
                                print(f"Erreur {table_name}: {e}")

        # 5. Fusion de PlaylistItemIndependentMediaMap
        print("\n[FUSION INDEPENDENTMEDIAMAP]")
        if cursor.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='PlaylistItemIndependentMediaMap'"
        ).fetchone():
            for db_path in [file1_db, file2_db]:
                with sqlite3.connect(db_path) as source_conn:
                    source_cursor = source_conn.cursor()
                    source_cursor.execute("""
                        SELECT PlaylistItemId, IndependentMediaId, DurationTicks
                        FROM PlaylistItemIndependentMediaMap
                    """)
                    media_maps = source_cursor.fetchall()
                    print(f"{len(media_maps)} entries dans {os.path.basename(db_path)}")
                    for old_item_id, old_media_id, duration in media_maps:
                        new_item_id = item_id_map.get((db_path, old_item_id))
                        new_media_id = independent_media_map.get((db_path, old_media_id))
                        if new_item_id and new_media_id:
                            try:
                                cursor.execute("""
                                    INSERT OR IGNORE INTO PlaylistItemIndependentMediaMap
                                    VALUES (?, ?, ?)
                                """, (new_item_id, new_media_id, duration))
                            except sqlite3.IntegrityError as e:
                                print(f"Erreur IndependentMediaMap: {e}")
                        else:
                            print(
                                f"  ⚠️ Mapping manquant pour Item={old_item_id}, Media={old_media_id} (db: {db_path})")
        else:
            print("Table PlaylistItemIndependentMediaMap non trouvée - ignorée")

        # ========================
        # Maintenant, on démarre les opérations qui ouvrent leurs propres connexions
        # ========================

        # 7. Fusion de la table IndependentMedia (améliorée)
        print("\n[FUSION INDEPENDENTMEDIA]")
        # On réutilise le mapping déjà préparé dans merge_data
        im_mapping = independent_media_map

        # 8. Vérification finale des thumbnails
        print("\n[VÉRIFICATION THUMBNAILS ORPHELINS]")
        cursor.execute("""
            SELECT p.PlaylistItemId, p.ThumbnailFilePath
            FROM PlaylistItem p
            WHERE p.ThumbnailFilePath IS NOT NULL
              AND NOT EXISTS (
                  SELECT 1 FROM IndependentMedia m 
                  WHERE m.FilePath = p.ThumbnailFilePath
              )
        """)
        orphaned_thumbnails = cursor.fetchall()
        if orphaned_thumbnails:
            print(f"Avertissement : {len(orphaned_thumbnails)} thumbnails sans média associé")

            # ✅ Ajoute ceci ici (pas en dehors)
            conn.commit()

        # 9. Finalisation playlists
        print("\n=== FUSION PLAYLISTS TERMINÉE ===")
        playlist_results = {
            'item_id_map': item_id_map,
            'marker_id_map': marker_id_map,
            'media_status': {
                'total_media': max_media_id,
                'orphaned_thumbnails': len(orphaned_thumbnails) if 'orphaned_thumbnails' in locals() else 0
            }
        }
        print(f"Résumé intermédiaire: {playlist_results}")

        # 10. (Optionnel) Fusion de la table Playlist si elle existe – ici on l'ignore car vous n'avez pas de table Playlist
        if cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='Playlist'").fetchone():
            print("\n=== DEBUT FUSION PLAYLIST ===")
            cursor.execute("SELECT MAX(PlaylistId) FROM Playlist")
            max_playlist_id = cursor.fetchone()[0] or 0
            print(f"ID max initial Playlist: {max_playlist_id}")
            playlist_id_map = {}
            for db_path in [file1_db, file2_db]:
                source_conn = sqlite3.connect(db_path)
                source_cursor = source_conn.cursor()
                source_cursor.execute("""
                    SELECT PlaylistId, Name, Description, IconId, OrderIndex, LastModified
                    FROM Playlist
                """)
                playlists = source_cursor.fetchall()
                print(f"{len(playlists)} playlists trouvées dans {os.path.basename(db_path)}")
                for pl_id, name, desc, icon, order_idx, modified in playlists:
                    original_name = name
                    suffix = 1
                    while True:
                        cursor.execute("SELECT 1 FROM Playlist WHERE Name = ?", (name,))
                        if not cursor.fetchone():
                            break
                        name = f"{original_name} ({suffix})"
                        suffix += 1
                    max_playlist_id += 1
                    try:
                        cursor.execute("""
                            INSERT INTO Playlist VALUES (?, ?, ?, ?, ?, ?)
                        """, (max_playlist_id, name, desc, icon, order_idx, modified))
                        playlist_id_map[(db_path, pl_id)] = max_playlist_id
                    except sqlite3.IntegrityError as e:
                        print(f"ERREUR Playlist {pl_id}: {str(e)}")
                source_conn.close()
            print(f"Playlist fusionnée - ID max final: {max_playlist_id}")
            print(f"Total playlists fusionnées: {len(playlist_id_map)}")
        else:
            # Vérifie si la table Playlist existe avant d'agir
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='Playlist'")
            if cursor.fetchone():
                print("Fusion de la table Playlist...")
                # (Tu peux mettre ici la future logique si un jour Playlist est utilisée)
            else:
                print("Table Playlist non trouvée - étape ignorée")

        # 11. Vérification de cohérence
        print("\n=== VERIFICATION COHERENCE ===")
        cursor.execute("""
            SELECT COUNT(*) FROM PlaylistItem 
            WHERE PlaylistItemId NOT IN (
                SELECT pim.PlaylistItemId FROM PlaylistItemMap pim
            )
        """)
        orphaned_items = cursor.fetchone()[0]
        status_color = "\033[91m" if orphaned_items > 0 else "\033[92m"
        print(f"{status_color}Éléments sans parent détectés (non supprimés) : {orphaned_items}\033[0m")

        # 12. Optimisations finales (désactivé pour PlaylistItem)
        print("\n=== DEBUT OPTIMISATIONS ===")
        print("Aucun nettoyage n'est effectué sur PlaylistItem.")
        orphaned_deleted = 0

        # Journalisation détaillée
        log_file = os.path.join(UPLOAD_FOLDER, "fusion.log")
        print(f"\nCréation fichier log: {log_file}")
        with open(log_file, "a") as f:
            f.write(f"\n\n=== Session {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ===\n")

        def log_message(message, log_type="INFO"):
            print(message)
            with open(log_file, "a") as f:
                f.write(f"[{log_type}] {datetime.now().strftime('%H:%M:%S')} - {message}\n")

        # 13.1 Reconstruction des index
        print("\nReconstruction des index...")
        cursor.execute("SELECT name FROM sqlite_master WHERE type='index'")
        indexes = [row[0] for row in cursor.fetchall() if not row[0].startswith('sqlite_autoindex_')]
        for index_name in indexes:
            try:
                cursor.execute(f"REINDEX {index_name}")
                log_message(f"Index reconstruit: {index_name}")
            except sqlite3.Error as e:
                log_message(f"ERREUR sur index {index_name}: {str(e)}", "ERROR")

        # 13.2 Vérification intégrité
        print("\nVérification intégrité base de données...")
        cursor.execute("PRAGMA quick_check")
        integrity_result = cursor.fetchone()[0]
        if integrity_result == "ok":
            log_message("Intégrité de la base: OK")
        else:
            log_message(f"ERREUR intégrité: {integrity_result}", "ERROR")

        # 13.3 Vérification clés étrangères
        cursor.execute("PRAGMA foreign_key_check")
        fk_issues = cursor.fetchall()
        if fk_issues:
            log_message(f"ATTENTION: {len(fk_issues)} problèmes de clés étrangères", "WARNING")
            for issue in fk_issues[:3]:
                log_message(f"- Problème: {issue}", "WARNING")
        else:
            log_message("Aucun problème de clé étrangère détecté")

        # 13.4 Optimisation finale
        print("\nOptimisation finale...")
        conn.commit()  # S'assurer de clôturer la transaction en cours
        start_time = time.perf_counter()
        cursor.execute("VACUUM")
        cursor.execute("PRAGMA optimize")
        optimization_time = time.perf_counter() - start_time
        log_message(f"Optimisation terminée en {optimization_time:.2f}s")

        # 14. Finalisation
        # commit final et fermeture propre
        conn.commit()

        # Récapitulatif final
        print("\n=== RÉCAPITULATIF FINAL ===")
        print(f"{'Playlists:':<20} {max_playlist_id}")
        print(f"{'Éléments:':<20} {len(item_id_map)}")
        print(f"{'Médias:':<20} {max_media_id}")
        print(f"{'Nettoyés:':<20} {orphaned_deleted}")
        print(f"{'Intégrité:':<20} {integrity_result}")
        if fk_issues:
            print(f"{'Problèmes FK:':<20} \033[91m{len(fk_issues)}\033[0m")
        else:
            print(f"{'Problèmes FK:':<20} \033[92mAucun\033[0m")
        with sqlite3.connect(merged_db_path) as test_conn:
            test_cursor = test_conn.cursor()
            test_cursor.execute("SELECT 1 FROM sqlite_master LIMIT 1")
            db_status = "OK" if test_cursor.fetchone() else "ERREUR"
            print(f"\nStatut final DB: {db_status}")

        # 15. Fusion des autres tables (hors playlists)
        merge_other_tables(
            merged_db_path,
            file1_db,
            file2_db,
            exclude_tables=[
                'Note', 'UserMark', 'Location', 'BlockRange',
                'LastModified', 'Tag', 'TagMap', 'PlaylistItem',
                'PlaylistItemAccuracy', 'PlaylistItemLocationMap',
                'PlaylistItemMarker', 'PlaylistItemMarkerBibleVerseMap',
                'PlaylistItemMarkerParagraphMap', 'PlaylistItemIndependentMediaMap'
            ]
        )

        # 16. Activation WAL
        conn = sqlite3.connect(merged_db_path)
        cursor = conn.cursor()
        cursor.execute("PRAGMA journal_mode=WAL")
        cursor.execute("CREATE TABLE IF NOT EXISTS dummy_for_wal (id INTEGER PRIMARY KEY)")
        cursor.execute("INSERT INTO dummy_for_wal DEFAULT VALUES")
        conn.commit()
        cursor.execute("DELETE FROM dummy_for_wal")
        conn.commit()
        cursor.execute("DROP TABLE dummy_for_wal")
        conn.commit()
        conn.close()
        with sqlite3.connect(merged_db_path) as test_conn:
            new_wal_status = test_conn.execute("PRAGMA journal_mode").fetchone()[0]
            print(f"Statut WAL après activation: {new_wal_status}")
            if new_wal_status != "wal":
                print("Avertissement: Échec de l'activation WAL")

        # 17. Préparation archive .jwlibrary (hors de tout with)
        base_folder = os.path.join(EXTRACT_FOLDER, "file1_extracted")
        merged_folder = os.path.join(UPLOAD_FOLDER, "merged_folder")
        if os.path.exists(merged_folder):
            shutil.rmtree(merged_folder)
        shutil.copytree(base_folder, merged_folder)
        for fname in ["userData.db", "userData.db-wal", "userData.db-shm"]:
            dest = os.path.join(merged_folder, fname)
            if os.path.exists(dest):
                os.remove(dest)
        shutil.copy(merged_db_path, os.path.join(merged_folder, "userData.db"))
        open(os.path.join(merged_folder, "userData.db-wal"), 'wb').close()
        open(os.path.join(merged_folder, "userData.db-shm"), 'wb').close()
        merged_zip = os.path.join(UPLOAD_FOLDER, "merged.zip")
        if os.path.exists(merged_zip):
            os.remove(merged_zip)
        shutil.make_archive(
            base_name=merged_zip.replace('.zip', ''),
            format='zip',
            root_dir=merged_folder
        )
        merged_jwlibrary = merged_zip.replace(".zip", ".jwlibrary")
        if os.path.exists(merged_jwlibrary):
            os.remove(merged_jwlibrary)
        time.sleep(0.5)
        os.rename(merged_zip, merged_jwlibrary)
        print(f"\n✅ Fichier généré : {merged_jwlibrary} ({os.path.getsize(merged_jwlibrary) / 1024 / 1024:.2f} Mo)")
        if os.path.getsize(merged_jwlibrary) < 1024:
            print("⚠️ Avertissement: Le fichier semble trop petit")

        # Retour final de merge_playlists
        print(">>> Fin de merge_playlists, on retourne les valeurs")
        return merged_jwlibrary, max_playlist_id, len(item_id_map), max_media_id, orphaned_deleted, integrity_result



    except Exception as e:

        print(f"ERREUR CRITIQUE dans merge_playlists: {str(e)}")

        return None, 0, 0, 0, 0, "error"


    finally:

        if conn:
            conn.close()


def create_note_mapping(merged_db_path, file1_db, file2_db):
    """Crée un mapping (source_db_path, old_note_id) -> new_note_id en se basant sur les GUID."""
    mapping = {}
    try:
        with sqlite3.connect(merged_db_path, timeout=30) as merged_conn:
            merged_conn.execute("PRAGMA busy_timeout = 10000")
            merged_cursor = merged_conn.cursor()
            merged_cursor.execute("SELECT Guid, NoteId FROM Note")
            merged_guid_map = {guid: note_id for guid, note_id in merged_cursor.fetchall() if guid}

        for db_path in [file1_db, file2_db]:
            if not os.path.exists(db_path):
                print(f"[WARN] Fichier DB manquant : {db_path}")
                continue

            with sqlite3.connect(db_path) as src_conn:
                src_cursor = src_conn.cursor()
                src_cursor.execute("SELECT NoteId, Guid FROM Note")
                for old_note_id, guid in src_cursor.fetchall():
                    if guid and guid in merged_guid_map:
                        mapping[(db_path, old_note_id)] = merged_guid_map[guid]

    except Exception as e:
        print(f"[ERREUR] create_note_mapping: {str(e)}")

    return mapping or {}


@app.route('/merge', methods=['POST'])
def merge_data():
    try:
        global note_mapping  # Si vous souhaitez utiliser le scope global (optionnel)
        payload = request.get_json()
        conflict_choices_notes = payload.get("conflicts_notes", {})
        conflict_choices_highlights = payload.get("conflicts_highlights", {})
        local_datetime = payload.get("local_datetime")
        print(f"local_datetime reçu du client : {local_datetime}")
        if local_datetime:
            merge_date = local_datetime if len(local_datetime) > 16 else local_datetime + ":00"
        else:
            merge_date = get_current_local_iso8601()

        file1_db = os.path.join(EXTRACT_FOLDER, "file1_extracted", "userData.db")
        file2_db = os.path.join(EXTRACT_FOLDER, "file2_extracted", "userData.db")

        # Validation fichiers sources
        if not all(os.path.exists(db) for db in [file1_db, file2_db]):
            return jsonify({"error": "Fichiers source manquants"}), 400

        data1 = read_notes_and_highlights(file1_db)
        data2 = read_notes_and_highlights(file2_db)

        # Fusion simple des Notes et Highlights (vous pouvez conserver votre logique ici)
        notes_db1 = data1["notes"]
        notes_db2 = data2["notes"]
        merged_notes_list = []

        def format_note(db_path, note_row):
            # Cette fonction garantit que chaque note ait 10 champs
            (
                note_id,
                guid,
                title,
                content,
                old_loc_id,
                usermark_guid,
                last_modified,
                created,
                block_type,
                block_identifier
            ) = note_row
            return (
                db_path, guid, title, content,
                old_loc_id, usermark_guid,
                last_modified, created,
                block_type, block_identifier
            )

        # Ajout des notes du fichier 1
        for note in notes_db1:
            merged_notes_list.append(format_note(file1_db, note))

        # Ajout des notes du fichier 2 sans doublons
        for note in notes_db2:
            _, _, title2, content2, *_ = note
            existe = any(n[2] == title2 and n[3] == content2 for n in merged_notes_list)
            if not existe:
                merged_notes_list.append(format_note(file2_db, note))

        highlights_db1 = data1["highlights"]
        highlights_db2 = data2["highlights"]
        merged_highlights_dict = {}
        for h in highlights_db1:
            _, color, loc, style, guid, version = h
            merged_highlights_dict[guid] = (color, loc, style, version)
        for h in highlights_db2:
            _, color2, loc2, style2, guid2, version2 = h
            if guid2 not in merged_highlights_dict:
                merged_highlights_dict[guid2] = (color2, loc2, style2, version2)
            else:
                (color1, loc1, style1, version1) = merged_highlights_dict[guid2]
                if (color1 == color2 and loc1 == loc2 and style1 == style2 and version1 == version2):
                    continue
                else:
                    choice = conflict_choices_highlights.get(guid2, "file1")
                    if choice == "file2":
                        merged_highlights_dict[guid2] = (color2, loc2, style2, version2)

        # === Validation préalable ===
        required_dbs = [
            os.path.join(EXTRACT_FOLDER, "file1_extracted", "userData.db"),
            os.path.join(EXTRACT_FOLDER, "file2_extracted", "userData.db")
        ]
        if not all(os.path.exists(db) for db in required_dbs):
            return jsonify({"error": "Fichiers source manquants"}), 400

        # Création de la DB fusionnée
        merged_db_path = os.path.join(UPLOAD_FOLDER, "merged_userData.db")
        if os.path.exists(merged_db_path):
            os.remove(merged_db_path)
        base_db_path = os.path.join(EXTRACT_FOLDER, "file1_extracted", "userData.db")
        create_merged_schema(merged_db_path, base_db_path)

        # ... Après la création de merged_db_path et avant la fusion des autres mappings
        # Fusion de la table IndependentMedia et PlaylistItems, etc.
        location_id_map = merge_location_from_sources(merged_db_path, *required_dbs)
        print("Location ID Map:", location_id_map)

        independent_media_map = merge_independent_media(merged_db_path, file1_db, file2_db)
        print("Mapping IndependentMedia:", independent_media_map)

        item_id_map = merge_playlist_items(merged_db_path, file1_db, file2_db, independent_media_map)
        print("Mapping PlaylistItems:", item_id_map)

        # === INSÉRER LES NOTES et USERMARK DANS LA DB FUSIONNÉE AVANT de créer note_mapping ===
        conn = sqlite3.connect(merged_db_path)
        cursor = conn.cursor()
        for note_tuple in merged_notes_list:
            old_db_path, guid, title, content, old_loc_id, usermark_guid, last_modified, created, block_type, block_identifier = note_tuple
            new_guid = str(uuid.uuid4())

            # Appliquer le mapping de LocationId
            normalized_key = (os.path.normpath(old_db_path), old_loc_id)
            normalized_location_map = {(os.path.normpath(k[0]), k[1]): v for k, v in location_id_map.items()}
            new_location_id = normalized_location_map.get(normalized_key)

            # Appliquer le mapping de UserMarkId si possible
            new_usermark_id = None
            if usermark_guid:
                cursor.execute("SELECT UserMarkId FROM UserMark WHERE UserMarkGuid = ?", (usermark_guid,))
                result = cursor.fetchone()
                if result:
                    new_usermark_id = result[0]

            # 🧩 C’est ici qu’il faut corriger :
            note_guid = guid if guid else str(uuid.uuid4())  # <== Garantit un GUID pour la Note

            cursor.execute("""
                INSERT OR REPLACE INTO Note (Guid, Title, Content, LocationId, UserMarkId, LastModified, Created, BlockType, BlockIdentifier)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                note_guid,
                title,
                content,
                new_location_id,
                new_usermark_id,
                last_modified,
                created,
                block_type,
                block_identifier
            ))

        for guid, (color, loc, style, version) in merged_highlights_dict.items():
            try:
                cursor.execute("""
                    INSERT INTO UserMark (ColorIndex, LocationId, StyleIndex, UserMarkGuid, Version)
                    VALUES (?, ?, ?, ?, ?)
                """, (color, loc, style, guid, version))
            except Exception as e:
                print(f"Erreur lors de l'insertion de UserMarkGuid={guid}: {e}")
        # Gestion spécifique de LastModified
        cursor.execute("DELETE FROM LastModified")
        cursor.execute("INSERT INTO LastModified (LastModified) VALUES (?)", (merge_date,))
        conn.commit()
        conn.close()

        # Maintenant, créer le mapping des Notes à partir de la DB fusionnée
        note_mapping = create_note_mapping(merged_db_path, file1_db, file2_db)
        print("Note Mapping:", note_mapping)

        # ===== Fusion des tables critiques et vérification =====
        location_id_map = merge_location_from_sources(merged_db_path, file1_db, file2_db)
        print("\n=== LOCATION VERIFICATION ===")
        print(f"Total Locations: {len(location_id_map)}")
        with sqlite3.connect(merged_db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT KeySymbol, COUNT(*) FROM Location GROUP BY KeySymbol")
            for key, count in cursor.fetchall():
                print(f"- {key}: {count} locations")

        usermark_id_map = merge_usermark_from_sources(merged_db_path, file1_db, file2_db, location_id_map)
        print("\n=== USERMARK VERIFICATION ===")
        print(f"Total UserMaps mappés: {len(usermark_id_map)}")
        with sqlite3.connect(merged_db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM UserMark")
            total = cursor.fetchone()[0]
            print(f"UserMarks dans la DB: {total}")
            cursor.execute("""
                SELECT ColorIndex, StyleIndex, COUNT(*) 
                FROM UserMark 
                GROUP BY ColorIndex, StyleIndex
            """)
            print("Répartition par couleur/style:")
            for color, style, count in cursor.fetchall():
                print(f"- Couleur {color}, Style {style}: {count} marques")

        print(f"Location IDs mappés: {location_id_map}")
        print(f"UserMark IDs mappés: {usermark_id_map}")

        # ===== Vérification pré-fusion complète =====
        print("\n=== VERIFICATION PRE-FUSION ===")
        print("\n[VÉRIFICATION FICHIERS SOURCES]")
        source_files = [
            os.path.join(EXTRACT_FOLDER, "file1_extracted", "userData.db"),
            os.path.join(EXTRACT_FOLDER, "file2_extracted", "userData.db")
        ]
        for file in source_files:
            print(f"Vérification {file}... ", end="")
            if not os.path.exists(file):
                print("ERREUR: Fichier manquant")
                return jsonify({"error": f"Fichier source manquant: {file}"}), 400
            else:
                print(f"OK ({os.path.getsize(file) / 1024:.1f} KB)")

        print("\n[VÉRIFICATION SCHÉMA]")

        def verify_schema(db_path):
            try:
                conn = sqlite3.connect(db_path)
                cursor = conn.cursor()
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
                tables = [t[0] for t in cursor.fetchall()]
                print(f"Tables dans {os.path.basename(db_path)}: {len(tables)}")
                required_tables = {'Bookmark', 'Location', 'UserMark', 'Note'}
                missing = required_tables - set(tables)
                if missing:
                    print(f"  TABLES MANQUANTES: {missing}")
                conn.close()
                return not bool(missing)
            except Exception as e:
                print(f"  ERREUR: {str(e)}")
                return False
        if not all(verify_schema(db) for db in source_files):
            return jsonify({"error": "Schéma de base de données incompatible"}), 400

        print("\n[VÉRIFICATION BASE DE DESTINATION]")
        print(f"Vérification {merged_db_path}... ", end="")
        try:
            conn = sqlite3.connect(merged_db_path)
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = [t[0] for t in cursor.fetchall()]
            print(f"OK ({len(tables)} tables)")
            conn.close()
        except Exception as e:
            print(f"ERREUR: {str(e)}")
            return jsonify({"error": "Base de destination corrompue"}), 500

        print("\n[VÉRIFICATION SYSTÈME]")
        try:
            import psutil
            mem = psutil.virtual_memory()
            print(f"Mémoire disponible: {mem.available / 1024 / 1024:.1f} MB")
            if mem.available < 500 * 1024 * 1024:
                print("ATTENTION: Mémoire insuffisante")
        except ImportError:
            print("psutil non installé - vérification mémoire ignorée")

        print("\n=== PRÊT POUR FUSION ===\n")

        # --- FUSION DE BOOKMARKS (sans suppression préalable) ---
        conn = sqlite3.connect(merged_db_path)
        cursor = conn.cursor()

        for db_path in [file1_db, file2_db]:
            source_conn = sqlite3.connect(db_path)
            source_cursor = source_conn.cursor()

            print(f"\nDEBUG: Contenu de {db_path} avant fusion:")
            source_cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = [t[0] for t in source_cursor.fetchall()]
            print(f"Tables disponibles: {tables}")

            if 'Playlist' in tables:
                source_cursor.execute("SELECT COUNT(*) FROM Playlist")
                count = source_cursor.fetchone()[0]
                print(f"Nombre de playlists: {count}")
                source_cursor.execute("SELECT PlaylistId, Name FROM Playlist LIMIT 5")
                print(f"Exemples de playlists: {source_cursor.fetchall()}")

            source_cursor.execute("""
                SELECT BookmarkId, LocationId, PublicationLocationId, Slot, Title, 
                       Snippet, BlockType, BlockIdentifier
                FROM Bookmark
            """)
            for row in source_cursor.fetchall():
                b_id, loc_id, pub_loc_id, slot, title, snippet, block_type, block_id = row

                new_loc_id = location_id_map.get((db_path, loc_id), loc_id)
                new_pub_loc_id = location_id_map.get((db_path, pub_loc_id), pub_loc_id)

                # Vérification de l'existence des locations
                cursor.execute("SELECT 1 FROM Location WHERE LocationId IN (?, ?)", (new_loc_id, new_pub_loc_id))
                if len(cursor.fetchall()) != 2:
                    print(f"Attention : Location {new_loc_id} ou {new_pub_loc_id} manquante - Bookmark ignoré")
                    continue

                # Empêche les doublons exacts
                cursor.execute("""
                    SELECT 1 FROM Bookmark 
                    WHERE PublicationLocationId=? AND Slot=?
                """, (new_pub_loc_id, slot))
                exists = cursor.fetchone()

                if not exists:
                    try:
                        cursor.execute("""
                            INSERT INTO Bookmark
                            (LocationId, PublicationLocationId, Slot, Title,
                             Snippet, BlockType, BlockIdentifier)
                            VALUES (?, ?, ?, ?, ?, ?, ?)
                        """, (new_loc_id, new_pub_loc_id, slot, title, snippet, block_type, block_id))
                    except sqlite3.IntegrityError as e:
                        print(f"Erreur insertion BookmarkId={b_id}: {e}")
                else:
                    print(f"Bookmark ignoré : doublon PublicationLocationId={new_pub_loc_id}, Slot={slot}")

            source_conn.close()

        conn.commit()
        conn.close()

        # --- SECTION BLOCKRANGE ---
        conn = sqlite3.connect(merged_db_path)
        cursor = conn.cursor()
        cursor.execute("DELETE FROM BlockRange")
        existing_blockranges = {}
        for db_path in [file1_db, file2_db]:
            source_conn = sqlite3.connect(db_path)
            source_cursor = source_conn.cursor()
            source_cursor.execute("""
                    SELECT br.BlockType, br.Identifier, br.StartToken, br.EndToken, um.UserMarkGuid
                    FROM BlockRange br
                    JOIN UserMark um ON br.UserMarkId = um.UserMarkId
                """)
            for row in source_cursor.fetchall():
                block_type, identifier, start_token, end_token, usermark_guid = row
                cursor.execute("SELECT UserMarkId FROM UserMark WHERE UserMarkGuid=?", (usermark_guid,))
                new_usermark_id = cursor.fetchone()
                if new_usermark_id:
                    new_usermark_id = new_usermark_id[0]
                    blockrange_key = (block_type, identifier, start_token, end_token, new_usermark_id)
                    if blockrange_key not in existing_blockranges:
                        cursor.execute("""
                                INSERT INTO BlockRange
                                (BlockType, Identifier, StartToken, EndToken, UserMarkId)
                                VALUES (?, ?, ?, ?, ?)
                            """, (block_type, identifier, start_token, end_token, new_usermark_id))
                        existing_blockranges[blockrange_key] = True
            source_conn.close()
        conn.commit()
        conn.close()

        # Mapping inverse UserMarkId original → nouveau
        usermark_guid_map = {}
        conn = sqlite3.connect(merged_db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT UserMarkId, UserMarkGuid FROM UserMark")
        for new_id, guid in cursor.fetchall():
            usermark_guid_map[guid] = new_id
        conn.close()

        # Fusion des Notes
        location_id_map = merge_location_from_sources(merged_db_path, file1_db, file2_db)
        print("Location ID Map:", location_id_map)

        conn = sqlite3.connect(merged_db_path)
        cursor = conn.cursor()
        cursor.execute("DELETE FROM Note")
        for db_path in [file1_db, file2_db]:
            source_conn = sqlite3.connect(db_path)
            source_cursor = source_conn.cursor()
            source_cursor.execute("""
                SELECT n.Guid, um.UserMarkGuid, n.LocationId, n.Title, n.Content,
                       n.LastModified, n.Created, n.BlockType, n.BlockIdentifier
                FROM Note n
                LEFT JOIN UserMark um ON n.UserMarkId = um.UserMarkId
            """)
            for (guid, usermark_guid, location_id, title, content,
                 last_modified, created, block_type, block_identifier) in source_cursor.fetchall():
                new_usermark_id = usermark_guid_map.get(usermark_guid) if usermark_guid else None
                normalized_key = (os.path.normpath(db_path), location_id)
                normalized_map = {(os.path.normpath(k[0]), k[1]): v for k, v in location_id_map.items()}
                new_location_id = normalized_map.get(normalized_key) if location_id else None

                if new_location_id is None and location_id:
                    print(f"⚠️ LocationId {location_id} introuvable dans {db_path}")

                try:
                    cursor.execute("""
                        INSERT INTO Note
                        (Guid, UserMarkId, LocationId, Title, Content,
                         LastModified, Created, BlockType, BlockIdentifier)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        guid,
                        new_usermark_id,
                        new_location_id,
                        title,
                        content,
                        last_modified,
                        created,
                        block_type,
                        block_identifier
                    ))
                except sqlite3.IntegrityError as e:
                    if "UNIQUE constraint failed: Note.Guid" in str(e):
                        print(f"Mise à jour de la note existante {guid}")
                        cursor.execute("""
                            UPDATE Note SET
                                UserMarkId=?,
                                LocationId=?,
                                Title=?,
                                Content=?,
                                LastModified=?,
                                BlockType=?,
                                BlockIdentifier=?
                            WHERE Guid=?
                        """, (
                            new_usermark_id,
                            new_location_id,
                            title,
                            content,
                            last_modified,
                            block_type,
                            block_identifier,
                            guid
                        ))
            source_conn.close()
        conn.commit()
        conn.close()

        # --- Nouvelle étape : fusion des bookmarks
        try:
            bookmark_id_map = merge_bookmarks(merged_db_path, file1_db, file2_db, location_id_map)
            print("Mapping BookmarkId :", bookmark_id_map)
        except Exception as e:
            print(f"Erreur lors de la fusion des bookmarks : {e}")
            return jsonify({"error": "Échec de la fusion des bookmarks"}), 500

        independent_media_map = merge_independent_media(merged_db_path, file1_db, file2_db)
        print("Mapping IndependentMedia :", independent_media_map)

        try:
            tag_id_map, tagmap_id_map = merge_tags_and_tagmap(
                merged_db_path,
                file1_db,
                file2_db,
                note_mapping,
                location_id_map,
                item_id_map
            )
        except Exception as e:
            print(f"Échec de merge_tags_and_tagmap: {str(e)}")
            return jsonify({"error": "Échec de la fusion des tags"}), 500

        print(f"Tag ID Map: {tag_id_map}")
        print(f"TagMap ID Map: {tagmap_id_map}")

        print("\n=== TAGS VERIFICATION ===")
        with sqlite3.connect(merged_db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM Tag")
            tags_count = cursor.fetchone()[0]
            cursor.execute("SELECT COUNT(*) FROM TagMap")
            tagmaps_count = cursor.fetchone()[0]
            print(f"Tags: {tags_count}")
            print(f"TagMaps: {tagmaps_count}")
            cursor.execute("""
                SELECT COUNT(*) 
                FROM TagMap 
                WHERE NoteId NOT IN (SELECT NoteId FROM Note)
            """)
            orphaned = cursor.fetchone()[0]
            print(f"TagMaps orphelins: {orphaned}")

        merged_file, playlist_count, playlist_item_count, media_count, cleaned_items, integrity_check = merge_playlists(
            merged_db_path, file1_db, file2_db, location_id_map, independent_media_map
        )

        if not merged_file:
            return jsonify({"error": "Échec de la fusion des playlists"}), 500

        # --- Mise à jour des LocationId résiduels
        location_replacements_flat = {}
        for (db_path, old_id), new_id in location_id_map.items():
            location_replacements_flat[old_id] = new_id

        update_location_references(merged_db_path, location_replacements_flat)

        print("\n=== VERIFICATION POST-FUSION ===")
        with sqlite3.connect(merged_db_path) as conn:
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM PlaylistItem")
            count = cur.fetchone()[0]
            print(f"Nombre d'enregistrements dans PlaylistItem après fusion : {count}")

        return jsonify({
            "status": "success",
            "merged_file": merged_file,
            "stats": {
                "playlists": playlist_count,
                "playlist_items": playlist_item_count,
                "media_files": media_count,
                "cleaned_items": cleaned_items
            },
            "integrity_check": integrity_check
        }), 200

    except Exception as e:
        import traceback

        traceback.print_exc()  # Affiche la trace complète dans les logs
        return jsonify({"error": f"Erreur interne : {str(e)}"}), 500


@app.route('/download', methods=['GET'])
def download_file():
    merged_jwlibrary = os.path.join(UPLOAD_FOLDER, "merged.jwlibrary")
    if not os.path.exists(merged_jwlibrary):
        return jsonify({"error": "Fichier fusionné non trouvé."}), 404
    response = send_file(merged_jwlibrary, as_attachment=True)
    response.headers.add("Access-Control-Allow-Origin", "*")
    return response


@app.errorhandler(Exception)
def handle_exception(e):
    response = jsonify({"error": str(e)})
    response.headers.add("Access-Control-Allow-Origin", "*")
    return response, 500


@app.route('/')
def home():
    return jsonify({"message": "Le serveur Flask fonctionne 🎉"})


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0')

