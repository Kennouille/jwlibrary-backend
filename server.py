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


def normalize_mapping_keys(mapping):
    return {
        (os.path.normpath(k[0]), k[1]): v
        for k, v in mapping.items()
    }


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
    Si une ligne existe déjà, on ignore la nouvelle pour préserver les données existantes.
    Retourne un mapping : {(db_source, ancien_ID) : nouveau_ID, ...}
    """
    print("\n[FUSION INDEPENDENTMEDIA]")
    mapping = {}
    with sqlite3.connect(merged_db_path) as merged_conn:
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

                    # Vérifie si la ligne existe déjà (évite doublons)
                    merged_cursor.execute("""
                        SELECT IndependentMediaId, MimeType
                        FROM IndependentMedia
                        WHERE OriginalFilename = ? AND FilePath = ? AND Hash = ?
                    """, (orig_fn, file_path, hash_val))
                    result = merged_cursor.fetchone()

                    if result:
                        new_id, existing_mime = result
                        # Au lieu de mettre à jour le MimeType, on ignore simplement la nouvelle ligne
                        print(f"    > Ligne déjà présente pour ID {new_id} (ignorée pour {db_path})")
                    else:
                        merged_cursor.execute("""
                            INSERT INTO IndependentMedia (OriginalFilename, FilePath, MimeType, Hash)
                            VALUES (?, ?, ?, ?)
                        """, (orig_fn, file_path, mime, hash_val))
                        new_id = merged_cursor.lastrowid
                        print(f"    > Insertion nouvelle ligne ID {new_id}")

                    mapping[(db_path, old_id)] = new_id

        merged_conn.commit()

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

    # Création correcte de PlaylistItemMediaMap si elle n'existe pas
    merged_cursor.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='PlaylistItemMediaMap'"
    )
    if not merged_cursor.fetchone():
        merged_cursor.execute("""
            CREATE TABLE PlaylistItemMediaMap (
                PlaylistItemId   INTEGER NOT NULL,
                MediaFileId      INTEGER NOT NULL,
                OrderIndex       INTEGER NOT NULL,
                PRIMARY KEY (PlaylistItemId, MediaFileId),
                FOREIGN KEY (PlaylistItemId) REFERENCES PlaylistItem(PlaylistItemId),
                FOREIGN KEY (MediaFileId)  REFERENCES IndependentMedia(IndependentMediaId)
            )
        """)
        print("PlaylistItemMediaMap (avec MediaFileId, OrderIndex) créée dans la base fusionnée.")

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
    """
    Fusionne toutes les tables restantes (hors celles spécifiées dans exclude_tables)
    dans la base fusionnée de manière idempotente.
    Pour chaque table, la fonction vérifie si une ligne identique (comparaison sur toutes
    les colonnes sauf la clé primaire) est déjà présente avant insertion.
    """
    if exclude_tables is None:
        exclude_tables = ["Note", "UserMark", "Bookmark", "InputField"]

    # On effectue un checkpoint pour s'assurer que les données sont bien synchronisées.
    checkpoint_db(db1_path)
    checkpoint_db(db2_path)

    def get_tables(path):
        with sqlite3.connect(path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
            return {row[0] for row in cursor.fetchall()}

    tables1 = get_tables(db1_path)
    tables2 = get_tables(db2_path)
    all_tables = (tables1 | tables2) - set(exclude_tables)

    merged_conn = sqlite3.connect(merged_db_path)
    merged_cursor = merged_conn.cursor()
    source_db_paths = [db1_path, db2_path]

    for table in all_tables:
        # Crée la table dans la DB fusionnée si elle est manquante
        create_table_if_missing(merged_conn, source_db_paths, table)
        merged_cursor.execute(f"PRAGMA table_info({table})")
        columns_info = merged_cursor.fetchall()
        if not columns_info:
            print(f"❌ Table {table} introuvable dans la DB fusionnée.")
            continue

        # Récupération de la liste des colonnes (on suppose que la première colonne est la clé primaire)
        columns = [col[1] for col in columns_info]
        columns_joined = ", ".join(columns)
        placeholders = ", ".join(["?"] * len(columns))

        # Pour chaque source, insérer les lignes qui n'existent pas déjà
        for source_path in source_db_paths:
            with sqlite3.connect(source_path) as src_conn:
                src_cursor = src_conn.cursor()
                try:
                    src_cursor.execute(f"SELECT * FROM {table}")
                    rows = src_cursor.fetchall()
                except Exception as e:
                    print(f"⚠️ Erreur lecture de {table} depuis {source_path}: {e}")
                    rows = []
                for row in rows:
                    # On ignore la première colonne (clé primaire) lors de la comparaison
                    where_clause = " AND ".join([f"{col}=?" for col in columns[1:]])
                    check_query = f"SELECT 1 FROM {table} WHERE {where_clause} LIMIT 1"
                    merged_cursor.execute(check_query, row[1:])
                    exists = merged_cursor.fetchone()
                    if not exists:
                        # Si la ligne n'existe pas, on détermine la nouvelle clé
                        cur_max = merged_cursor.execute(f"SELECT MAX({columns[0]}) FROM {table}").fetchone()[0] or 0
                        new_id = cur_max + 1
                        new_row = (new_id,) + row[1:]
                        print(f"✅ INSERT dans {table} depuis {source_path}: {new_row}")
                        merged_cursor.execute(f"INSERT INTO {table} ({columns_joined}) VALUES ({placeholders})",
                                              new_row)
                    else:
                        print(f"⏩ Doublon ignoré dans {table} depuis {source_path}: {row[1:]}")
    merged_conn.commit()
    merged_conn.close()


def merge_bookmarks(merged_db_path, file1_db, file2_db, location_id_map):
    """
    Fusionne les bookmarks depuis les deux fichiers sources dans la base fusionnée,
    de façon idempotente.
    Pour chaque bookmark, un mapping (SourceDb, OldID) -> NewID est enregistré dans
    la table MergeMapping_Bookmark. En cas de conflit sur (PublicationLocationId, Slot),
    le Slot est incrémenté jusqu'à trouver une valeur libre.
    Retourne un mapping { (db_source, old_id): new_id }.
    """
    print("\n[FUSION BOOKMARKS - IDÉMPOTENT]")
    mapping = {}
    conn = sqlite3.connect(merged_db_path)
    cursor = conn.cursor()

    # Créer la table de mapping pour Bookmark si elle n'existe pas déjà
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS MergeMapping_Bookmark (
            SourceDb TEXT,
            OldID INTEGER,
            NewID INTEGER,
            PRIMARY KEY (SourceDb, OldID)
        )
    """)
    conn.commit()

    # Liste des fichiers sources à traiter
    for db_path in [file1_db, file2_db]:
        with sqlite3.connect(db_path) as src_conn:
            src_cursor = src_conn.cursor()

            # Vérifie si la table Bookmark existe dans le fichier source
            src_cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='Bookmark'")
            if not src_cursor.fetchone():
                print(f"Aucune table Bookmark trouvée dans {db_path}")
                continue

            src_cursor.execute("""
                SELECT BookmarkId, LocationId, PublicationLocationId, Slot, Title, 
                       Snippet, BlockType, BlockIdentifier
                FROM Bookmark
            """)
            for row in src_cursor.fetchall():
                old_id, loc_id, pub_loc_id, slot, title, snippet, block_type, block_id = row

                # Vérifier si cet enregistrement a déjà été fusionné
                cursor.execute("""
                    SELECT NewID FROM MergeMapping_Bookmark
                    WHERE SourceDb = ? AND OldID = ?
                """, (db_path, old_id))
                res = cursor.fetchone()
                if res:
                    new_id = res[0]
                    print(f"Bookmark déjà fusionné pour OldID {old_id} dans {db_path} -> NewID {new_id}")
                    mapping[(db_path, old_id)] = new_id
                    continue

                # Appliquer le mapping sur LocationId et PublicationLocationId
                new_loc_id = location_id_map.get((db_path, loc_id), loc_id)
                new_pub_loc_id = location_id_map.get((db_path, pub_loc_id), pub_loc_id)

                # Vérifier que les LocationIds existent dans la DB fusionnée
                cursor.execute("SELECT 1 FROM Location WHERE LocationId IN (?, ?)", (new_loc_id, new_pub_loc_id))
                if len(cursor.fetchall()) != 2:
                    print(
                        f"⚠️ LocationId introuvable pour Bookmark OldID {old_id} dans {db_path} (LocationId {new_loc_id} ou PublicationLocationId {new_pub_loc_id}), bookmark ignoré.")
                    continue

                # Vérifier et ajuster le slot pour éviter les conflits
                original_slot = slot
                while True:
                    cursor.execute("""
                        SELECT BookmarkId FROM Bookmark
                        WHERE PublicationLocationId = ? AND Slot = ?
                    """, (new_pub_loc_id, slot))
                    if not cursor.fetchone():
                        break
                    print(
                        f"Conflit détecté pour PublicationLocationId={new_pub_loc_id}, Slot={slot}. Incrémentation du slot.")
                    slot += 1

                print(
                    f"Insertion Bookmark: OldID {old_id} (slot initial {original_slot} -> {slot}), PubLocId {new_pub_loc_id}, Title='{title}'")
                cursor.execute("""
                    INSERT INTO Bookmark
                    (LocationId, PublicationLocationId, Slot, Title,
                     Snippet, BlockType, BlockIdentifier)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (new_loc_id, new_pub_loc_id, slot, title, snippet, block_type, block_id))
                new_id = cursor.lastrowid
                mapping[(db_path, old_id)] = new_id

                # Enregistrer le mapping dans la table dédiée
                cursor.execute("""
                    INSERT INTO MergeMapping_Bookmark (SourceDb, OldID, NewID)
                    VALUES (?, ?, ?)
                """, (db_path, old_id, new_id))
                conn.commit()

    conn.close()
    print("Fusion Bookmarks terminée (idempotente).")
    return mapping


def merge_notes(merged_db_path, file1_db, file2_db, location_id_map, usermark_guid_map):
    """
    Fusionne la table Note de façon à ne pas écraser les données existantes.
    Si une note avec le même GUID existe déjà mais que le contenu diffère,
    on insère une nouvelle note avec un nouveau GUID et on laisse en place la note existante.
    """
    print("\n=== FUSION DES NOTES (résolution de conflit par insertion) ===")
    inserted = 0

    conn = sqlite3.connect(merged_db_path)
    cursor = conn.cursor()

    # On itère d'abord sur file1, puis sur file2
    for db_path in [file1_db, file2_db]:
        with sqlite3.connect(db_path) as src_conn:
            src_cursor = src_conn.cursor()
            src_cursor.execute("""
                SELECT n.Guid, um.UserMarkGuid, n.LocationId, n.Title, n.Content,
                       n.LastModified, n.Created, n.BlockType, n.BlockIdentifier
                FROM Note n
                LEFT JOIN UserMark um ON n.UserMarkId = um.UserMarkId
            """)
            for (guid, usermark_guid, location_id, title, content,
                 last_modified, created, block_type, block_identifier) in src_cursor.fetchall():

                # Appliquer les mappings sur LocationId et UserMarkId
                normalized_key = (os.path.normpath(db_path), location_id)
                normalized_map = {(os.path.normpath(k[0]), k[1]): v for k, v in location_id_map.items()}
                new_location_id = normalized_map.get(normalized_key) if location_id else None

                new_usermark_id = usermark_guid_map.get(usermark_guid) if usermark_guid else None

                if new_location_id is None:
                    print(f"⚠️ LocationId introuvable pour Note guid={guid} (source: {db_path}), ignorée.")
                    continue

                # Vérifier l'existence d'une note avec ce GUID dans la DB fusionnée
                cursor.execute("SELECT Title, Content FROM Note WHERE Guid = ?", (guid,))
                existing = cursor.fetchone()

                if existing:
                    # Si le contenu est identique, on ne fait rien
                    if existing[0] == title and existing[1] == content:
                        print(f"Note guid={guid} déjà présente et identique (source: {db_path}), aucune action.")
                        continue
                    else:
                        # Conflit détecté : le GUID existe mais le contenu est différent.
                        # On génère alors un nouveau GUID pour insérer la note sans écraser l'existante.
                        new_guid = str(uuid.uuid4())
                        print(f"Conflit pour Note guid={guid} (source: {db_path}). "
                              f"Insertion d'une nouvelle note avec nouveau GUID {new_guid}.")
                        guid_to_insert = new_guid
                else:
                    guid_to_insert = guid

                # Insertion de la note (soit en conservant le GUID, soit avec un nouveau)
                cursor.execute("""
                    INSERT INTO Note
                    (Guid, UserMarkId, LocationId, Title, Content,
                     LastModified, Created, BlockType, BlockIdentifier)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    guid_to_insert,
                    new_usermark_id,
                    new_location_id,
                    title,
                    content,
                    last_modified,
                    created,
                    block_type,
                    block_identifier
                ))
                inserted += 1

    conn.commit()
    conn.close()
    print(f"✅ Total notes insérées: {inserted}")


def merge_usermark_with_id_relabeling(merged_db_path, source_db_path, location_id_map):
    conn_merged = sqlite3.connect(merged_db_path)
    cur_merged = conn_merged.cursor()

    # Récupère les IDs existants pour éviter les conflits
    cur_merged.execute("SELECT UserMarkId FROM UserMark")
    existing_ids = set(row[0] for row in cur_merged.fetchall())
    current_max_id = max(existing_ids) if existing_ids else 0

    # Charge les données source
    conn_source = sqlite3.connect(source_db_path)
    cur_source = conn_source.cursor()
    cur_source.execute("SELECT UserMarkId, ColorIndex, LocationId, StyleIndex, UserMarkGuid, Version FROM UserMark")
    source_rows = cur_source.fetchall()
    conn_source.close()

    # Création du mapping UserMarkId (si conflits)
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

    # Insertion dans la base fusionnée avec LocationId mappé
    for row in source_rows:
        old_id = row[0]
        new_id = replacements[old_id]
        ColorIndex = row[1]
        LocationId = row[2]
        StyleIndex = row[3]
        UserMarkGuid = row[4]
        Version = row[5]

        # Mapping du LocationId
        mapped_loc_id = location_id_map.get((source_db_path, LocationId), LocationId)

        try:
            cur_merged.execute("""
                INSERT INTO UserMark (UserMarkId, ColorIndex, LocationId, StyleIndex, UserMarkGuid, Version)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (new_id, ColorIndex, mapped_loc_id, StyleIndex, UserMarkGuid, Version))
        except Exception as e:
            print(f"Erreur insertion UserMark old_id={old_id}, new_id={new_id}: {e}")

    conn_merged.commit()
    conn_merged.close()
    return replacements


def merge_blockrange_from_two_sources(merged_db_path, file1_db, file2_db):
    print("\n=== FUSION BLOCKRANGE ===")

    # 1) Vérification initiale
    with sqlite3.connect(merged_db_path) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM BlockRange")
        print(f"BlockRanges initiaux: {cursor.fetchone()[0]}")

    # 2) Récupération des mappings
    try:
        with sqlite3.connect(merged_db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT UserMarkId, UserMarkGuid FROM UserMark")
            usermark_guid_map = {guid: uid for uid, guid in cursor.fetchall()}
            print(f"UserMark GUIDs: {usermark_guid_map}")
    except Exception as e:
        print(f"❌ Erreur mapping UserMark: {e}")
        return False

    # 3) Traitement des sources
    with sqlite3.connect(merged_db_path) as dest_conn:
        dest_cursor = dest_conn.cursor()

        for db_path in [file1_db, file2_db]:
            print(f"\nTraitement de {db_path}")
            try:
                with sqlite3.connect(db_path) as src_conn:
                    src_cursor = src_conn.cursor()

                    # Requête optimisée
                    src_cursor.execute("""
                        SELECT br.BlockType, br.Identifier, br.StartToken, br.EndToken, um.UserMarkGuid
                        FROM BlockRange br
                        JOIN UserMark um ON br.UserMarkId = um.UserMarkId
                        ORDER BY br.BlockType, br.Identifier
                    """)

                    for row in src_cursor.fetchall():
                        block_type, identifier, start_token, end_token, usermark_guid = row
                        new_usermark_id = usermark_guid_map.get(usermark_guid)

                        if not new_usermark_id:
                            print(f"⚠️ GUID non mappé: {usermark_guid}")
                            continue

                        try:
                            # Vérification d'existence
                            dest_cursor.execute("""
                                SELECT 1 FROM BlockRange
                                WHERE BlockType=? AND Identifier=? AND UserMarkId=?
                                AND StartToken=? AND EndToken=?
                            """, (block_type, identifier, new_usermark_id, start_token, end_token))

                            if dest_cursor.fetchone():
                                print(f"⏩ Existe déjà: {row}")
                                continue

                            # Insertion
                            dest_cursor.execute("""
                                INSERT INTO BlockRange
                                (BlockType, Identifier, StartToken, EndToken, UserMarkId)
                                VALUES (?, ?, ?, ?, ?)
                            """, (block_type, identifier, start_token, end_token, new_usermark_id))

                            dest_conn.commit()
                            print(f"✅ Inserté: {row}")

                        except sqlite3.IntegrityError as e:
                            dest_conn.rollback()
                            print(f"❌ Erreur intégrité: {e}")
                            print(f"Ligne problématique: {row}")
                            # Debug avancé
                            dest_cursor.execute("PRAGMA foreign_key_check")
                            print("Problèmes clés étrangères:", dest_cursor.fetchall())
                            return False

            except Exception as e:
                print(f"❌ Erreur fichier {db_path}: {e}")
                return False

        # Vérification finale
        dest_cursor.execute("SELECT COUNT(*) FROM BlockRange")
        print(f"Total BlockRanges après fusion: {dest_cursor.fetchone()[0]}")

    return True


def merge_inputfields(merged_db_path, file1_db, file2_db, location_id_map):
    print("\n[FUSION INPUTFIELD - IDÉMPOTENTE]")
    inserted_count = 0
    skipped_count = 0
    missing_count = 0

    conn = sqlite3.connect(merged_db_path)
    cursor = conn.cursor()

    # Créer la table de mapping pour InputField si elle n'existe pas déjà
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS MergeMapping_InputField (
            SourceDb TEXT,
            OldLocationId INTEGER,
            TextTag TEXT,
            Value TEXT,
            PRIMARY KEY (SourceDb, OldLocationId, TextTag)
        )
    """)
    conn.commit()

    for db_path in [file1_db, file2_db]:
        with sqlite3.connect(db_path) as src_conn:
            src_cursor = src_conn.cursor()
            src_cursor.execute("SELECT LocationId, TextTag, Value FROM InputField")
            rows = src_cursor.fetchall()

            for loc_id, tag, value in rows:
                # Appliquer le mapping de LocationId pour ce fichier source
                mapped_loc = location_id_map.get((db_path, loc_id))
                if mapped_loc is None:
                    print(f"❌ LocationId {loc_id} (depuis {db_path}) non mappé — ligne ignorée")
                    missing_count += 1
                    continue

                # Vérifier dans la table de mapping si cette entrée a déjà été fusionnée
                cursor.execute("""
                    SELECT 1 FROM MergeMapping_InputField 
                    WHERE SourceDb = ? AND OldLocationId = ? AND TextTag = ?
                """, (db_path, loc_id, tag))
                if cursor.fetchone():
                    print(f"⏩ InputField déjà fusionnée : Source={db_path}, OldLocId={loc_id}, Tag={tag}")
                    skipped_count += 1
                    continue

                # Vérifier si une ligne identique est déjà présente dans la table InputField de la DB fusionnée
                cursor.execute("""
                    SELECT 1 FROM InputField
                    WHERE LocationId = ? AND TextTag = ? AND Value = ?
                """, (mapped_loc, tag, value))
                if cursor.fetchone():
                    print(f"⏩ Doublon détecté dans InputField pour Loc={mapped_loc}, Tag={tag} (ajout mapping)")
                    cursor.execute("""
                        INSERT OR IGNORE INTO MergeMapping_InputField (SourceDb, OldLocationId, TextTag, Value)
                        VALUES (?, ?, ?, ?)
                    """, (db_path, loc_id, tag, value))
                    conn.commit()
                    skipped_count += 1
                    continue

                try:
                    cursor.execute("""
                        INSERT INTO InputField (LocationId, TextTag, Value)
                        VALUES (?, ?, ?)
                    """, (mapped_loc, tag, value))
                    inserted_count += 1
                    print(f"✅ Insert InputField : Loc={mapped_loc}, Tag={tag}")

                    # Enregistrer le mapping de cette ligne fusionnée
                    cursor.execute("""
                        INSERT INTO MergeMapping_InputField (SourceDb, OldLocationId, TextTag, Value)
                        VALUES (?, ?, ?, ?)
                    """, (db_path, loc_id, tag, value))
                    conn.commit()
                except Exception as e:
                    print(f"❌ Erreur insertion InputField (Loc={mapped_loc}, Tag={tag}): {e}")
    conn.close()

    print("\n=== STATISTIQUES INPUTFIELD ===")
    print(f"✅ Lignes insérées     : {inserted_count}")
    print(f"⏩ Doublons ignorés    : {skipped_count}")
    print(f"❌ LocationId manquants : {missing_count}")


def update_location_references(merged_db_path, location_replacements):
    conn = sqlite3.connect(merged_db_path)
    cursor = conn.cursor()

    tables_with_single_location = [
        "InputField", "Note", "PlaylistItemLocationMap", "TagMap", "UserMark"
    ]

    for table in tables_with_single_location:
        for old_loc, new_loc in location_replacements.items():
            try:
                if table == "InputField":
                    # On gère ligne par ligne pour éviter les doublons
                    cursor.execute("SELECT TextTag FROM InputField WHERE LocationId = ?", (old_loc,))
                    texttags = cursor.fetchall()
                    for (texttag,) in texttags:
                        cursor.execute("""
                            SELECT 1 FROM InputField WHERE LocationId = ? AND TextTag = ?
                        """, (new_loc, texttag))
                        if cursor.fetchone():
                            # Conflit détecté, on cherche un TextTag libre
                            base_tag = texttag
                            i = 1
                            new_tag = f"{base_tag}_{i}"
                            while True:
                                cursor.execute("""
                                    SELECT 1 FROM InputField WHERE LocationId = ? AND TextTag = ?
                                """, (new_loc, new_tag))
                                if not cursor.fetchone():
                                    break
                                i += 1
                                new_tag = f"{base_tag}_{i}"

                            # Met à jour en modifiant le TextTag
                            cursor.execute("""
                                UPDATE InputField SET LocationId = ?, TextTag = ?
                                WHERE LocationId = ? AND TextTag = ?
                            """, (new_loc, new_tag, old_loc, base_tag))
                            print(
                                f"⚠️ Conflit évité : LocationId {old_loc} -> {new_loc}, TextTag={base_tag} devient {new_tag}")
                        cursor.execute("""
                            UPDATE InputField SET LocationId = ?
                            WHERE LocationId = ? AND TextTag = ?
                        """, (new_loc, old_loc, texttag))
                        print(f"InputField mis à jour: LocationId {old_loc} -> {new_loc}, TextTag={texttag}")
                else:
                    cursor.execute(f"UPDATE {table} SET LocationId = ? WHERE LocationId = ?", (new_loc, old_loc))
                    print(f"{table} mis à jour: LocationId {old_loc} -> {new_loc}")
            except Exception as e:
                print(f"Erreur mise à jour {table} pour LocationId {old_loc}: {e}")

    # Mise à jour sécurisée pour Bookmark
    for old_loc, new_loc in location_replacements.items():
        try:
            cursor.execute("UPDATE Bookmark SET LocationId = ? WHERE LocationId = ?", (new_loc, old_loc))
            print(f"Bookmark LocationId mis à jour: {old_loc} -> {new_loc}")
        except Exception as e:
            print(f"Erreur mise à jour Bookmark LocationId {old_loc}: {e}")

        try:
            # Sélection des lignes concernées par PublicationLocationId
            cursor.execute("""
                SELECT BookmarkId, Slot FROM Bookmark
                WHERE PublicationLocationId = ?
            """, (old_loc,))
            rows = cursor.fetchall()

            for bookmark_id, slot in rows:
                cursor.execute("""
                    SELECT 1 FROM Bookmark
                    WHERE PublicationLocationId = ? AND Slot = ? AND BookmarkId != ?
                """, (new_loc, slot, bookmark_id))
                conflict = cursor.fetchone()

                if conflict:
                    print(f"⚠️ Mise à jour ignorée pour Bookmark ID {bookmark_id} (conflit avec PublicationLocationId={new_loc}, Slot={slot})")
                else:
                    cursor.execute("""
                        UPDATE Bookmark
                        SET PublicationLocationId = ?
                        WHERE BookmarkId = ?
                    """, (new_loc, bookmark_id))
                    print(f"Bookmark PublicationLocationId mis à jour: {old_loc} -> {new_loc} (BookmarkId {bookmark_id})")

        except Exception as e:
            print(f"Erreur sécurisée mise à jour PublicationLocationId {old_loc}: {e}")

    conn.commit()
    conn.close()


def merge_usermark_from_sources(merged_db_path, file1_db, file2_db, location_id_map):
    """
    Fusionne la table UserMark des deux bases sources dans la DB fusionnée de façon idempotente.
    Pour chaque UserMark, un mapping (SourceDb, OldUserMarkId) -> NewUserMarkId est enregistré dans la table
    MergeMapping_UserMark afin d'éviter les réinsertion en doublon.
    Retourne un dictionnaire mapping de UserMark GUID vers le NewUserMarkId.
    """
    print("\n[FUSION USERMARK - IDÉMPOTENTE]")
    mapping = {}
    conn = sqlite3.connect(merged_db_path)
    cursor = conn.cursor()
    # Crée la table de mapping pour UserMark (si elle n'existe pas)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS MergeMapping_UserMark (
            SourceDb TEXT,
            OldUserMarkId INTEGER,
            NewUserMarkId INTEGER,
            PRIMARY KEY (SourceDb, OldUserMarkId)
        )
    """)
    conn.commit()

    # Récupère le dernier ID dans la table UserMark de la DB fusionnée
    cursor.execute("SELECT COALESCE(MAX(UserMarkId), 0) FROM UserMark")
    max_id = cursor.fetchone()[0]

    # Traiter les deux fichiers source
    for db_path in [file1_db, file2_db]:
        with sqlite3.connect(db_path) as src_conn:
            src_cursor = src_conn.cursor()
            src_cursor.execute("""
                SELECT UserMarkId, ColorIndex, LocationId, StyleIndex, UserMarkGuid, Version 
                FROM UserMark
            """)
            rows = src_cursor.fetchall()
            for old_um_id, color, loc_id, style, guid, version in rows:
                # Vérifier si cet enregistrement a déjà été traité
                cursor.execute("""
                    SELECT NewUserMarkId FROM MergeMapping_UserMark
                    WHERE SourceDb = ? AND OldUserMarkId = ?
                """, (db_path, old_um_id))
                res = cursor.fetchone()
                if res:
                    mapping[(db_path, old_um_id)] = res[0]
                    continue

                # Appliquer le mapping sur LocationId
                new_loc = location_id_map.get((db_path, loc_id), loc_id) if loc_id is not None else None

                # Vérifier s'il existe déjà un UserMark dans la DB fusionnée avec ce GUID
                cursor.execute("""
                    SELECT UserMarkId, ColorIndex, LocationId, StyleIndex, Version 
                    FROM UserMark 
                    WHERE UserMarkGuid = ?
                """, (guid,))
                existing = cursor.fetchone()
                if existing:
                    existing_id, ex_color, ex_loc, ex_style, ex_version = existing
                    # Si les données sont identiques, on peut réutiliser cet enregistrement
                    if (ex_color, ex_loc, ex_style, ex_version) == (color, new_loc, style, version):
                        new_um_id = existing_id
                    else:
                        # Conflit : on doit créer un nouvel enregistrement
                        max_id += 1
                        new_um_id = max_id
                        cursor.execute("""
                            INSERT INTO UserMark (UserMarkId, ColorIndex, LocationId, StyleIndex, UserMarkGuid, Version)
                            VALUES (?, ?, ?, ?, ?, ?)
                        """, (new_um_id, color, new_loc, style, guid, version))
                else:
                    max_id += 1
                    new_um_id = max_id
                    cursor.execute("""
                        INSERT INTO UserMark (UserMarkId, ColorIndex, LocationId, StyleIndex, UserMarkGuid, Version)
                        VALUES (?, ?, ?, ?, ?, ?)
                    """, (new_um_id, color, new_loc, style, guid, version))
                mapping[(db_path, old_um_id)] = new_um_id
                # Enregistrer le mapping dans la table dédiée
                cursor.execute("""
                    INSERT INTO MergeMapping_UserMark (SourceDb, OldUserMarkId, NewUserMarkId)
                    VALUES (?, ?, ?)
                """, (db_path, old_um_id, new_um_id))
                conn.commit()
                # Pour faciliter l'usage en aval, on peut aussi construire un mapping par GUID
                mapping[guid] = new_um_id

    conn.close()
    print("Fusion UserMark terminée (idempotente).")
    return mapping


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
    Fusionne les enregistrements de la table Location depuis file1 et file2
    dans la base fusionnée de façon idempotente.
    Un mapping (SourceDb, OldID) -> NewID est persisté dans une table dédiée pour éviter
    de réinsérer des entrées déjà fusionnées.
    Retourne un dictionnaire de mapping.
    """
    def read_locations(db_path):
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()
        cur.execute("""
            SELECT LocationId, BookNumber, ChapterNumber, DocumentId, Track,
                   IssueTagNumber, KeySymbol, MepsLanguage, Type, Title
            FROM Location
        """)
        rows = cur.fetchall()
        conn.close()
        return [(db_path,) + row for row in rows]

    locations = read_locations(file1_db) + read_locations(file2_db)

    conn = sqlite3.connect(merged_db_path)
    cur = conn.cursor()

    # Créer la table de mapping pour Location si elle n'existe pas
    cur.execute("""
        CREATE TABLE IF NOT EXISTS MergeMapping_Location (
            SourceDb TEXT,
            OldID INTEGER,
            NewID INTEGER,
            PRIMARY KEY (SourceDb, OldID)
        )
    """)
    conn.commit()

    # Récupérer le dernier ID de Location déjà présent dans la DB fusionnée
    cur.execute("SELECT MAX(LocationId) FROM Location")
    max_existing_id = cur.fetchone()[0]
    current_max_id = max_existing_id if max_existing_id is not None else 0

    location_id_map = {}

    for entry in locations:
        db_source = entry[0]
        old_loc_id, book_num, chap_num, doc_id, track, issue, key_sym, meps_lang, loc_type, title = entry[1:]

        # Vérifier si ce Location a déjà été fusionné depuis ce fichier source
        cur.execute("""
            SELECT NewID FROM MergeMapping_Location
            WHERE SourceDb = ? AND OldID = ?
        """, (db_source, old_loc_id))
        res = cur.fetchone()
        if res:
            new_id = res[0]
            print(f"⏩ Location déjà fusionnée pour OldID {old_loc_id} depuis {db_source} -> NewID {new_id}")
            location_id_map[(db_source, old_loc_id)] = new_id
            continue

        # Chercher une correspondance déjà existante dans la DB fusionnée
        found = False
        new_loc_id = None

        # Première contrainte UNIQUE pour publications classiques
        if None not in (book_num, chap_num, key_sym, meps_lang, loc_type):
            cur.execute("""
                SELECT LocationId FROM Location
                WHERE BookNumber = ? AND ChapterNumber = ? AND KeySymbol = ? AND MepsLanguage = ? AND Type = ?
            """, (book_num, chap_num, key_sym, meps_lang, loc_type))
            result = cur.fetchone()
            if result:
                found = True
                new_loc_id = result[0]

        # Sinon, vérification pour périodiques
        if not found and None not in (key_sym, issue, meps_lang, doc_id, track, loc_type):
            cur.execute("""
                SELECT LocationId FROM Location
                WHERE KeySymbol = ? AND IssueTagNumber = ? AND MepsLanguage = ? AND DocumentId = ? AND Track = ? AND Type = ?
            """, (key_sym, issue, meps_lang, doc_id, track, loc_type))
            result = cur.fetchone()
            if result:
                found = True
                new_loc_id = result[0]

        if not found:
            # Aucune correspondance : on insère une nouvelle ligne
            current_max_id += 1
            new_loc_id = current_max_id
            new_row = (
                new_loc_id, book_num, chap_num, doc_id, track, issue,
                key_sym, meps_lang, loc_type, title
            )
            try:
                cur.execute("""
                    INSERT INTO Location
                    (LocationId, BookNumber, ChapterNumber, DocumentId, Track, IssueTagNumber,
                     KeySymbol, MepsLanguage, Type, Title)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, new_row)
                print(f"Insertion Location depuis {db_source} : {new_row}")
            except sqlite3.IntegrityError as e:
                print(f"⚠️ Erreur insertion Location pour {new_row}: {e}")
                continue
        else:
            print(f"⏩ Location existante pour OldID {old_loc_id} depuis {db_source} -> NewID {new_loc_id}")

        location_id_map[(db_source, old_loc_id)] = new_loc_id

        # Enregistrer le mapping dans la table de mapping pour Location
        cur.execute("""
            INSERT INTO MergeMapping_Location (SourceDb, OldID, NewID)
            VALUES (?, ?, ?)
        """, (db_source, old_loc_id, new_loc_id))
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
    Fusionne les Tags et la table TagMap de façon idempotente.
    Pour chaque tag (sachant qu'un tag est identifié par (Type, Name)), on vérifie s'il existe déjà dans la DB fusionnée.
    En cas d'insertion, le mapping (SourceDb, OldTagId) -> NewTagId est stocké dans MergeMapping_Tag.
    De même, pour chaque entrée TagMap, si des conflits de Position sont détectés, la Position est incrémentée.
    Un mapping (SourceDb, OldTagMapId) -> NewTagMapId est enregistré dans MergeMapping_TagMap.
    Retourne deux mappings : tag_id_map et tagmap_id_map.
    """
    print("\n[FUSION TAGS ET TAGMAP - IDÉMPOTENT]")
    conn = sqlite3.connect(merged_db_path)
    cursor = conn.cursor()

    # Créer les tables de mapping pour Tag et TagMap si elles n'existent pas
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS MergeMapping_Tag (
            SourceDb TEXT,
            OldTagId INTEGER,
            NewTagId INTEGER,
            PRIMARY KEY (SourceDb, OldTagId)
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS MergeMapping_TagMap (
            SourceDb TEXT,
            OldTagMapId INTEGER,
            NewTagMapId INTEGER,
            PRIMARY KEY (SourceDb, OldTagMapId)
        )
    """)
    conn.commit()

    # 1. Fusion des Tags
    cursor.execute("SELECT COALESCE(MAX(TagId), 0) FROM Tag")
    max_tag_id = cursor.fetchone()[0]

    tag_id_map = {}
    for db_path in [file1_db, file2_db]:
        with sqlite3.connect(db_path) as src_conn:
            src_cursor = src_conn.cursor()
            src_cursor.execute("SELECT TagId, Type, Name FROM Tag")
            for tag_id, tag_type, tag_name in src_cursor.fetchall():
                # Vérifier si ce tag a déjà été fusionné
                cursor.execute("""
                    SELECT NewTagId FROM MergeMapping_Tag
                    WHERE SourceDb = ? AND OldTagId = ?
                """, (db_path, tag_id))
                res = cursor.fetchone()
                if res:
                    new_tag_id = res[0]
                    tag_id_map[(db_path, tag_id)] = new_tag_id
                    print(f"Tag déjà fusionné: '{tag_name}' (OldTagId {tag_id}) -> NewTagId {new_tag_id}")
                    continue

                # Recherche d'un tag identique déjà présent dans la DB fusionnée
                cursor.execute("SELECT TagId FROM Tag WHERE Type = ? AND Name = ?", (tag_type, tag_name))
                existing = cursor.fetchone()
                if existing:
                    new_tag_id = existing[0]
                    print(f"Tag existant trouvé: '{tag_name}' (OldTagId {tag_id}) -> NewTagId {new_tag_id}")
                else:
                    max_tag_id += 1
                    new_tag_id = max_tag_id
                    cursor.execute("INSERT INTO Tag (TagId, Type, Name) VALUES (?, ?, ?)",
                                   (new_tag_id, tag_type, tag_name))
                    print(f"Insertion Tag: '{tag_name}' (OldTagId {tag_id}) -> NewTagId {new_tag_id}")

                tag_id_map[(db_path, tag_id)] = new_tag_id

                # Enregistrer le mapping pour ce tag
                cursor.execute("INSERT INTO MergeMapping_Tag (SourceDb, OldTagId, NewTagId) VALUES (?, ?, ?)",
                               (db_path, tag_id, new_tag_id))
                conn.commit()

    # 2. Fusion des TagMap
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
            for old_tagmap_id, playlist_item_id, location_id, note_id, old_tag_id, position in src_cursor.fetchall():
                # Vérifier si cet enregistrement TagMap est déjà fusionné
                cursor.execute("""
                    SELECT NewTagMapId FROM MergeMapping_TagMap
                    WHERE SourceDb = ? AND OldTagMapId = ?
                """, (db_path, old_tagmap_id))
                res = cursor.fetchone()
                if res:
                    tagmap_id_map[(db_path, old_tagmap_id)] = res[0]
                    print(f"TagMap déjà fusionné: OldTagMapId {old_tagmap_id} -> NewTagMapId {res[0]}")
                    continue

                # Appliquer les mappings aux références
                new_note_id = note_mapping.get((db_path, note_id)) if note_id else None
                norm_key = (os.path.normpath(db_path), location_id)
                normalized_map = {(os.path.normpath(k[0]), k[1]): v for k, v in location_id_map.items()}
                new_location_id = normalized_map.get(norm_key) if location_id else None
                new_playlist_item_id = playlist_item_id_map.get((db_path, playlist_item_id)) if playlist_item_id else None
                new_tag_id = tag_id_map.get((db_path, old_tag_id))

                # S'assurer qu'il y a au moins une référence parmi Note, Location ou PlaylistItem
                if all(v is None for v in [new_note_id, new_location_id, new_playlist_item_id]):
                    print(f"Aucune référence valide pour TagMap OldTagMapId {old_tagmap_id} (db: {db_path}), ignorée.")
                    continue

                # Ajuster la Position en cas de conflit pour ce Tag
                tentative = position
                while True:
                    cursor.execute("SELECT 1 FROM TagMap WHERE TagId = ? AND Position = ?", (new_tag_id, tentative))
                    if not cursor.fetchone():
                        break
                    print(f"Conflit sur TagMap pour TagId {new_tag_id} à Position {tentative}. Incrémentation.")
                    tentative += 1

                max_tagmap_id += 1
                new_tagmap_id = max_tagmap_id
                try:
                    cursor.execute("""
                        INSERT INTO TagMap (TagMapId, PlaylistItemId, LocationId, NoteId, TagId, Position)
                        VALUES (?, ?, ?, ?, ?, ?)
                    """, (new_tagmap_id, new_playlist_item_id, new_location_id, new_note_id, new_tag_id, tentative))
                    print(f"Insertion TagMap: OldTagMapId {old_tagmap_id} -> NewTagMapId {new_tagmap_id} avec Position {tentative}")
                except sqlite3.IntegrityError as e:
                    print(f"Erreur d'insertion TagMap pour OldTagMapId {old_tagmap_id}: {e}")
                    continue

                tagmap_id_map[(db_path, old_tagmap_id)] = new_tagmap_id
                cursor.execute("INSERT INTO MergeMapping_TagMap (SourceDb, OldTagMapId, NewTagMapId) VALUES (?, ?, ?)",
                               (db_path, old_tagmap_id, new_tagmap_id))
                conn.commit()

    conn.close()
    print("Fusion des Tags et TagMap terminée (idempotente).")
    return tag_id_map, tagmap_id_map


def merge_playlist_items(merged_db_path, file1_db, file2_db, im_mapping=None):
    """
    Fusionne la table PlaylistItem des deux bases sources dans la base fusionnée de façon idempotente.
    Pour chaque enregistrement, un mapping (SourceDb, OldItemId) -> NewItemId est enregistré dans la table
    MergeMapping_PlaylistItem afin d'éviter de dupliquer les entrées lors d'exécutions répétées.
    En cas de doublon (défini par une clé composée des champs Label, StartTrimOffsetTicks, EndTrimOffsetTicks,
    Accuracy, EndAction et ThumbnailFilePath), la même entrée est réutilisée.
    Retourne un mapping { (db_source, old_item_id): new_item_id }.
    """
    print("\n[FUSION PLAYLISTITEMS - IDÉMPOTENTE]")
    mapping = {}
    conn = sqlite3.connect(merged_db_path, timeout=30)
    conn.execute("PRAGMA busy_timeout = 10000")
    cursor = conn.cursor()

    # Créer la table de mapping pour PlaylistItem si elle n'existe pas
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS MergeMapping_PlaylistItem (
            SourceDb TEXT,
            OldItemId INTEGER,
            NewItemId INTEGER,
            PRIMARY KEY (SourceDb, OldItemId)
        )
    """)
    conn.commit()

    # Vérifier que la table PlaylistItem existe dans la base fusionnée.
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='PlaylistItem'")
    if not cursor.fetchone():
        print("[ERREUR] La table PlaylistItem n'existe pas dans la DB fusionnée.")
        conn.close()
        return {}

    # Fonctions utilitaires pour normaliser les valeurs textuelles et numériques.
    def safe_text(val):
        return val if val is not None else ""

    def safe_number(val):
        return val if val is not None else 0

    # Dictionnaire pour détecter les doublons dans la DB fusionnée, clé = (Label, StartTrim, EndTrim, Accuracy, EndAction, Thumbnail)
    existing_items = {}

    # Fonction pour lire les PlaylistItems d'une source.
    def read_playlist_items(db_path):
        with sqlite3.connect(db_path) as src_conn:
            cur_source = src_conn.cursor()
            cur_source.execute("""
                SELECT PlaylistItemId, Label, StartTrimOffsetTicks, EndTrimOffsetTicks, Accuracy, EndAction, ThumbnailFilePath
                FROM PlaylistItem
            """)
            rows = cur_source.fetchall()
        return [(db_path,) + row for row in rows]

    all_items = read_playlist_items(file1_db) + read_playlist_items(file2_db)
    print(f"Total playlist items lus : {len(all_items)}")

    # Itération sur tous les items des deux sources.
    for item in all_items:
        db_source = item[0]
        old_id, label, start_trim, end_trim, accuracy, end_action, thumb_path = item[1:]

        norm_label = safe_text(label)
        norm_start = safe_number(start_trim)
        norm_end = safe_number(end_trim)
        norm_thumb = safe_text(thumb_path)
        # Définir la clé de correspondance permettant de détecter des doublons
        key = (norm_label, norm_start, norm_end, accuracy, end_action, norm_thumb)

        # Vérifier si cet enregistrement a déjà été traité en consultant la table de mapping
        cursor.execute("SELECT NewItemId FROM MergeMapping_PlaylistItem WHERE SourceDb = ? AND OldItemId = ?", (db_source, old_id))
        res = cursor.fetchone()
        if res:
            new_id = res[0]
            print(f"PlaylistItem déjà fusionné pour OldID {old_id} de {db_source} -> NewID {new_id}")
            mapping[(db_source, old_id)] = new_id
            # Enregistrement dans existing_items s'il n'est pas déjà présent.
            if key not in existing_items:
                existing_items[key] = new_id
            continue

        if key in existing_items:
            # Doublon détecté : on réutilise le NewItemId déjà inséré.
            new_id = existing_items[key]
            print(f"Doublon détecté pour key {key}. Réutilisation de l'ID {new_id} pour OldID {old_id} de {db_source}.")
        else:
            try:
                cursor.execute("""
                    INSERT INTO PlaylistItem 
                    (Label, StartTrimOffsetTicks, EndTrimOffsetTicks, Accuracy, EndAction, ThumbnailFilePath)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (label, start_trim, end_trim, accuracy, end_action, thumb_path))
                new_id = cursor.lastrowid
                print(f"Insertion réussie avec nouvel ID {new_id} pour '{label}' (OldID {old_id} de {db_source})")
                existing_items[key] = new_id
            except sqlite3.IntegrityError as e:
                print(f"Erreur insertion PlaylistItem OldID {old_id} de {db_source}: {e}")
                continue

        # Enregistrer le mapping pour cet enregistrement.
        cursor.execute("""
            INSERT INTO MergeMapping_PlaylistItem (SourceDb, OldItemId, NewItemId)
            VALUES (?, ?, ?)
        """, (db_source, old_id, new_id))
        conn.commit()
        mapping[(db_source, old_id)] = new_id

    print(f"Total items mappés: {len(mapping)}")
    conn.close()
    return mapping


def merge_playlist_item_accuracy(merged_db_path, file1_db, file2_db):
    """
    Fusionne la table PlaylistItemAccuracy de façon idempotente.
    Utilise INSERT OR IGNORE car cette table est simple.
    Retourne l'ID max final.
    """
    print("\n[FUSION PLAYLISTITEMACCURACY]")
    conn = sqlite3.connect(merged_db_path)
    cursor = conn.cursor()

    cursor.execute("SELECT COALESCE(MAX(PlaylistItemAccuracyId), 0) FROM PlaylistItemAccuracy")
    max_acc_id = cursor.fetchone()[0] or 0
    print(f"ID max initial: {max_acc_id}")

    for db_path in [file1_db, file2_db]:
        with sqlite3.connect(db_path) as src_conn:
            src_cursor = src_conn.cursor()
            src_cursor.execute("SELECT * FROM PlaylistItemAccuracy")
            records = src_cursor.fetchall()
            print(f"{len(records)} records trouvés dans {os.path.basename(db_path)}")
            for acc_id, desc in records:
                try:
                    cursor.execute("INSERT OR IGNORE INTO PlaylistItemAccuracy VALUES (?, ?)", (acc_id, desc))
                    max_acc_id = max(max_acc_id, acc_id)
                except sqlite3.IntegrityError:
                    print(f"Duplicate PlaylistItemAccuracy: {acc_id} (ignoré)")
    conn.commit()
    conn.close()
    print(f"ID max final: {max_acc_id}")
    return max_acc_id


def merge_playlist_item_location_map(merged_db_path, file1_db, file2_db, item_id_map, location_id_map):
    """
    Fusionne la table PlaylistItemLocationMap de façon idempotente en appliquant
    le mapping des PlaylistItems et des Locations.
    """
    print("\n[FUSION PLAYLISTITEMLOCATIONMAP]")
    conn = sqlite3.connect(merged_db_path)
    cursor = conn.cursor()

    for db_path in [file1_db, file2_db]:
        with sqlite3.connect(db_path) as src_conn:
            src_cursor = src_conn.cursor()
            src_cursor.execute("""
                SELECT PlaylistItemId, LocationId, MajorMultimediaType, BaseDurationTicks
                FROM PlaylistItemLocationMap
            """)
            mappings = src_cursor.fetchall()
            print(f"{len(mappings)} mappings trouvés dans {os.path.basename(db_path)}")
            for old_item_id, old_loc_id, mm_type, duration in mappings:
                new_item_id = item_id_map.get((os.path.normpath(db_path), old_item_id))
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
                    print(
                        f"⚠️ Mapping manquant pour PlaylistItemId={old_item_id}, LocationId={old_loc_id} (db: {db_path})")
    conn.commit()
    conn.close()


def merge_playlist_item_media_map(merged_db_path, file1_db, file2_db, item_id_map, independent_media_map):
    """
    Fusionne la table PlaylistItemIndependentMediaMap des deux sources dans
    PlaylistItemMediaMap de la DB fusionnée de façon idempotente.
    Utilise DurationTicks de la source pour remplir OrderIndex dans la cible.
    """
    print("\n[FUSION PLAYLISTITEMMEDIA MAP]")
    conn = sqlite3.connect(merged_db_path)
    cursor = conn.cursor()

    for db_path in [file1_db, file2_db]:
        with sqlite3.connect(db_path) as src_conn:
            src_cursor = src_conn.cursor()
            src_cursor.execute("""
                SELECT PlaylistItemId, IndependentMediaId, DurationTicks
                FROM PlaylistItemIndependentMediaMap
            """)
            rows = src_cursor.fetchall()
            print(f"{len(rows)} lignes trouvées dans {os.path.basename(db_path)}")

            for old_item_id, old_media_id, duration_ticks in rows:
                new_item_id = item_id_map.get((os.path.normpath(db_path), old_item_id))
                new_media_id = independent_media_map.get((db_path, old_media_id))
                order_idx = duration_ticks  # on réutilise DurationTicks comme OrderIndex

                if new_item_id and new_media_id:
                    try:
                        cursor.execute("""
                            INSERT OR IGNORE INTO PlaylistItemMediaMap
                            (PlaylistItemId, MediaFileId, OrderIndex)
                            VALUES (?, ?, ?)
                        """, (new_item_id, new_media_id, order_idx))
                    except sqlite3.IntegrityError as e:
                        print(f"Erreur PlaylistItemMediaMap: {e}")
                else:
                    print(
                        f"⚠️ Mapping manquant pour PlaylistItemId={old_item_id}, "
                        f"IndependentMediaId={old_media_id} (db: {db_path})"
                    )

    conn.commit()
    conn.close()


def merge_playlist_item_marker(merged_db_path, file1_db, file2_db, item_id_map):
    """
    Fusionne la table PlaylistItemMarker de façon idempotente.
    Utilise une table de mapping MergeMapping_PlaylistItemMarker pour éviter de réinsérer
    des markers déjà fusionnés. Retourne marker_id_map.
    """
    print("\n[FUSION PLAYLISTITEMMARKER]")
    conn = sqlite3.connect(merged_db_path)
    cursor = conn.cursor()

    # Créer la table de mapping pour PlaylistItemMarker si elle n'existe pas
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS MergeMapping_PlaylistItemMarker (
            SourceDb TEXT,
            OldMarkerId INTEGER,
            NewMarkerId INTEGER,
            PRIMARY KEY (SourceDb, OldMarkerId)
        )
    """)
    conn.commit()

    cursor.execute("SELECT COALESCE(MAX(PlaylistItemMarkerId), 0) FROM PlaylistItemMarker")
    max_marker_id = cursor.fetchone()[0] or 0
    print(f"ID max initial: {max_marker_id}")
    marker_id_map = {}

    for db_path in [file1_db, file2_db]:
        with sqlite3.connect(db_path) as src_conn:
            src_cursor = src_conn.cursor()
            src_cursor.execute("""
                SELECT PlaylistItemMarkerId, PlaylistItemId, Label, StartTimeTicks, DurationTicks, EndTransitionDurationTicks
                FROM PlaylistItemMarker
            """)
            markers = src_cursor.fetchall()
            print(f"{len(markers)} markers trouvés dans {os.path.basename(db_path)}")
            for old_marker_id, old_item_id, label, start_time, duration, end_transition in markers:
                new_item_id = item_id_map.get((os.path.normpath(db_path), old_item_id))
                if not new_item_id:
                    print(f"    > ID item introuvable pour marker {old_marker_id} — ignoré")
                    continue
                # Vérifier dans le mapping persistant
                cursor.execute("""
                    SELECT NewMarkerId FROM MergeMapping_PlaylistItemMarker
                    WHERE SourceDb = ? AND OldMarkerId = ?
                """, (db_path, old_marker_id))
                res = cursor.fetchone()
                if res:
                    marker_id_map[(db_path, old_marker_id)] = res[0]
                    continue
                max_marker_id += 1
                new_row = (max_marker_id, new_item_id, label, start_time, duration, end_transition)
                try:
                    cursor.execute("""
                        INSERT INTO PlaylistItemMarker
                        VALUES (?, ?, ?, ?, ?, ?)
                    """, new_row)
                    marker_id_map[(db_path, old_marker_id)] = max_marker_id
                    cursor.execute("""
                        INSERT INTO MergeMapping_PlaylistItemMarker (SourceDb, OldMarkerId, NewMarkerId)
                        VALUES (?, ?, ?)
                    """, (db_path, old_marker_id, max_marker_id))
                    conn.commit()
                except sqlite3.IntegrityError as e:
                    print(f"Erreur insertion PlaylistItemMarker pour OldMarkerId {old_marker_id}: {e}")
    print(f"ID max final: {max_marker_id}")
    print(f"Total markers mappés: {len(marker_id_map)}")
    conn.close()
    return marker_id_map


def merge_marker_maps(merged_db_path, file1_db, file2_db, marker_id_map):
    """
    Fusionne les tables de mapping liées aux markers, y compris PlaylistItemMarkerMap
    et les MarkerMaps spécifiques (BibleVerse et Paragraph).
    """
    print("\n[FUSION MARKER MAPS]")
    conn = sqlite3.connect(merged_db_path)
    cursor = conn.cursor()

    # 5.2. Fusion des MarkerMaps spécifiques
    for map_type in ['BibleVerse', 'Paragraph']:
        table_name = f'PlaylistItemMarker{map_type}Map'
        cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'")
        if not cursor.fetchone():
            print(f"Table {table_name} non trouvée - ignorée")
            continue
        print(f"\nFusion de {table_name}")
        for db_path in [file1_db, file2_db]:
            with sqlite3.connect(db_path) as src_conn:
                src_cursor = src_conn.cursor()
                src_cursor.execute(f"SELECT * FROM {table_name}")
                rows = src_cursor.fetchall()
                print(f"{len(rows)} entrées trouvées dans {os.path.basename(db_path)} pour {table_name}")
                for row in rows:
                    old_marker_id = row[0]
                    new_marker_id = marker_id_map.get((db_path, old_marker_id))
                    if not new_marker_id:
                        continue
                    new_row = (new_marker_id,) + row[1:]
                    placeholders = ",".join(["?"] * len(new_row))
                    try:
                        cursor.execute(f"INSERT OR IGNORE INTO {table_name} VALUES ({placeholders})", new_row)
                    except sqlite3.IntegrityError as e:
                        print(f"Erreur dans {table_name}: {e}")
    conn.commit()
    conn.close()


def merge_playlist_final(merged_db_path, file1_db, file2_db):
    """
    Fusionne la table Playlist finale de façon idempotente.
    Gère les conflits sur le nom en ajoutant un suffixe pour garantir l'unicité.
    Utilise la table de mapping MergeMapping_Playlist pour éviter la réinsertion.
    Retourne un mapping { (SourceDb, OldPlaylistId) : NewPlaylistId }.
    """
    print("\n[FUSION PLAYLIST FINAL - IDÉMPOTENTE]")
    conn = sqlite3.connect(merged_db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='Playlist'")
    if not cursor.fetchone():
        print("🚫 Table 'Playlist' absente — étape ignorée.")
        conn.close()
        return {}

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS MergeMapping_Playlist (
            SourceDb TEXT,
            OldPlaylistId INTEGER,
            NewPlaylistId INTEGER,
            PRIMARY KEY (SourceDb, OldPlaylistId)
        )
    """)
    conn.commit()
    cursor.execute("SELECT COALESCE(MAX(PlaylistId), 0) FROM Playlist")
    max_playlist_id = cursor.fetchone()[0] or 0
    playlist_id_map = {}

    for db_path in [file1_db, file2_db]:
        with sqlite3.connect(db_path) as source_conn:
            source_cursor = source_conn.cursor()
            source_cursor.execute("""
                SELECT PlaylistId, Name, Description, IconId, OrderIndex, LastModified
                FROM Playlist
            """)
            playlists = source_cursor.fetchall()
            print(f"{len(playlists)} playlists trouvées dans {os.path.basename(db_path)}")
            for pl_id, name, desc, icon, order_idx, modified in playlists:
                # Vérifier si cet enregistrement est déjà fusionné
                cursor.execute("""
                    SELECT NewPlaylistId FROM MergeMapping_Playlist
                    WHERE SourceDb = ? AND OldPlaylistId = ?
                """, (db_path, pl_id))
                res = cursor.fetchone()
                if res:
                    new_id = res[0]
                    playlist_id_map[(db_path, pl_id)] = new_id
                    print(f"Playlist déjà fusionnée : OldID {pl_id} -> NewID {new_id}")
                    continue
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
                        INSERT INTO Playlist
                        (PlaylistId, Name, Description, IconId, OrderIndex, LastModified)
                        VALUES (?, ?, ?, ?, ?, ?)
                    """, (max_playlist_id, name, desc, icon, order_idx, modified))
                    playlist_id_map[(db_path, pl_id)] = max_playlist_id
                    print(f"Insertion Playlist : OldID {pl_id} -> NewID {max_playlist_id} avec nom '{name}'")
                except sqlite3.IntegrityError as e:
                    print(f"ERREUR Playlist {pl_id} de {db_path}: {str(e)}")
                    continue
                cursor.execute("""
                    INSERT INTO MergeMapping_Playlist (SourceDb, OldPlaylistId, NewPlaylistId)
                    VALUES (?, ?, ?)
                """, (db_path, pl_id, max_playlist_id))
                conn.commit()
    conn.close()
    return playlist_id_map


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

        item_id_map = merge_playlist_items(
            merged_db_path, file1_db, file2_db, location_id_map, independent_media_map
        )

        # Appel immédiat à merge_playlist_items pour avoir item_id_map dispo dès le début
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
                    new_item_id = item_id_map.get((os.path.normpath(db_path), old_item_id))
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
                            print(
                                f"⚠️ Mapping manquant : ItemId={old_item_id}, LocationId={old_loc_id} (dans {os.path.basename(db_path)})")

        # 3. Fusion PlaylistItemMediaMap
        print("\n[FUSION PlaylistItemMediaMap]")
        for db_path in [file1_db, file2_db]:
            with sqlite3.connect(db_path) as src_conn:
                src_cur = src_conn.cursor()
                src_cur.execute("""
                    SELECT PlaylistItemId, MediaFileId, OrderIndex
                    FROM PlaylistItemMediaMap
                """)
                rows = src_cur.fetchall()
                print(f"{len(rows)} lignes trouvées dans {os.path.basename(db_path)}")

                for old_item_id, old_media_id, order_idx in rows:
                    print(
                        f"Mapping demandé pour ({db_path}, {old_item_id}) → {item_id_map.get((db_path, old_item_id))}")
                    new_item_id = item_id_map.get((os.path.normpath(db_path), old_item_id))
                    new_media_id = independent_media_map.get((db_path, old_media_id))

                    if new_item_id and new_media_id:
                        try:
                            cursor.execute("""
                                INSERT OR IGNORE INTO PlaylistItemMediaMap
                                (PlaylistItemId, MediaFileId, OrderIndex)
                                VALUES (?, ?, ?)
                            """, (new_item_id, new_media_id, order_idx))
                        except sqlite3.IntegrityError as e:
                            print(f"Erreur PlaylistItemMediaMap: {e}")
                    else:
                        print(
                            f"⚠️ Mapping manquant pour PlaylistItemId={old_item_id}, MediaFileId={old_media_id} (db: {db_path})")

        # 4. Fusion PlaylistItemMarker
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
                    new_item_id = item_id_map.get((os.path.normpath(db_path), old_item_id))

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

        # 5. Fusion des PlaylistItemMarkerMap et Marker*Map (BibleVerse/Paragraph)
        print("\n[FUSION MARKER MAPS]")

        # 5.1. Fusion de PlaylistItemMarkerMap (principale)
        print("\nFusion PlaylistItemMarkerMap")
        for db_path in [file1_db, file2_db]:
            with sqlite3.connect(db_path) as src_conn:
                src_cur = src_conn.cursor()
                src_cur.execute("""
                    SELECT PlaylistItemId, PlaylistItemMarkerId, OrderInList
                    FROM PlaylistItemMarkerMap
                """)
                mappings = src_cur.fetchall()
                print(f"{len(mappings)} mappings trouvés dans {os.path.basename(db_path)}")
                for old_item_id, old_marker_id, order in mappings:
                    new_item_id = item_id_map.get((os.path.normpath(db_path), old_item_id))
                    new_marker_id = marker_id_map.get((db_path, old_marker_id))
                    if new_item_id and new_marker_id:
                        try:
                            cursor.execute("""
                                INSERT OR IGNORE INTO PlaylistItemMarkerMap
                                (PlaylistItemId, PlaylistItemMarkerId, OrderInList)
                                VALUES (?, ?, ?)
                            """, (new_item_id, new_marker_id, order))
                        except sqlite3.IntegrityError as e:
                            print(f"Erreur PlaylistItemMarkerMap: {e}")
                    else:
                        print(f"⚠️ Mapping manquant item={old_item_id}, marker={old_marker_id} (db: {db_path})")

        # 5.2. Fusion des PlaylistItemMarkerBibleVerseMap et ParagraphMap
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
                        new_marker_id = marker_id_map.get((db_path, old_marker_id))
                        if new_marker_id:
                            new_row = (new_marker_id,) + row[1:]
                            try:
                                placeholders = ",".join(["?"] * len(new_row))
                                cursor.execute(f"INSERT OR IGNORE INTO {table_name} VALUES ({placeholders})", new_row)
                            except sqlite3.IntegrityError as e:
                                print(f"Erreur {table_name}: {e}")

        # 6. Fusion de PlaylistItemIndependentMediaMap
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
                        new_item_id = item_id_map.get((os.path.normpath(db_path), old_item_id))
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

        # 10. (Optionnel) Fusion de la table Playlist si elle existe
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='Playlist'")
        has_playlist_table = cursor.fetchone() is not None

        if not has_playlist_table:
            print("🚫 Table 'Playlist' absente — étape ignorée.")
        else:
            print("\n=== DÉBUT FUSION PLAYLIST ===")
            cursor.execute("SELECT MAX(PlaylistId) FROM Playlist")
            max_playlist_id = cursor.fetchone()[0] or 0
            print(f"ID max initial Playlist: {max_playlist_id}")
            playlist_id_map = {}

            for db_path in [file1_db, file2_db]:
                with sqlite3.connect(db_path) as source_conn:
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
                                INSERT INTO Playlist
                                (PlaylistId, Name, Description, IconId, OrderIndex, LastModified)
                                VALUES (?, ?, ?, ?, ?, ?)
                            """, (max_playlist_id, name, desc, icon, order_idx, modified))
                            playlist_id_map[(db_path, pl_id)] = max_playlist_id
                        except sqlite3.IntegrityError as e:
                            print(f"ERREUR Playlist {pl_id}: {str(e)}")

            print(f"Playlist fusionnée - ID max final: {max_playlist_id}")
            print(f"Total playlists fusionnées: {len(playlist_id_map)}")

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
        print("\n🎯 Résumé final:")
        print(f"- Fichier fusionné: {merged_jwlibrary}")
        print(f"- Playlists max ID: {max_playlist_id}")
        print(f"- PlaylistItem total: {len(item_id_map)}")
        print(f"- Médias max ID: {max_media_id}")
        print(f"- Orphelins supprimés: {orphaned_deleted}")
        print(f"- Résultat intégrité: {integrity_result}")

        print(">>> Fin de merge_playlists, on retourne les valeurs")

        print("✅ Tous les calculs terminés, retour imminent")

        return merged_jwlibrary, max_playlist_id, len(item_id_map), max_media_id, orphaned_deleted, integrity_result, item_id_map

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
    # ─── 0. Initialisation des variables utilisées plus bas ─────────────────────────────
    merged_jwlibrary = None
    max_playlist_id = 0
    max_media_id = 0
    orphaned_deleted = 0
    integrity_result = "ok"
    item_id_map = {}
    marker_id_map = {}
    playlist_id_map = {}

    conn = None  # pour le finally

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

        # juste après create_merged_schema(merged_db_path, base_db_path)
        print("\n→ Debug: listing des tables juste après create_merged_schema")
        with sqlite3.connect(merged_db_path) as dbg_conn:
            dbg_cur = dbg_conn.cursor()
            dbg_cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = [t[0] for t in dbg_cur.fetchall()]
            print("Tables présentes dans merged_userData.db :", tables)

        # ... Après la création de merged_db_path et avant la fusion des autres mappings
        # Fusion de la table IndependentMedia et PlaylistItems, etc.
        location_id_map = merge_location_from_sources(merged_db_path, *required_dbs)
        print("Location ID Map:", location_id_map)

        independent_media_map = merge_independent_media(merged_db_path, file1_db, file2_db)
        print("Mapping IndependentMedia:", independent_media_map)

        # ❌ NE PAS appeler merge_playlist_items ici
        # item_id_map = merge_playlist_items(...)

        usermark_guid_map = merge_usermark_from_sources(merged_db_path, file1_db, file2_db, location_id_map)

        # === INSÉRER LES USERMARKS AVANT DE FUSIONNER LES BLOCKRANGE ===
        conn = sqlite3.connect(merged_db_path)
        cursor = conn.cursor()

        for guid, (color, loc, style, version) in merged_highlights_dict.items():
            try:
                cursor.execute("""
                    INSERT OR IGNORE INTO UserMark (ColorIndex, LocationId, StyleIndex, UserMarkGuid, Version)
                    VALUES (?, ?, ?, ?, ?)
                """, (color, loc, style, guid, version))
            except Exception as e:
                print(f"Erreur lors de l'insertion de UserMarkGuid={guid}: {e}")

        conn.commit()
        conn.close()

        # === INSÉRER LES NOTES et USERMARK DANS LA DB FUSIONNÉE AVANT de créer note_mapping ===
        conn = sqlite3.connect(merged_db_path)
        cursor = conn.cursor()
        # --- Insertion des NOTES dans merge_data (dans la boucle sur merged_notes_list)
        for note_tuple in merged_notes_list:
            old_db_path, guid, title, content, old_loc_id, usermark_guid, last_modified, created, block_type, block_identifier = note_tuple

            new_guid = guid if guid else str(uuid.uuid4())

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

            # On va maintenant vérifier si une note avec ce GUID existe déjà dans la DB fusionnée
            cursor.execute("SELECT Title, Content FROM Note WHERE Guid = ?", (new_guid,))
            existing = cursor.fetchone()

            if existing:
                if existing[0] == title and existing[1] == content:
                    # La note existe déjà et est identique, on n'insère rien
                    print(f"Note avec GUID {new_guid} déjà présente et identique, insertion ignorée.")
                else:
                    # Conflit : le GUID existe, mais le contenu est différent.
                    # On génère un nouveau GUID pour préserver l'information
                    new_guid_conflict = str(uuid.uuid4())
                    print(
                        f"Conflit pour GUID {new_guid}: contenu différent. Insertion de la note avec nouveau GUID {new_guid_conflict}.")
                    new_guid = new_guid_conflict  # On met à jour pour cette insertion
                    cursor.execute("""
                        INSERT INTO Note (Guid, Title, Content, LocationId, UserMarkId, LastModified, Created, BlockType, BlockIdentifier)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        new_guid,
                        title,
                        content,
                        new_location_id,
                        new_usermark_id,
                        last_modified,
                        created,
                        block_type,
                        block_identifier
                    ))
            else:
                # Aucune note existante, on insère directement
                cursor.execute("""
                    INSERT INTO Note (Guid, Title, Content, LocationId, UserMarkId, LastModified, Created, BlockType, BlockIdentifier)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    new_guid,
                    title,
                    content,
                    new_location_id,
                    new_usermark_id,
                    last_modified,
                    created,
                    block_type,
                    block_identifier
                ))

        # Gestion spécifique de LastModified
        cursor.execute("DELETE FROM LastModified")
        cursor.execute("INSERT INTO LastModified (LastModified) VALUES (?)", (merge_date,))
        conn.commit()
        conn.close()

        # Après create_note_mapping…
        note_mapping = create_note_mapping(merged_db_path, file1_db, file2_db)
        print("Note Mapping:", note_mapping)

        # (Ré)ouvrir la connexion pour PlaylistItem
        conn = sqlite3.connect(merged_db_path)
        cursor = conn.cursor()

        # … juste après avoir construit note_mapping …

        print("🔹 Avant merge_playlist_items :",
              conn.execute("SELECT COUNT(*) FROM PlaylistItem").fetchone()[0])

        # ─── 3. Fusion de la table PlaylistItem ───────────────────────────────────────
        item_id_map = merge_playlist_items(
            merged_db_path,
            file1_db,
            file2_db
        )

        # Affichage du contenu de PlaylistItem avec la bonne colonne
        for row in conn.execute("SELECT PlaylistItemId, Label FROM PlaylistItem ORDER BY PlaylistItemId"):
            print(row)

        print("🔹 Après merge_playlist_items :",
              conn.execute("SELECT COUNT(*) FROM PlaylistItem").fetchone()[0])

        print(f"--> PlaylistItem fusionnés : {len(item_id_map)} items")

        conn.close()

        print("\n=== USERMARK VERIFICATION ===")
        print(f"Total UserMarks mappés (GUIDs) : {len(usermark_guid_map)}")
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
        print(f"UserMark GUIDs mappés: {usermark_guid_map}")

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

        # --- FUSION BOOKMARKS ---
        merge_bookmarks(merged_db_path, file1_db, file2_db, location_id_map)

        # --- FUSION BLOCKRANGE ---
        print("\n=== DEBUT FUSION BLOCKRANGE ===")
        if not merge_blockrange_from_two_sources(merged_db_path, file1_db, file2_db):
            print("ÉCHEC Fusion BlockRange")
            return jsonify({"error": "BlockRange merge failed"}), 500

        # Mapping inverse UserMarkId original → nouveau
        usermark_guid_map = {}
        conn = sqlite3.connect(merged_db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT UserMarkId, UserMarkGuid FROM UserMark")
        for new_id, guid in cursor.fetchall():
            usermark_guid_map[guid] = new_id
        conn.close()

        # --- FUSION NOTES ---
        merge_notes(merged_db_path, file1_db, file2_db, location_id_map, usermark_guid_map)

        # --- Étape 1 : fusion des Tags et TagMap (utilise location_id_map) ---
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

        # --- Vérification Tag ---
        print("\n=== TAGS VERIFICATION ===")
        try:
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
        except Exception as e:
            print(f"❌ ERREUR dans la vérification des tags : {e}")
            import traceback
            traceback.print_exc()
            return jsonify({"error": "Erreur lors de la vérification des tags"}), 500

        print("\n▶️ Début de la fusion des éléments liés aux playlists...")

        # Fusion de PlaylistItemAccuracy
        max_acc_id = merge_playlist_item_accuracy(merged_db_path, file1_db, file2_db)
        print(f"--> PlaylistItemAccuracy fusionnée, max ID final: {max_acc_id}")

        # Fusion de PlaylistItemLocationMap
        merge_playlist_item_location_map(merged_db_path, file1_db, file2_db, item_id_map, location_id_map)
        print("--> PlaylistItemLocationMap fusionnée.")

        # Fusion de PlaylistItemMediaMap
        merge_playlist_item_media_map(merged_db_path, file1_db, file2_db, item_id_map, independent_media_map)
        print("--> PlaylistItemMediaMap fusionnée.")

        # Fusion de PlaylistItemMarker et récupération du mapping des markers
        marker_id_map = merge_playlist_item_marker(merged_db_path, file1_db, file2_db, item_id_map)
        print(f"--> PlaylistItemMarker fusionnée, markers mappés: {len(marker_id_map)}")

        # Fusion des MarkerMaps (BibleVerse, Paragraph, etc.)
        merge_marker_maps(merged_db_path, file1_db, file2_db, marker_id_map)
        print("--> MarkerMaps fusionnées.")

        # Fusion finale de la table Playlist
        playlist_id_map = merge_playlist_final(merged_db_path, file1_db, file2_db)
        print(f"--> Table Playlist fusionnée, playlists mappées: {len(playlist_id_map)}")

        print("\n▶️ Fusion des éléments liés aux playlists terminée.")

        # ─── Avant merge_other_tables ────────────────────────────────────────────
        tables_to_check = [
            'PlaylistItem',
            'IndependentMedia',
            'PlaylistItemLocationMap',
            'PlaylistItemIndependentMediaMap'
        ]
        print("\n--- COMPTES AVANT merge_other_tables ---")
        with sqlite3.connect(merged_db_path) as dbg_conn:
            dbg_cur = dbg_conn.cursor()
            for tbl in tables_to_check:
                # compte dans la base fusionnée
                dbg_cur.execute(f"SELECT COUNT(*) FROM {tbl}")
                cnt_merged = dbg_cur.fetchone()[0]
                # compte dans file1
                dbg_cur.execute(f"ATTACH DATABASE ? AS src1", (file1_db,))
                dbg_cur.execute(f"SELECT COUNT(*) FROM src1.{tbl}")
                cnt1 = dbg_cur.fetchone()[0]
                dbg_cur.execute("DETACH DATABASE src1")
                # compte dans file2
                dbg_cur.execute(f"ATTACH DATABASE ? AS src2", (file2_db,))
                dbg_cur.execute(f"SELECT COUNT(*) FROM src2.{tbl}")
                cnt2 = dbg_cur.fetchone()[0]
                dbg_cur.execute("DETACH DATABASE src2")
                print(f"[AVANT ] {tbl}: merged={cnt_merged}, file1={cnt1}, file2={cnt2}")

        # ─── Appel original ────────────────────────────────────────────────────
        merge_other_tables(
            merged_db_path,
            file1_db,
            file2_db,
            exclude_tables=[
                'Note', 'UserMark', 'Location', 'BlockRange',
                'LastModified', 'Tag', 'TagMap', 'PlaylistItem',
                'InputField', 'Bookmark'
            ]
        )

        # ─── Après merge_other_tables ───────────────────────────────────────────
        print("\n--- COMPTES APRÈS merge_other_tables ---")
        with sqlite3.connect(merged_db_path) as dbg_conn:
            dbg_cur = dbg_conn.cursor()
            for tbl in tables_to_check:
                dbg_cur.execute(f"SELECT COUNT(*) FROM {tbl}")
                cnt_merged = dbg_cur.fetchone()[0]
                dbg_cur.execute(f"ATTACH DATABASE ? AS src1", (file1_db,))
                dbg_cur.execute(f"SELECT COUNT(*) FROM src1.{tbl}")
                cnt1 = dbg_cur.fetchone()[0]
                dbg_cur.execute("DETACH DATABASE src1")
                dbg_cur.execute(f"ATTACH DATABASE ? AS src2", (file2_db,))
                dbg_cur.execute(f"SELECT COUNT(*) FROM src2.{tbl}")
                cnt2 = dbg_cur.fetchone()[0]
                dbg_cur.execute("DETACH DATABASE src2")
                print(f"[APRÈS] {tbl}: merged={cnt_merged}, file1={cnt1}, file2={cnt2}")

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

        # 10. (Optionnel) Fusion de la table Playlist si elle existe
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='Playlist'")
        has_playlist_table = cursor.fetchone() is not None

        if not has_playlist_table:
            print("🚫 Table 'Playlist' absente — étape ignorée.")
        else:
            print("\n=== DÉBUT FUSION PLAYLIST ===")
            cursor.execute("SELECT MAX(PlaylistId) FROM Playlist")
            max_playlist_id = cursor.fetchone()[0] or 0
            print(f"ID max initial Playlist: {max_playlist_id}")
            playlist_id_map = {}

            for db_path in [file1_db, file2_db]:
                with sqlite3.connect(db_path) as source_conn:
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
                                        INSERT INTO Playlist
                                        (PlaylistId, Name, Description, IconId, OrderIndex, LastModified)
                                        VALUES (?, ?, ?, ?, ?, ?)
                                    """, (max_playlist_id, name, desc, icon, order_idx, modified))
                            playlist_id_map[(db_path, pl_id)] = max_playlist_id
                        except sqlite3.IntegrityError as e:
                            print(f"ERREUR Playlist {pl_id}: {str(e)}")

            print(f"Playlist fusionnée - ID max final: {max_playlist_id}")
            print(f"Total playlists fusionnées: {len(playlist_id_map)}")

        # 11. Vérification de cohérence
        print("\n=== VERIFICATION COHERENCE ===")
        cursor.execute("""
            SELECT COUNT(*) 
              FROM PlaylistItem pi
             WHERE pi.PlaylistItemId NOT IN (
                    SELECT PlaylistItemId FROM PlaylistItemLocationMap
                    UNION
                    SELECT PlaylistItemId FROM PlaylistItemIndependentMediaMap
                )
        """)
        orphaned_items = cursor.fetchone()[0]
        status_color = "\033[91m" if orphaned_items > 0 else "\033[92m"
        print(f"{status_color}Éléments sans parent détectés (non supprimés) : {orphaned_items}\033[0m")

        # 12. Suppression des PlaylistItem orphelins
        with sqlite3.connect(merged_db_path) as conn_del:
            cur = conn_del.cursor()
            cur.execute("""
                DELETE FROM PlaylistItem
                 WHERE PlaylistItemId NOT IN (
                    SELECT PlaylistItemId FROM PlaylistItemLocationMap
                    UNION
                    SELECT PlaylistItemId FROM PlaylistItemIndependentMediaMap
                 )
            """)
            conn_del.commit()
        print("→ PlaylistItem orphelins supprimés")

        # 13. Optimisations finales
        print("\n=== DEBUT OPTIMISATIONS ===")

        # Définition de log_message **avant** son premier appel
        log_file = os.path.join(UPLOAD_FOLDER, "fusion.log")

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

        # --- 14. Finalisation ---
        # commit final et fermeture propre de la transaction playlists
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

        # 16. Activation du WAL
        cursor.execute("PRAGMA journal_mode=WAL")
        cursor.execute("CREATE TABLE IF NOT EXISTS dummy_for_wal (id INTEGER PRIMARY KEY)")
        cursor.execute("INSERT INTO dummy_for_wal DEFAULT VALUES")
        conn.commit()
        cursor.execute("DELETE FROM dummy_for_wal")
        conn.commit()
        cursor.execute("DROP TABLE dummy_for_wal")
        conn.commit()
        conn.close()

        # Vérification du mode WAL
        with sqlite3.connect(merged_db_path) as test_conn:
            new_wal_status = test_conn.execute("PRAGMA journal_mode").fetchone()[0]
            print(f"Statut WAL après activation: {new_wal_status}")
            if new_wal_status != "wal":
                print("Avertissement: Échec de l'activation WAL")

        # Retour final de merge_data
        print("\n🎯 Résumé final:")
        print(f"- Fichier fusionné: {merged_jwlibrary}")
        print(f"- Playlists max ID: {max_playlist_id}")
        print(f"- PlaylistItem total: {len(item_id_map)}")
        print(f"- Médias max ID: {max_media_id}")
        print(f"- Orphelins supprimés: {orphaned_deleted}")
        print(f"- Résultat intégrité: {integrity_result}")
        print("✅ Tous les calculs terminés, retour imminent")

        # ─── fin de la fusion, avant tout copy/zip ────────────────────────

        # 1️⃣ Supprimer les tables temporaires MergeMapping_* de la DB fusionnée
        with sqlite3.connect(merged_db_path) as conn:
            cur = conn.cursor()
            cur.execute("""
                SELECT name
                  FROM sqlite_master
                 WHERE type='table'
                   AND name LIKE 'MergeMapping_%'
            """)
            mapping_tables = [row[0] for row in cur.fetchall()]
            print(f"🔎 Tables MergeMapping_* trouvées : {mapping_tables}")  # Debug

            if not mapping_tables:
                print("⚠ Aucune table MergeMapping_* trouvée")
            else:
                for tbl in mapping_tables:
                    print(f"🧹 Suppression de la table {tbl}")
                    try:
                        cur.execute(f"DROP TABLE {tbl}")  # On enlève IF EXISTS pour voir si ça échoue
                        print(f"  -> Table {tbl} supprimée avec succès")
                    except sqlite3.Error as e:
                        print(f"  ❌ Erreur lors de la suppression de {tbl}: {e}")

                conn.commit()
                print("✔ Commit effectué pour les suppressions")

        print("✔ Toutes les MergeMapping_* ont été traitées")

        # 2️⃣ Copier cette DB clean dans le folder qu’on va zipper
        merged_folder = os.path.join(UPLOAD_FOLDER, "merged_folder")
        # recréer merged_folder à partir du template
        if os.path.exists(merged_folder):
            shutil.rmtree(merged_folder)
        shutil.copytree(
            os.path.join(EXTRACT_FOLDER, "file1_extracted"),
            merged_folder
        )

        # remplacer userData.db par notre DB nettoyée
        dest_db = os.path.join(merged_folder, "userData.db")
        if os.path.exists(dest_db):
            os.remove(dest_db)
        shutil.copy(merged_db_path, dest_db)

        # créer les fichiers WAL et SHM vides
        open(os.path.join(merged_folder, "userData.db-wal"), 'wb').close()
        open(os.path.join(merged_folder, "userData.db-shm"), 'wb').close()

        # 3️⃣ Générer l’archive .jwlibrary
        merged_zip = os.path.join(UPLOAD_FOLDER, "merged.zip")
        if os.path.exists(merged_zip):
            os.remove(merged_zip)

        shutil.make_archive(
            base_name=merged_zip.replace(".zip", ""),
            format="zip",
            root_dir=merged_folder
        )

        merged_jwlibrary = merged_zip.replace(".zip", ".jwlibrary")
        if os.path.exists(merged_jwlibrary):
            os.remove(merged_jwlibrary)
        os.rename(merged_zip, merged_jwlibrary)

        print(f"✅ Archive finale créée : {merged_jwlibrary}")

        # ─── Retour **à l’intérieur** du try ───────────────────────────────────────────────
        final_result = {
            "merged_file": "merged.jwlibrary",
            "playlists": max_playlist_id,
            "playlist_items": len(item_id_map),
            "media_files": max_media_id,
            "cleaned_items": orphaned_deleted,
            "integrity_check": integrity_result
        }
        return jsonify(final_result), 200

    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500

    finally:
        # ─── cleanup / mapping résiduel ────────────────────────────────────────────────
        try:
            if conn:
                conn.close()
        except:
            pass

        print("\n=== MISE À JOUR DES LocationId RÉSIDUELS ===")
        merge_inputfields(merged_db_path, file1_db, file2_db, location_id_map)
        print("✔ Fusion InputFields terminée")

        # Aplatir puis appliquer le mapping dans toutes les autres tables
        location_replacements_flat = {
            old_id: new_id
            for (_, old_id), new_id in sorted(location_id_map.items())
        }
        update_location_references(merged_db_path, location_replacements_flat)
        print("✔ Mise à jour des références LocationId terminée")

        # --- Étape 4 : vérification post-fusion ---
        print("\n=== VERIFICATION POST-FUSION ===")
        with sqlite3.connect(merged_db_path) as conn:
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM PlaylistItem")
            count = cur.fetchone()[0]
            print(f"Nombre d'enregistrements dans PlaylistItem après fusion : {count}")

        print("\n=== FUSION TERMINÉE AVEC SUCCÈS ===")
        print(f"Fichier fusionné : {merged_jwlibrary}")
        print(f"Playlists fusionnées : {max_playlist_id}")
        print(f"Items fusionnés : {len(item_id_map)}")
        print(f"Médias traités : {max_media_id}")
        print(f"Éléments nettoyés : {orphaned_deleted}")
        print(f"Intégrité : {'✅ OK' if integrity_result == 'ok' else '⚠️ ÉCHEC'}")
        print("✅ Tous les résultats sont prêts, retour JSON imminent")


@app.route('/download', methods=['GET'])
def download_file():
    merged_jwlibrary = os.path.join(UPLOAD_FOLDER, "merged.jwlibrary")
    if not os.path.exists(merged_jwlibrary):
        return jsonify({"error": "Fichier fusionné non trouvé."}), 404
    print("📥 Envoi du fichier :", merged_jwlibrary)
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

