#!/usr/bin/env python3
import os
import json
import glob
from datetime import date

import requests

ZENODO_ENV = os.getenv("ZENODO_ENV", "production")
ZENODO_TOKEN = os.getenv("ZENODO_TOKEN")

if ZENODO_ENV == "sandbox":
    ZENODO_BASE_URL = "https://sandbox.zenodo.org"
else:
    ZENODO_BASE_URL = "https://zenodo.org"

API = f"{ZENODO_BASE_URL}/api"
HEADERS = {"Authorization": f"Bearer {ZENODO_TOKEN}"}


def load_json(path, default=None):
    """
    Charge un JSON si le fichier existe et est valide.
    Si le fichier est absent, vide ou invalide, retourne default (ou {}).
    """
    if not os.path.exists(path):
        return default if default is not None else {}

    # fichier existant mais potentiellement vide
    if os.stat(path).st_size == 0:
        return default if default is not None else {}

    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except json.JSONDecodeError:
        # fallback safe
        return default if default is not None else {}


def save_json(path, data):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def build_metadata(base_meta, file_meta, pdf_path):
    meta = dict(base_meta)

    # titre
    if "title" in file_meta:
        meta["title"] = file_meta["title"]
    else:
        base_title = base_meta.get("title", "")
        meta["title"] = f"{base_title}: {os.path.basename(pdf_path)}"

    # description
    if "description" in file_meta:
        meta["description"] = file_meta["description"]
    elif "description" not in meta:
        meta["description"] = ""

    # keywords
    if "keywords" in file_meta:
        meta["keywords"] = file_meta["keywords"]

    # champs obligatoires pour Zenodo (si absents)
    meta.setdefault("upload_type", "publication")
    meta.setdefault("publication_type", "report")
    meta.setdefault("license", "CC-BY-4.0")

    # date de publication
    meta["publication_date"] = str(date.today())

    return meta


def find_latest_record_for_concept(conceptdoi):
    # récupère le dernier record publié pour ce conceptdoi
    params = {
        "q": f'conceptdoi:"{conceptdoi}"',
        "sort": "version",
        "order": "desc",
        "size": 1,
    }
    r = requests.get(f"{API}/records", headers=HEADERS, params=params)
    r.raise_for_status()
    hits = r.json().get("hits", {}).get("hits", [])
    if not hits:
        return None
    return hits[0]


def create_new_deposition(metadata):
    r = requests.post(
        f"{API}/deposit/depositions",
        headers={**HEADERS, "Content-Type": "application/json"},
        json={"metadata": metadata},
    )
    r.raise_for_status()
    return r.json()


def new_version_deposition(conceptdoi, metadata):
    latest = find_latest_record_for_concept(conceptdoi)
    if latest is None:
        # concept introuvable: on repart sur un nouveau dépôt
        return create_new_deposition(metadata)

    recid = latest["id"]

    # créer une nouvelle version (draft)
    r = requests.post(
        f"{API}/deposit/depositions/{recid}/actions/newversion",
        headers=HEADERS,
    )
    r.raise_for_status()
    dep = r.json()

    # récupérer le draft le plus récent
    draft_url = dep["links"]["latest_draft"]
    r = requests.get(draft_url, headers=HEADERS)
    r.raise_for_status()
    draft = r.json()
    dep_id = draft["id"]

    # mettre à jour les métadonnées
    r = requests.put(
        f"{API}/deposit/depositions/{dep_id}",
        headers={**HEADERS, "Content-Type": "application/json"},
        json={"metadata": metadata},
    )
    r.raise_for_status()
    return r.json()


def upload_file(deposition, pdf_path, dest_name):
    dep_id = deposition["id"]
    files_url = f"{API}/deposit/depositions/{dep_id}/files"

    # supprimer d'éventuels fichiers existants portant le même nom
    existing_files = deposition.get("files", [])
    for f in existing_files:
        if f["filename"] == dest_name:
            del_url = f"{API}/deposit/depositions/{dep_id}/files/{f['id']}"
            dr = requests.delete(del_url, headers=HEADERS)
            dr.raise_for_status()

    with open(pdf_path, "rb") as fp:
        r = requests.post(
            files_url,
            headers=HEADERS,
            data={"name": dest_name},
            files={"file": fp},
        )
    r.raise_for_status()


def publish_deposition(deposition):
    dep_id = deposition["id"]
    r = requests.post(
        f"{API}/deposit/depositions/{dep_id}/actions/publish", headers=HEADERS
    )
    r.raise_for_status()
    # récupérer le record publié pour accéder à conceptdoi
    published = r.json()
    record_url = published["links"]["record"]
    r2 = requests.get(record_url, headers=HEADERS)
    r2.raise_for_status()
    return r2.json()


def main():
    if not ZENODO_TOKEN:
        raise SystemExit("ZENODO_TOKEN manquant dans les variables d'environnement.")

    base_meta = load_json("zenodo.json")
    files_meta = load_json("zenodo.files.json", default={})
    state = load_json(".zenodo_state.json", default={})

    changed = False

    for pdf_path in sorted(glob.glob("out/*.pdf")):
        rel_path = os.path.normpath(pdf_path)
        fname = os.path.basename(pdf_path)

        file_state = state.get(rel_path, {})
        conceptdoi = file_state.get("conceptdoi")
        file_meta = files_meta.get(rel_path, {})
        metadata = build_metadata(base_meta, file_meta, rel_path)

        print(f"==> Sync {rel_path}")
        if conceptdoi:
            print(f"   Existing concept DOI: {conceptdoi} → new version")
            deposition = new_version_deposition(conceptdoi, metadata)
        else:
            print("   No concept DOI yet → new deposition")
            deposition = create_new_deposition(metadata)

        upload_file(deposition, pdf_path, fname)
        record = publish_deposition(deposition)

        new_conceptdoi = record.get("conceptdoi")
        doi = record.get("doi")
        print(f"   Published DOI: {doi}")
        print(f"   Concept DOI: {new_conceptdoi}")

        if new_conceptdoi and new_conceptdoi != conceptdoi:
            state[rel_path] = {"conceptdoi": new_conceptdoi}
            changed = True

    if changed:
        save_json(".zenodo_state.json", state)
        print("State updated in .zenodo_state.json")
    else:
        print("No state change.")


if __name__ == "__main__":
    main()
