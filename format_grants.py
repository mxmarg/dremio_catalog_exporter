import json

def format_grants(dremio_catalog_entries: dict[list]):
    
    grants = []
    grant_sqls = ""

    for e in dremio_catalog_entries:
        if len(e.get("grants")) == 0:
            continue

        object_path = e.get("object_path", "")
        scope = '"' + '"."'.join(object_path) + '"'
        object_type = e.get("object_type", "").upper()

        for g in e.get("grants"):
            grantee_type = g.get("granteeType", "")
            grantee_id = g.get("name", "")

            for p in g.get("privileges", ""):
                privilege = p.replace("_", " ")
                grant_sql = f'GRANT {privilege} ON {object_type} {scope} TO {grantee_type} "{grantee_id}"'
                print(grant_sql)
                grant_sqls += grant_sql + ";\n"
                grants.append({
                    "privilege": privilege,
                    "object_type": object_type,
                    "scope": object_path,
                    "grantee_type": grantee_type,
                    "grantee_id": grantee_id
                })
    
    return grants, grant_sqls


if __name__ == '__main__':

    json_filename = 'dremio_catalog_entries.json'
    with open(json_filename, 'r') as f:
        dremio_catalog_entries = json.load(f)

    grants, grant_sqls = format_grants(dremio_catalog_entries)

    grant_sqls_filename = 'grants.sql'
    with open(grant_sqls_filename, 'w') as f:
        f.write(grant_sqls)

    grant_filename = 'grants.json'
    with open(grant_filename, 'w') as f:
        json.dump(grants, f)