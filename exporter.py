import requests
import os
from requests.auth import HTTPBasicAuth
import vcr
from vcr.record_mode import RecordMode
import csv


NAME = os.environ.get('NAME')
XRELEASE_ID = os.environ.get('XRELEASE_ID')
COL_API = os.environ.get('COL_API')
if COL_API is None or COL_API == '':
    COL_API = 'https://api.checklistbank.org'
COL_USER = os.environ.get('COL_USER')
COL_PASS = os.environ.get('COL_PASS')
TAXON_ID = os.environ.get('TAXON_ID')


def login():
    print('\nAuthenticating')
    login_url = COL_API + '/user/login'
    r = requests.get(login_url, auth=HTTPBasicAuth(COL_USER, COL_PASS))
    r.raise_for_status()
    bearer = r.text
    headers = {'Authorization': 'Bearer ' + bearer, 'Accept':  '*/*'}
    return headers


def crawl(headers):
    print('\nCrawling merged name usages')
    search_url = COL_API + '/dataset/' + XRELEASE_ID + '/nameusage/search'
    params = {
        'TAXON_ID': TAXON_ID,
        'facet': ['rank', 'issue', 'status', 'nomStatus', 'nomCode', 'nameType', 'field', 'authorship', 'authorshipYear', 
                  'extinct', 'environment', 'origin', 'sectorMode', 'secondarySourceGroup', 'sectorDatasetKey', 'group'],
        'limit': 1000,
        'offset': 0,
        'sectorMode': 'merge',
        'sortBy': 'taxonomic'
    }
    total = float('inf')
    results = []
    i = 0
    while params['offset'] < total:
        with vcr.use_cassette(f'raw/vcr_cassettes/{NAME}_{XRELEASE_ID}_{TAXON_ID}/nameusages/{params["offset"]}.yaml', 
                              filter_headers=['authorization'], record_mode=RecordMode.NEW_EPISODES):
            r = requests.get(search_url, headers=headers, params=params)
            r.raise_for_status()
            if 'result' in r.json():
                results += r.json()['result']
            params['offset'] += params['limit']
            total = r.json()['total']
        if i % params['limit'] == 0:
            print(f"{i} of {total}")
        i += params['limit']
    if len(results) != total:
        exit('Error: only ' + str(len(results)) + ' of ' + str(total) + ' results were returned')
    return results


def format_classification(classification):
    output = {
        'kingdom': '',
        'phylum': '',
        'subphylum': '',
        'class': '',
        'order': '',
        'suborder': '',
        'infraorder': '',
        'parvorder': '',
        'superfamily': '',
        'family': '',
        'subfamily': '',
        'tribe': '',
        'subtribe': '',
        'genus': '',
        'subgenus': '',
        'species': '',
        'subspecies': '',
        'infraspecies': '',
        'form': '',
        'variety': '',
        'aberration': '',
        'other': ''
    }
    for row in classification:
        output[row['rank']] = row['name']
    
    # unranked causes problems because barcodes use unranked as rank and Biota uses rank unranked
    output.pop('unranked', None)
    return output


def get_datasets(results, headers):
    dataset_ids = []
    for row in results:
        dataset_id = row['sectorDatasetKey']
        if dataset_id not in dataset_ids:
            dataset_ids.append(dataset_id)
    dataset_ids = list(set(dataset_ids))
    print('\nCrawling dataset metadata')
    datasets = {}
    i = 0
    for dataset_id in dataset_ids:
        if i % 10 == 0:
            print(f"{i} of {len(dataset_ids)}")
        with vcr.use_cassette(f'raw/vcr_cassettes/{NAME}_{XRELEASE_ID}_{TAXON_ID}/datasets/{dataset_id}.yaml', 
                              filter_headers=['authorization'], record_mode=RecordMode.NEW_EPISODES):
            dataset_url = COL_API + '/dataset/' + str(dataset_id)
            r = requests.get(dataset_url, headers=headers)
            r.raise_for_status()
            datasets[dataset_id] = r.json()
        i += 1
    return datasets


def format_people(people):
    output = []
    for person in people:
        output.append(person['name'])
    return '; '.join(output)


def write_datasets(datasets):
    print('\nWriting dataset output')
    tsv_file = f'output/{NAME}_{XRELEASE_ID}_{TAXON_ID}_datasets.tsv'
    headers = [
        'dataset_id',
        'alias',
        'title',
        'issued',
        'version',
        'description',
        'contact',
        'creator',
        'editor',
        'publisher',
        'contributor',
        'doi',
        'license',
        'geographic_scope',
        'temporal_scope',
        'taxonomic_scope',
        'confidence',
        'completeness',
        'logo',
        'created',
        'modified',
        'type',
        'origin'
    ]
    with open(tsv_file, 'w', newline='') as f:
        writer = csv.writer(f, delimiter='\t', quotechar='"', quoting=csv.QUOTE_MINIMAL, escapechar='\\')
        writer.writerow(headers)
        for dataset_id, dataset in datasets.items():
            output = {}
            output['dataset_id'] = dataset_id
            output['alias'] = dataset.get('alias', '')
            output['title'] = dataset.get('title', '')
            output['issued'] = dataset.get('issued', '')
            output['version'] = dataset.get('version', '')
            output['description'] = dataset.get('description', '')
            contact = dataset.get('contact', '')
            output['contact'] = contact.get('name', '')
            output['creator'] = format_people(dataset.get('creator', []))
            output['editor'] = format_people(dataset.get('editor', []))
            publisher = dataset.get('publisher', {'name': ''})
            output['publisher'] = publisher.get('name', '')
            output['contributor'] = format_people(dataset.get('contributor', []))
            output['doi'] = dataset.get('doi', '')
            output['license'] = dataset.get('license', '')
            output['geographic_scope'] = dataset.get('geographicScope', '')
            output['temporal_scope'] = dataset.get('temporalScope', '')
            output['taxonomic_scope'] = dataset.get('taxonomicScope', '')
            output['confidence'] = dataset.get('confidence', '')
            output['completeness'] = dataset.get('completeness', '')
            output['logo'] = dataset.get('logo', '')
            output['created'] = dataset.get('created', '')
            output['modified'] = dataset.get('modified', '')
            output['type'] = dataset.get('type', '')
            output['origin'] = dataset.get('origin', '')
            writer.writerow(output.values())


def write_tsv(results, datasets):
    print('\nWriting name usage output')
    tsv_file = f'output/{NAME}_{XRELEASE_ID}_{TAXON_ID}.tsv'
    reference_ids = []
    headers = [
        'kingdom',
        'phylum',
        'subphylum',
        'class',
        'order',
        'suborder',
        'infraorder',
        'parvorder',
        'superfamily',
        'family',
        'subfamily',
        'tribe',
        'subtribe',
        'genus',
        'subgenus',
        'species',
        'subspecies',
        'infraspecies',
        'form',
        'variety',
        'aberration',
        'other',
        'dataset_id',
        'dataset_alias',
        'parent_id',
        'taxon_id',
        'name_id',
        'rank',
        'accepted_id',
        'accepted_name',
        'accepted_author',
        'scientific_name',
        'authorship',
        'authors',
        'year',
        'status',
        'extinct',
        'temporal_range_start',
        'temporal_range_end',
        'link',
        'reference_id',
        'identifiers'
    ]
    with open(tsv_file, 'w', newline='') as f:
        writer = csv.writer(f, delimiter='\t', quotechar='"', quoting=csv.QUOTE_MINIMAL, escapechar='\\')
        writer.writerow(headers)
        for row in results:

            sector_dataset_id = ''
            if 'sectorDatasetKey' in row:
                sector_dataset_id = row['sectorDatasetKey']
                if 'alias' in datasets[sector_dataset_id]:
                    sector_dataset = datasets[sector_dataset_id]['alias']
                else:
                    sector_dataset = datasets[sector_dataset_id]['title']

            status = row['usage']['status']
            accepted_id = ''
            accepted_name = ''
            accepted_author = ''
            if status == 'synonym':
                accepted_id = row['usage']['accepted']['id']
                accepted_name = row['usage']['accepted']['name']['scientificName']
                if '†' in row['usage']['accepted']['label']:
                    accepted_name = '†' + accepted_name
                accepted_author = row['usage']['accepted']['name'].get('authorship', '')

            output = format_classification(row['classification'])
            output['sector_dataset_id'] = sector_dataset_id
            output['sector_dataset_alias'] = sector_dataset
            output['parent_id'] = row['usage']['parentId']
            output['taxon_id'] = row['usage']['id']
            output['name_id'] = row['usage']['name']['id']
            output['rank'] = row['usage']['name']['rank']
            output['accepted_id'] = accepted_id
            output['accepted_name'] = accepted_name
            output['accepted_author'] = accepted_author
            output['scientific_name'] = row['usage']['name']['scientificName']
            output['authorship'] = row['usage']['name'].get('authorship', '')
            combinationAuthorship = row['usage']['name'].get('combinationAuthorship', {})
            output['authors'] = '|'.join(combinationAuthorship.get('authors', ''))
            output['year'] = combinationAuthorship.get('year', '')
            output['status'] = status
            output['extinct'] = row['usage'].get('extinct', '')
            output['temporal_range_start'] = row['usage'].get('temporalRangeStart', '')
            output['temporal_range_end'] = row['usage'].get('temporalRangeEnd', '')
            output['link'] = row['usage'].get('link', '')
            referenceIds = row['usage'].get('referenceIds', [])
            output['reference_id'] = '|'.join(referenceIds)
            output['identifiers'] = '|'.join(row['usage']['name'].get('identifier', ''))
            reference_ids.extend([ref_id for ref_id in referenceIds if ref_id not in reference_ids])
            writer.writerow(output.values())
    return reference_ids


def crawl_references(references_ids, headers):
    print('\nCrawling references')
    i = 0
    results = []
    for ref_id in references_ids:
        if i % 10 == 0:
            print(f"{i} of {len(references_ids)}")
        with vcr.use_cassette(f'raw/vcr_cassettes/{NAME}_{XRELEASE_ID}_{TAXON_ID}/references/{ref_id}.yaml', 
                              filter_headers=['authorization'], record_mode=RecordMode.NEW_EPISODES):
            ref_url = COL_API + '/dataset/' + XRELEASE_ID +'/reference/' + ref_id
            r = requests.get(ref_url, headers=headers)
            r.raise_for_status()
            results.append(r.json())
        i += 1
    return results


def write_references(results):
    print('\nWriting references output')
    tsv_file = f'output/{NAME}_{XRELEASE_ID}_{TAXON_ID}_references.tsv'
    headers = ['reference_id', 'source_id', 'type', 'authors', 'year', 
               'title', 'citation', 'journal', 'volume', 'page']
    with open(tsv_file, 'w', newline='') as f:
        writer = csv.writer(f, delimiter='\t', quotechar='"', quoting=csv.QUOTE_MINIMAL, escapechar='\\')
        writer.writerow(headers)
        for row in results:
            output = {}
            output['reference_id'] = row['id']
            csl = row.get('csl', {})
            output['source_id'] = csl.get('id', '')
            output['type'] = csl.get('type', '')
            output['authors'] = csl.get('author', '')
            output['year'] = row.get('year', '')
            output['title'] = csl.get('title', '')
            output['citation'] = row.get('citation', '')
            output['journal'] = csl.get('container-title', '')
            output['volume'] = csl.get('volume', '')
            output['page'] = csl.get('page', '')
            writer.writerow(output.values())


def import_tsv_to_sqlite():
    print('\nImporting TSV files to SQLite')
    os.system(f'sqlite3 output/{NAME}_{XRELEASE_ID}_{TAXON_ID}.sqlite < schema.sql')
    os.system(f'sqlite3 output/{NAME}_{XRELEASE_ID}_{TAXON_ID}.sqlite ".mode tabs" ".import --skip 1 output/{NAME}_{XRELEASE_ID}_{TAXON_ID}.tsv nameusage"')
    os.system(f'sqlite3 output/{NAME}_{XRELEASE_ID}_{TAXON_ID}.sqlite ".mode tabs" ".import --skip 1 output/{NAME}_{XRELEASE_ID}_{TAXON_ID}_references.tsv reference"')
    os.system(f'sqlite3 output/{NAME}_{XRELEASE_ID}_{TAXON_ID}.sqlite ".mode tabs" ".import --skip 1 output/{NAME}_{XRELEASE_ID}_{TAXON_ID}_datasets.tsv dataset"')

def main():
    headers = login()
    results = crawl(headers=headers)
    datasets = get_datasets(results=results, headers=headers)
    write_datasets(datasets=datasets)
    reference_ids = write_tsv(results=results, datasets=datasets)
    references = crawl_references(references_ids=reference_ids, headers=headers)
    write_references(references)
    import_tsv_to_sqlite()


if __name__ == '__main__':
    main()
