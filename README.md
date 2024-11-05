# xcol feedback exporter

This tool enables exporting name usages that were added to Catalogue of Life in the extended xcol releases in order to assist with gathering feedback from global species database experts.

To assist with debugging, all interactions with the API server are recorded using [VCR.py](https://vcrpy.readthedocs.io/en/latest/) and the cassettes are stored in the raw folder. Keeping the VCR cassettes could be particularly useful in case an issue is found after a draft xcol release has been deleted. The authorization bearer tokens are filtered out of the cassette files, so it should be safe to share the VCR cassettes.

# Running

1) Install [docker](https://www.docker.com/). If using Linux, you may still also have to separately install [docker-compose](https://docs.docker.com/compose/install/), which is now bundled with the Windows and Mac installers.

2) Run the exporter with the following command, replacing the environment variables with your values:

```
NAME=your_export_name TAXON_ID=taxon_id XRELEASE_ID=dataset_id COL_USER=your_user COL_PASS=your_password docker compose up
```

Optionally, you can also set the COL_API environment variable to use the dev server.

3) The export will be in the output directory and includes both the added name usages and any cited references in tab-separated value files and also imported into a SQLite database because it makes for [easier exchange of biodiversity data](https://biss.pensoft.net/article/138931/). File names include the export name, taxon_id, and xrelease_id.
