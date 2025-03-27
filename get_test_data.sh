wget https://dbs.uni-leipzig.de/files/datasets/saeedi/musicbrainz-20000-A01.csv.zip;
unzip musicbrainz-20000-A01.csv.zip; rm musicbrainz-20000-A01.csv.zip;
mv musicbrainz-20000-A01.csv tests/mb.csv;
