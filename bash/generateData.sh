#!/bin/bash

# Input parameters: a list of countrys

mkdir -p zip csv

# Download zips
for country in $* ; do
	#wget -P zip/ http://download.geonames.org/export/dump/$country.zip

	# Generate data file with its header
	echo '"geonameid","name","asciiname","alternatenames","latitude","longitude","featureClass","featureCode","countryCode","cc2","admin1Code","admin2Code","admin3Code","admin4Code","population","elevation","dem","timezone","modificationDate"' > csv/$country.csv

	# zips can't be unzipped through a pipe
	# Write directly semicolon-separated data
	unzip -op zip/$country.zip $country.txt \
		| sed 's/"/\\"/g' \
		| sed -r 's/([^\t]*)/"\1"/g' \
		| sed 's/\t/,/g' \
		>> csv/$country.csv
done



