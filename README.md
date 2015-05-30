# algolia-feed-cities

*Feeds an Algolia application with the cities of the world.*


## Instructions

1. Check that Ruby is installed
2. Setup your Algolia configuration in `rb/algolia.yml`
3. Run the following to import cities from `geonames.org`

        bundle install --path vendor/bundle
        ruby rb/feed_index.rb


## Additional info

* `column_headers.txt` configures the format of the data provided by `geonames.org`. A hash (`#`) may be prepended for a column name in order not to upload its associated data to your Algolia application.
* `countries.txt` contains the list of country codes to process. Feel free to reduce it in order not to upload the whole world!
