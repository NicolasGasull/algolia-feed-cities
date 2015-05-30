require 'rubygems'
require 'bundler/setup'
require 'yaml'
require 'net/http'
require 'zip'
require 'algoliasearch'

ZIP_DIRECTORY = "zip"
GEONAMES_DOMAIN = "download.geonames.org"
GEONAMES_PATH = "export/dump"
HEADERS_FILE = "column_headers.txt"
COUNTRIES_FILE = "countries.txt"

AL_INDEX = "Cities"
AL_APP_ID = "977AC8JAJ4"
AL_API_KEY = "3fc0b1f5c61a608f18e25c706186960c"
AL_BATCH_SIZE = 1000

# Returns a list of countries from the countries file (one country code per line)
def list_country_codes
  File.foreach(COUNTRIES_FILE) do |country|
    yield country.gsub(/\r?\n$/, "")
  end
end

# Returns a headers list from the headers file (one header name per line)
# Params:
# +headers_file+:: string containing the path to the headers file
def define_headers(headers_file)
  return IO.readlines(headers_file).map {|col| col.gsub(/\r?\n$/, "")}
end

# Downloads the contents of a country zip and saves it in the zip folder.
# Params:
# +country+:: the country code
def download_country_data(country)

  target_zip = "#{ZIP_DIRECTORY}/#{country}.zip"

  if !File.exist?(target_zip)

    puts "Downloading #{GEONAMES_DOMAIN}/#{GEONAMES_PATH}/#{country} ..."

    # Download the zip only when necessary
    Net::HTTP.start(GEONAMES_DOMAIN) do |http|

      resp = http.get("/#{GEONAMES_PATH}/#{country}.zip")

      open(target_zip, "wb") do |file|
          file.write(resp.body)
          puts "Wrote #{target_zip}"
      end
    end
  end
end

# Non blocking read of a country zip. Yields line by line the contents
# of the country data. Assumes the zip exists in zip/<country>.zip
# Params:
# +country+:: the country code
def read_country_zip(country)

  Zip::File.open("#{ZIP_DIRECTORY}/#{country}.zip") do |zip_file|
    # Read the right txt file from the zip file
    raw_data_file = zip_file.glob("#{country}.txt").first
    puts "Reading #{raw_data_file}"

    # Read in a non blocking way
    readable = raw_data_file.get_input_stream.select

    readable.each do |line|
      yield line
    end
  end
end

# Returns a hash of data parsed from a line considering headers provided
# Params:
# +headers+:: the headers list (made with define_headers)
# +line+:: the line to parse from headers model
def parse_line(headers, line)

  columns = line.split("\t")
  data = {}

  headers.each_with_index do |col, index|
    data[col] = columns[index].force_encoding "utf-8" if !col.start_with?('#')
  end

  return data
end

# Calls the chain that downloads country zips
# and parse their the data they contain.
def load_data_from_geonames

  headers = define_headers(HEADERS_FILE)

  list_country_codes do |country|

    # Do the wget equivalent (download the zip)
    download_country_data(country)

    read_country_zip(country) do |line|
      yield parse_line(headers, line)
    end
  end
end


## Main ##

Dir.mkdir(ZIP_DIRECTORY) if !Dir.exist?(ZIP_DIRECTORY)

# Load algolia conf which is located at the same place as the current script
algoliaConf = YAML.load_file("#{File.dirname(__FILE__)}/algolia.yml")

# Connect to the Algolia account
Algolia.init :application_id => algoliaConf["appId"], :api_key => algoliaConf["apiKey"]

index = Algolia::Index.new(algoliaConf["indexName"])

batch = Array.new

# Add everything to the new index
load_data_from_geonames do |data|

  batch << data

  if batch.length >= algoliaConf["batchSize"]
    index.add_objects(batch)
    batch = Array.new
  end
end
