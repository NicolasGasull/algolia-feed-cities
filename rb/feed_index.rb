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
CITY_FEATURE = "P"

# Pipes countries from the countries file (one country code per line)
def list_country_codes
  headers = nil
  File.foreach(COUNTRIES_FILE) do |line|
    countryData = line.gsub(/\r?\n$/, "")

    if headers.nil?
      headers = countryData.split("\t").map {|header| parse_header(header)}
    elsif countryData.length > 0
      yield parse_line(headers, countryData)
    end
  end
end

# Returns a list of headers from the headers file (one header name per line)
# Params:
# +headers_file+:: string containing the path to the headers file
def define_headers(headers_file)
  return IO.readlines(headers_file)
    .map {|col| parse_header(col.gsub(/\r?\n$/, ""))}
end

# Downloads the contents of a country zip and saves it in the zip folder.
# Params:
# +country_code+:: the country code
def download_country_data(country_code)

  target_zip = "#{ZIP_DIRECTORY}/#{country_code}.zip"

  if !File.exist?(target_zip)
    # Download the zip only when necessary
    puts "Downloading #{GEONAMES_DOMAIN}/#{GEONAMES_PATH}/#{country_code} ..."

    Net::HTTP.start(GEONAMES_DOMAIN) do |http|

      res = http.get("/#{GEONAMES_PATH}/#{country_code}.zip")

      if res.code == 200
        open(target_zip, "wb") do |file|
            file.write(res.body)
            puts "Wrote #{target_zip}"
        end
      else
        puts "WARN: Could not fetch #{target_zip}, got code #{res.code}: #{res.message}"
      end
    end
  end
end

# Non blocking read of a country zip. Yields line by line the contents
# of the country data. Assumes the zip exists in zip/<country>.zip
# Params:
# +country_code+:: the country code
def read_country_zip(country_code)

  target_zip = "#{ZIP_DIRECTORY}/#{country_code}.zip"

  if File.exist?(target_zip)

    Zip::File.open("#{ZIP_DIRECTORY}/#{country_code}.zip") do |zip_file|
      # Read the right txt file from the zip file
      raw_data_file = zip_file.glob("#{country_code}.txt").first
      puts "Reading #{raw_data_file}"

      # Read in a non blocking way
      readable = raw_data_file.get_input_stream.select

      readable.each do |line|
        yield line
      end
    end
  end
end

# Returns object defining the name and the type of a header
# Params:
# +header_string+:: the string definition of the header
def parse_header(header_string)
  headerSplit = header_string.split('@')
  return {
    :name => headerSplit[0],
    :type => headerSplit.length > 0 ? headerSplit[1] : 'string',
    :active => !headerSplit[0].start_with?('#')
  }
end

# Returns a hash of data parsed from a line considering headers provided
# Params:
# +headers+:: the headers list (made with define_headers)
# +line+:: the line to parse from headers model
def parse_line(headers, line)

  columns = line.split("\t")
  data = {}

  headers.each_with_index do |col, index|

    if col[:active] and index < columns.length
      value = columns[index].force_encoding "utf-8"

      if col[:type] == "int"
        value = Integer(value)
      elsif col[:type] == "float"
        value = Float(value)
      end

      data[col[:name]] = value
    end
  end

  return data
end

# Calls the chain that downloads country zips
# and parse their the data they contain.
def load_data_from_geonames

  headers = define_headers(HEADERS_FILE)

  list_country_codes do |country|

    # Do the wget equivalent (download the zip)
    download_country_data(country["ISO"])

    read_country_zip(country["ISO"]) do |line|
      place_data = parse_line(headers, line)

      if place_data["featureClass"] == CITY_FEATURE

        place_data["country"] = {
          "code" => country["ISO"],
          "name" => country["Country"],
          "area" => country["Area(in sq km)"],
          "population" => country["Population"],
          "capital" => country["Capital"],
          "continent" => country["Continent"],
        }

        yield place_data
      end
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
