# frozen_string_literal: true

require "bundler"
Bundler.require

require "nokogiri"
require "net/http"
require "json"
require "csv"
require "date"
require "time"
require "digest"
require "fileutils"

REPOSITORY = "http://ww11.doh.state.fl.us/comm/_partners/covid19_report_archive/"
FILENAMES = /^([a_-zA-Z1-9]+)_/
TIMESTAMP = /$(\w+)(\d{4})(\d{2})(\d{2})\.*/
EXTRACTOR = /([a_-zA-Z1-9]+)_(\d{4})(\d{2})(\d{2})(_\d{2,4}\w*)?/

puts "Checking #{REPOSITORY} for new files... "

response = Net::HTTP.get_response(URI(REPOSITORY))
doc = Nokogiri::HTML(response.body)
links = doc.css("tr td:nth-child(2) a").grep(/\w+_\d+.\w+/)
urls = links.map { _1["href"] }
puts "Found #{urls.count} documents..."
timestamps = doc.css("tr td:nth-child(3)").select { _1.content.match?(/\d+/) }.map do
  DateTime.strptime(_1.text.rstrip, "%d-%b-%Y %H:%M")
end

puts "Latest document modified at #{timestamps.sort.last.strftime("%Y-%m-%d %H:%M")}"

grouped_urls = urls.group_by do
  _1.match(FILENAMES).captures.first
rescue => error
  puts "Unexpected filename #{_1}"
end

def create_directory(directory)
  if !Dir.exist?(directory)
    puts "Creating #{directory} since it doesn't exist yet..."
    begin
      FileUtils.mkdir_p(directory)
    end
  end
end

def download_file(directory, file)
  filename, extension = file.split(".")
  timestamp = filename.match(TIMESTAMP)&.captures&.last
  title, year, month, day, time = filename.match(EXTRACTOR)&.captures

  new_filename = "#{title}_#{year}-#{month}-#{day}#{time&.gsub("_", "-")}.#{extension}"
  path = "#{directory}/#{new_filename}"

  if !File.exist?(path)
    puts "Downloading #{directory}/#{new_filename}..."
    File.open(path, "wb") do |local_file|
      response = Net::HTTP.get_response(URI("#{REPOSITORY}#{file}"))
      local_file.write(response.read_body)
    end
  end
end

def transform_directory(directory)
  case directory
    when "apd_report" then "agency_for_persons_with_disabilities"
    when "poc_antibody" then "point_of_care_antibody"
    when "ltcf" then "long_term_care_facilities"
    when "ltcf_deaths" then "long_term_care_facilities/deaths"
    when "county_reports" then "counties"
    when "fdc_death" then "florida_department_of_corrections/deaths"
    when "state_linelist" then "state/line_list"
    when "state_reports" then "state"
    when "pediatric_report" then "pediatric"
    when "serology_county" then "serology/counties"
  else
    directory
  end
end

directory_queue = Queue.new

grouped_urls.each do |directory, files|
  directory_queue.push([transform_directory(directory), files])
end

workers = (0..4).map do
  Thread.new do
    begin
      while all = directory_queue.pop(true)
        directory = all[0]
        files = all[1]

        create_directory(directory)

        files_queue = Queue.new
        files.each { files_queue.push(_1) }

        processors = (0..4).map do
          Thread.new do
            begin
              while file = files_queue.pop(true)
                download_file(directory, file)
              end
            rescue ThreadError
            end
          end
        end; "ok"

        processors.map(&:join); "ok"
      end
    rescue ThreadError
    end
  end
end; "ok"

workers.map(&:join); "ok"
