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

REPOSITORY = "http://ww11.doh.state.fl.us/comm/_partners/covid19_report_archive/"

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

urls.group_by { _1.match(/^([a_-zA-Z]+)_/).captures.first }.each do |dir, files|
  new_dir = case dir
    when "ltcf" then "long_term_care_facilities"
    when "ltcf_deaths" then "long_term_care_facilities/deaths"
    when "county_reports" then "counties"
    when "fdc_death" then "florida_department_of_corrections/deaths"
    when "state_linelist" then "state/line_list"
    when "state_reports" then "state"
    when "pediatric_reports" then "pediatric"
  else
    dir
  end

  puts "--- #{dir} --- "
  files.reverse.each do |file|
    filename, extension = file.split(".")
    timestamp = filename.match(/$(\w+)(\d{4})(\d{2})(\d{2})\.*/)&.captures&.last
    title, year, month, day, time = filename.match(/([a_-zA-Z]+)_(\d{4})(\d{2})(\d{2})(_\d{2,4}\w*)?/)&.captures

    new_filename = "#{title}_#{year}-#{month}-#{day}#{time&.gsub("_", "-")}.#{extension}"
    path = "#{new_dir}/#{new_filename}"

    if File.exist?(path)
    else
      directory_path = "#{new_dir}"
      if !Dir.exist?(directory_path)
        puts "Creating #{directory_path} since it doesn't exist yet..."
        Dir.mkdir(directory_path)
      end

      puts "Downloading #{new_dir}/#{new_filename}..."
      File.open(path, "wb") do |local_file|
        response = Net::HTTP.get_response(URI("#{REPOSITORY}#{file}"))
        local_file.write(response.read_body)
      end
    end
  end
end
