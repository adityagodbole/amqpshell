require 'zip/zip'
require 'zip/zipfilesystem'
require 'tmpdir'
require 'find'
require 'pathname'
require 'fileutils'
require 'log4r'

Unique = 1
Exchange = "amqpshell"

def create_fs(dir)
  Dir.mktmpdir do |tmp|
    zipfilename = File.expand_path("zip.zip",tmp)
    zip_dir(dir,zipfilename)
    $stderr.puts "Temp zip file: #{zipfilename}"
    fs = File.open(zipfilename) do |zf|
      zf.read
    end
  end
end

def make_empty_temp_dir(uniquetag)
  tempdirname = File.expand_path(uniquetag,Dir.tmpdir)
  begin
    FileUtils.rm_r(tempdirname,:secure => true)
  rescue
    # There probably will not be a previous directory
  end
  Dir.mkdir(tempdirname,0777) unless File.exist? tempdirname
  tempdirname
end


def zip_dir (path, zipfilename)
  #Sanitize path on windows if required
  path = path.gsub('\\','/')
  src_base_dir = path + '/'
  Zip::ZipFile.open(zipfilename, Zip::ZipFile::CREATE) do |zipfile|
    begin
      Find.find(path) do |fpath|
        if ("." != fpath and path != fpath)
          entry_path = fpath.gsub(src_base_dir, '')
          if File.file?(fpath)
            zipfile.add(entry_path, fpath)
          end
        end
      end
    rescue Exception => e
      $stderr.puts e.message
      $stderr.puts "No data found at #{path}."
      # There might be no logs. Create empty zip instead
    end
  end
  # read the zipfile as a blob
end

def unzip_file(source, target)
  # Create the target directory.
  # We'll ignore the error scenario where
  begin
    Dir.mkdir(target) unless File.exists? target
  end

  Zip::ZipFile.open(source) do |zip_file|
    zip_file.each do |f|
      f_path=File.join(target, f.name)
      dir = File.dirname(f_path)
      FileUtils.mkdir_p(dir) unless File.exist?(dir)
      zip_file.extract(f, f_path) unless File.exist?(f_path)
    end
  end
rescue Zip::ZipDestinationFileExistsError => ex
  # I'm going to ignore this and just overwrite the files.
rescue => ex
  puts "Unzip error: #{ex}"
end


def make_rand_str(n)
  (0...n).map{65.+(rand(25)).chr}.join
end

$Logger = Log4r::Logger.new('amqpshell.log')

module AmqpShell
  def self.setup_logger
    $Logger.outputters << Log4r::Outputter.stderr
  end
end
