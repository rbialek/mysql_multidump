require 'optparse'
require 'parallel'

class Multidump < Struct.new(:db, :opts)

  attr_accessor :bg # run in background
  # @return -h<hostname> -u<username> -p <databasename>
  def login_params
    ret = []
    ret << "-h" + host unless blank?(host)
    ret << "-u" + username unless blank?(user)
    ret << "-p" + password unless blank?(pass)
    ret.join(" ")
  end

  # return mysqldump code that can be executed in shell
  # use Runner.run_in_parallel to execute them side by side
  def generate_all_steps
    tables_with_ids = last_ids
    exec_lines      = tables_with_ids.collect {|table, limit|
      if limit == :no_limit
        dump_table(table)
      else
        key = limit.keys.first
        max = limit[key]
        dump_table(table, limit: "#{key} <= #{max}")
      end
    }
    exec_lines
  end

  protected

  # @return table names
  def table_names
    ret = Runner.run mysql "SELECT table_name FROM information_schema.tables where table_schema='#{db}';"
    extract_list(ret)
  end

  # @return hash with table names and their primary keys
  # {
  #   "table1" => ["id"],
  #   "table2" => ["name","order"]
  # }
  def table_pks
    tables = table_names
    sql    =%q"SELECT t.table_name, k.COLUMN_NAME
FROM information_schema.table_constraints t
LEFT JOIN information_schema.key_column_usage k
USING(constraint_name,table_schema,table_name)
WHERE t.constraint_type='PRIMARY KEY'
    AND t.table_schema=DATABASE();"
    ret    = Runner.run(mysql sql)

    extract_hash(ret)
  end

  # fetch the latest ID from the tables
  # {
  #   "table1" => {"id" => 123},
  # DELETE  "table2" => ["name","order"]
  # }
  def last_ids
    tables = table_pks
    ret    = {}
    tables.each {|table, ids|
      if ids.size == 1 # only one id
        pk         = ids.first
        max        = Runner.run mysql("SELECT MAX(#{pk}) FROM #{table}")
        ret[table] = {pk => max.split("\n").last}
      else
        ret[table] = :no_limit
      end
    }
    ret
  end

  # fetch the latest ID from the tables
  # {
  #   "table1" => {"id" => 123},
  #   "table2" => :no_limit # when primary key consists of multiple tables ["name","order"]
  # }
  def last_ids_fast
    tables  = table_pks
    ret     = {}
    selects = []
    froms   = []
    n       = 0
    tables.each {|table, ids|
      if ids.size == 1 # only one id
        pk = ids.first
        selects << "COALESCE(MAX(#{table}.#{pk}),0) AS #{table}_max"
        froms << table
      else
        ret[table] = :no_limit
      end
      n += 1
      if n>20
        puts "-" * 90
        list = extract_maxs Runner.run(mysql "\tSELECT #{selects.join(',\n')} FROM #{froms.join(',\n')}")
        puts list.inspect
        n       = 0
        selects = []
        froms   = []
      end
    }
    ret
  end

  # dump table with a limit
  # @param opts
  #   limit: "id < 12234"
  # @param table "--no-data" to dump schema only
  def dump_table(table, opts = {})
    limit = opts[:limit]
    ret   = [table]
    ret << "--where '#{limit}'" unless blank?(limit)

    file = "#{db}/#{table}.sql"
    file += "." + compressor(false) if compress? # table.sql.bzip2

    "rm -f #{file}; " +
        mysqldump(*ret) + compressor + " > #{file}"
  end

  # execute mysql command
  def mysql(sql)
    #str = Array.new(args)
    (["mysql", login_params, db, "-e"]+["\"#{sql}\""]).join(" ")
  end


  # mysqldump -h<hostname> -u<username> -p <databasename>
  # <table4> --where 'created > DATE_SUB(now(), INTERVAL 7 DAY)',
  # <table5> --where 'created > DATE_SUB(now(), INTERVAL 7 DAY)
  # --single-transaction --no-create-info >> dumpfile.sql
  def mysqldump(*args)
    str = Array.new(args)
    (["mysqldump", login_params, db]+[str]).join(" ")
  end

  private
  def blank?(str)
    str.nil? || str.empty?
  end

  def extract_list(str, separator = "\n")
    str.split(separator)[1..-1]
  end

  def extract_maxs(str)
    ret           = {}
    names, values = str.split("\n")
    names         = names.split("\t")
    values        = values.split("\t")
    while (true)
      n = names.pop
      break unless n
      ret[n] = values.pop
    end
    ret
  end

  def extract_hash(str)
    ret = {}
    extract_list(str).each {|line|
      k, v   = line.split("\t")
      ret[k] ||= []
      ret[k] << v
    }
    ret
  end

  # option accessors
  def host
    opts[:host]
  end

  def user
    opts[:user]
  end

  def pass
    opts[:pass]
  end

  def compress?
    !compressor.empty?
  end

  def compressor(with_pipe = true)
    if c = opts[:compressor]
      with_pipe ? " | " + c : c
    else
      ""
    end
  end
end


class Runner
  class << self
    def run_in_parallel(lines, no = 4)
      puts "Running shell commands in parallel of #{no}."
      Parallel.map(lines, in_processes: no.to_i) {|line|
        puts "(#{Parallel.worker_number+1}) Running: #{line}"
        run line
      }
    end

# execute system command
    def run(str)
      `#{str}`
    end
  end
end


def multidump(db, opts)
  opts[:parallelism] ||= 4
  puts "multidump #{db}: #{opts.inspect}"

  Runner.run "mkdir -p #{db}"
  db = Multidump.new(db, opts)

  Runner.run_in_parallel(db.generate_all_steps, opts[:parallelism])
rescue => e
  puts "Error: #{e.message}"
  puts e.backtrace.join("\n")
  puts "-" * 80
end

USAGE = "Usage:
multidump.rb DB [-hHOST -uUSER -pPASS] \t# generates DB/tables.sql
multidump.rb -H / --help               \t# to show help
"
opts  = {compressor: 'bzip2', parallelism: 4}
dbs   = OptionParser.new do |parser|
  parser.banner = USAGE

  parser.on('-h', '--host host', 'Source host') {|v| opts[:host] = v}
  parser.on('-u', '--user name', 'Database user name') {|v| opts[:user] = v}
  parser.on('-p', '--pass pass', 'Database user password') {|v| opts[:pass] = v}
  # opts.on('-o', '--output filename (defaults to DB.sql)') {|v| opts[:file] = v}
  parser.on('-n', 'Number of parallel processes (default 4)') {|v| opts[:parallelism] = v}
  parser.on('-c', '--compressor compressor (gzip/bzip2 defualt bzip2)') {|v| opts[:compressor] = v}

  # No argument, shows at tail.  This will print an options summary.
  # Try it and see!
  parser.on_tail("-H", "--help", "Show this message") do
    puts parser
    exit
  end
  # Another typical switch to print the version.
  parser.on_tail("-v", "--version", "Show version") do
    puts "1.0"
    exit
  end
end.parse!

if dbs.size == 1
  multidump(dbs.first, opts)
else
  puts USAGE
  exit 1
end
