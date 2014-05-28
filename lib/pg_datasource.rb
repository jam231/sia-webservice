require 'sequel'
require 'logger'

class PgDataSource  

  QUERIES = {
    best_buy_metric: "SELECT volume AS buy_volume, limit1 AS buy_price FROM best_buy_metric('?');",
    best_sell_metric: "SELECT volume AS sell_volume, limit1 AS sell_price FROM best_sell_metric('?');"
  }

  # connection_hash = {host: ..., database: ..., user: ..., port: ..., password: ..., loggers: ...}
  # Example: 
  #   host: 'localhost', database: 'postgres'
  #
  def initialize(connection_hash)
    # try with postgres, if it fails then with jdbc-postgres
    @connection_hash = connection_hash
    begin
      @db = Sequel.postgres(@connection_hash)
    rescue Sequel::AdapterNotFound
      @connection_hash.merge! host: ('jdbc:/postgresql://' + @connection_hash[:host])
      @db = Sequel.connect @connection_hash
    end
    @logger = @connection_hash[:logger] || Logger.new(STDOUT)
  end

  # connection_hash = {host: ..., database: ..., user: ..., port: ..., password: ..., loggers: ...}
  def self.connect(connection_hash)
    self.new(connection_hash)
  end


  # Returns {best_buy_metric: get_best_buy_metric(...), best_sell_metric: get_best_sell_metric(...)}
  def get_best_order_metric(stock_id)
    best_order_metric = {best_buy_metric: {}, best_sell_metric: {} } 

    begin
      # HOW TO BATCH QUERIES IN SEQUEL ?? If I knew how I could remove silliness and simplify this code.
      # I want to do smth like that:
      # rows = @db.fetch_multiple([query1, args], [query2, args], ...)
      # rows[0] # could be enoty if query1 returned null
      query = QUERIES[best_buy_metric] + QUERIES[best_sell_metric]
      rows = @db.fetch(query, stock_id, stock_id).all
      rows.each do |row|
        if row.include?(:buy_volume) && row.include?(:buy_price) 
          best_order_metric[:best_buy_metric] = { volume: row[:buy_volume], price: row[:buy_price] }
        elsif row.include?(:sell_volume) && row.include?(:sell_price) 
          best_order_metric[:best_sell_metric] = { volume: row[:sell_volume], price: row[:sell_price] }
        else
          @logger.warn "Unrecognized row in get_best_order_metric: row=#{row}"
        end
      end
    catch
    end
    best_order_metric
  end

  # Returns {volume: .., price: ...} on valid result or {} on either error or empty result.
  def get_best_buy_metric(stock_id)
    begin
      result = @db.fetch(QUERIES[:best_buy_metric], stock_id).all
      if result.empty?
        {}
      else
        row = result.first
        { volume: row[:buy_volume], price: row[:buy_price] }
      end 
    catch
      {}
    end
  end

  # Returns {volume: .., price: ...} on valid result or {} on either error or empty result.
  def get_best_sell_metric(stock_id)
    # VERY SIMILAR TO get_best_buy_metric(stock_id) but its hard get it DRY without 
    # typing by hand best_sell/buy_metric queries in get_best_order_metric, which is even uglier 
    begin
      result = @db.fetch(QUERIES[:best_sell_metric], stock_id).all
      if result.empty?
        {}
      else
        row = result.first
        { volume: row[:sell_volume], price: row[:sell_price] }
      end 
    catch
      {}
    end
  end
end