require 'sequel'
require 'logger'

class PgDataSource  

  QUERIES = {
    best_buy_metric: 'SELECT volume, limit1 FROM best_buy_metric(?);',
    best_sell_metric: 'SELECT volume, limit1 FROM best_sell_metric(?);'
  }

  # connection_hash = {host: ..., database: ..., user: ..., port: ..., password: ..., loggers: ...}
  # Example: 
  #   host: 'localhost', database: 'postgres'
  #
  def initialize(connection_hash)
    # try with postgres, if it fails then with jdbc-postgres
    begin
      @db = Sequel.postgres(connection_hash)
    rescue Sequel::AdapterNotFound
      connection_hash.merge! host: ('postgresql://' + connection_hash[:host])
      @db = Sequel.jdbc connection_hash
    end
    @logger = connection_hash[:logger] || Logger.new(STDOUT)
  end

  # connection_hash = {host: ..., database: ..., user: ..., port: ..., password: ..., loggers: ...}
  def self.connect(connection_hash)
    new(connection_hash)
  end


  # Returns {best_buy_metric: get_best_buy_metric(...), best_sell_metric: get_best_sell_metric(...)}
  def get_best_order_metric(stock_id)
      # HOW TO BATCH QUERIES IN SEQUEL ??
      # rows = @db.fetch_multiple([query1, args], [query2, args], ...)
      # rows[0] # could be enoty if query1 returned null
    { best_buy_metric: get_best_buy_metric(stock_id), 
      best_sell_metric: get_best_sell_metric(stock_id) }
  end

  # Returns {volume: .., price: ...} on valid result or {} on either error or empty result.
  def get_best_buy_metric(stock_id)
    get_best_order_metric_for(:best_buy_metric, stock_id)
  end

  # Returns {volume: .., price: ...} on valid result or {} on either error or empty result.
  def get_best_sell_metric(stock_id)
    get_best_order_metric_for(:best_sell_metric, stock_id)
  end


  private

  def get_best_order_metric_for(type, stock_id)
    begin
      result = @db.fetch(QUERIES[type], stock_id).all
      if result.empty?
        {}
      else
        row = result.first
        { volume: row[:volume], price: row[:limit1] }
      end 
    rescue Exception => error
      @logger.error "get_best_#{type}: #{error}"
      {}
    end
  end    
end