require 'sinatra/base'
require_relative 'pg_datasource'

class SiaWebservice < Sinatra::Base
  
  def self.setup_database(config)
    configure do
      set :db, PgDataSource.connect(config)
    end

    self
  end

  def self.run!(options = {}, &block)
    begin
      if settings.db.nil?
        raise 
      end
    rescue NoMethodError 
      raise "Database handler not initialized! Call setup_database before run!"
    end

    super(options, &block)
  end

  before do
    content_type :json
  end

  # {stock_id: ..., best_buy_metric: {...}}
  get '/best_order_metric/best_buy/:stock_id' do |stock_id|
    { stock_id: stock_id, best_buy_metric: settings.db.get_best_buy_metric(stock_id) }
  end

  # {stock_id: ..., best_sell_metric: {...}}
  get '/best_order_metric/best_sell/:stock_id' do |stock_id|
    { stock_id: stock_id, best_sell_metric: settings.db.get_best_sell_metric(stock_id) }
  end

  # {stock_id: ..., best_order_metric: {...}}
  get '/best_order_metric/:stock_id' do |stock_id|
    { stock_id: stock_id, best_order_metric: settings.db.get_best_order_metric(stock_id) }
  end
end
