require_relative './lib/webservice'


## TODO:
#
# Some option parsing and customized setup.
#



SiaWebservice.build(host: 'localhost', database: 'postgres', 
                    user: 'postgres', password: 'postgres')
              .run!