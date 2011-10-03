Gem::Specification.new do |s|
  s.name        = 'amqpshell'
  s.version     = '0.0.1'
  s.date        = '2011-10-03'
  s.summary     = "A remote execution shell using amqp"
  s.description = "A skeleton for implementing the client and server ends of nodes communicating over amqp"
  s.authors     = ["Aditya Godbole"]
  s.email       = 'aditya.a.godbole@gmail.com'
  s.files       = ["lib/amqpshell.rb",
				   "lib/amqpshell/node.rb",
				   "lib/amqpshell/client.rb",
                   "lib/amqpshell/utils.rb",
                   "lib/amqpshell/queueio.rb"]
  s.homepage    = 'https://github.com/adityagodbole/amqpshell'
  s.add_dependency('amqp', '>= 0.8.0')
  s.add_dependency('rubyzip')
  s.add_dependency('log4r')
end

