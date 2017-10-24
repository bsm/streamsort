#!/usr/bin/env ruby

require 'faker'
require 'json'

Faker::Config.random = Random.new(33)

company = Faker::Company.unique
File.open(File.expand_path("../stocks.json", __FILE__), "w") do |out|
  100000.times do |n|
    out.puts({
      id: n+1,
      company: company.name,
      year: Faker::Date.birthday(1, 30).year,
      price: Faker::Commerce.price,
    }.to_json)
  end
end
