
using CSV, DataFrames

const fileregex = r"^(.*)_(before|after|afterfiltered).txt$"

data = CSV.read("wholesomedata/results/liwc_results.csv", allowmissing=:none)

matches = String.(getindex.(match.([fileregex], data[:Filename]), [1 2]))

data[:username] = matches[:, 1]
data[:category] = matches[:, 2]

delete!(data, :Filename)

keep = filter(col -> col != :category, names(data))

categories = groupby(data, :category)
for category in categories
	df = category[keep]
	CSV.write("wholesomedata/organized/liwc_$(category[1, :category]).csv", df)
end
