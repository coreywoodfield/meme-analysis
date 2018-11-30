
using PyCall, DataFrames, CSV, ProgressMeter

const input, output = ARGS

const fileregex = r"^.*/(.*)_(before|after|afterfiltered).txt$"
const categories = ["before", "after", "afterfiltered"]

function processallfiles(process, columnnames)
	columns() = [:username => String[]; [Symbol(col) => Float64[] for col in columnnames]]
	dataframes = Dict(category => DataFrame(columns()...) for category in categories)

	files = map(file -> joinpath(input, file), readdir(input))
	@showprogress 1 "Analyzing files..." for file in files
		m = match(fileregex, file)
		username, category = m.captures
		row = process(read(file, String))
		row[:username] = username
		push!(dataframes[category], row)
	end
	dataframes
end

@pyimport empath

function runempath()
	lexicon = empath.Empath()
	analyze = lexicon[:analyze]
	processallfiles(keys(lexicon[:cats])) do text
		dict = analyze(text, normalize=true)
		Dict{Symbol,Any}(Symbol(k) => v for (k, v) in pairs(dict))
	end
end

data = runempath()
for category in categories
	CSV.write("$output/empath_$category.csv", data[category])
end
