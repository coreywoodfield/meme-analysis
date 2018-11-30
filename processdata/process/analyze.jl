using CSV, DataFrames, HypothesisTests

const folderprefix = popfirst!(ARGS)

struct Test
	label::Symbol
	increase::Float64
	pval::Float64
	conf1::Float64
	conf2::Float64
end

function Test(column, df1, df2)
	test = OneSampleTTest(df1[column], df2[column])
	Test(column, test.xbar, pvalue(test), confint(test)...)
end

function makedf(tests)
	df = DataFrame()
	matrix = getproperty.(tests, reshape(collect(fieldnames(Test)), 1, :))
	for (i, name) in enumerate(fieldnames(Test))
		df[name] = matrix[:, i]
	end
	df
end

const suffixes = ["before", "after", "afterfiltered"]

function processresults(fileprefix)
	before, after, filtered = map(suffixes) do suffix
		file = joinpath(folderprefix, "organized", "$(fileprefix)_$(suffix).csv")
		df = CSV.read(file, allowmissing=:none)
		sort!(df, :username)
	end

	testcolumns = filter(x -> x != :username, names(before))
	testba = Test.(testcolumns, [after], [before])
	testbf = Test.(testcolumns, [filtered], [before])

	pval(test::Test) = test.pval
	sort!(testba, by=pval)
	sort!(testbf, by=pval)

	dfa = makedf(testba)
	dff = makedf(testbf)

	results = joinpath(folderprefix, "results")
	isdir(results) || mkdir(results)
	CSV.write(joinpath(results, "$(fileprefix)_before_v_after.csv"), dfa)
	CSV.write(joinpath(results, "$(fileprefix)_before_v_filtered.csv"), dff)
end

for prefix in ARGS
	processresults(prefix)
end
