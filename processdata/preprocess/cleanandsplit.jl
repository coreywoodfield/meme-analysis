
using GZip, CSV, DataFrames, Unicode, Statistics, ProgressMeter

replaceall(input, patterns...) = foldl(replace, patterns, init=input)

function cleanbody(body)
	# remove accents from letters, and then remove all non-ascii characters
	# this may leave us with gibberish but I don't see any way around it
	body = filter(isascii, Unicode.normalize(body, stripmark=true))
	# the order of these is mostly unimportant
	# the only ones where order matters is in the urls (the last 3)
	cleaning_lady = [
		# bold
		r"\*\*(.*)\*\*"s => s"\1",
		# italics
		r"\*(.*)\*"s => s"\1",
		# strikethrough
		r"~~(.*)~~"s => s"\1",
		# code
		# r"`.*`"s => "",
		# r"^ {4}.*$"m => "",
		# r"^\t.*$"m => "",
		# superscript
		r"(?<!\s|\\)\^" => " ",
		# misc. formatting punctuation
		r"\*\*\*$"m => "",
		r"#{1,6} "m => "",
		# quotes
		# r"^&gt;"m => "",
		"&amp;" => "&",
		"&nbsp;" => " ",
		"&gt;" => ">",
		"&lt;" => "<",
		# subreddits/usernames
		r"/r/([A-Za-z0-9]*)" => s"\1",
		r"/u/[A-Za-z0-9]*" => "",
		# links with title text
		r"\[(.*?)\]\([^ ]* \"(.*?)\"\)"s => s"\1 (\2)",
		# links without title text
		r"\[(.*?)\]\(.*?(?<!\\)\)"s => s"\1",
		# unformatted links
		r"
			(?:https?://)?										# protocol
			[a-zA-Z0-9](?:[-a-zA-Z0-9]+[a-zA-Z0-9])?			# first section of domain
			(?:\.[a-zA-Z0-9](?:[-a-zA-Z0-9]+[a-zA-Z0-9])?)+		# more sections of domain, separated by '.'
			(?:/\S*)?											# the path, and any query parameters and such
		"x => ""
	]
	cleaned = replaceall(body, cleaning_lady...)
	# remove control characters, tabs, and newlines
	Unicode.normalize(cleaned, stripcc=true)
end

function clean(text)
	emptybody = r"
				\n							# start of line
				([-a-zA-Z0-9_]+),			# author
				,							# body (empty)
				(\w+|reddit\.com|t:\w+),	# subreddit - normally can't have '.', but reddit.com is the exception
				(1[0-9]{9})$				# created_utc
				"mx => ""
	brokenbody = r"
				^([-a-zA-Z0-9_]+),			# author
				(.*),						# body
				(\w+|reddit\.com|t:\w+),	# subreddit
				(1[0-9]{9})\",,$			# created_utc
				"mx => s"\1,\2,\3,\4"
	fixed = replaceall(text, emptybody, brokenbody)
	data = CSV.read(IOBuffer(fixed), allowmissing=:none, types=[String, String, String, Int])
	data[:body] .= cleanbody.(data[:body])
	data[(!isempty).(data[:body]), :]
end

struct Split
	before::DataFrame
	after::DataFrame
	filtered::DataFrame
end

function bodyisgood(body)
	count(isletter, body) > (length(body) / 2)
end

function splitcomments(inputdata)::Union{Split, Nothing}
	wholesome = ==("wholesomememes")
	times = sort(inputdata[wholesome.(inputdata.subreddit), :created_utc])

	# not enough data to make a good guess as to when they joined the subreddit
	length(times) < 5 && return nothing

	first = times[1]
	# average time between comments
	between = floor(Int, mean(j - i for (i, j) in zip(times, times[2:end])))
	window = first - between

	data = inputdata[bodyisgood.(inputdata.body), :]

	notenoughdata(df) = isempty(df) || sum(s -> length(split(s)), df.body) < 50

	before = data[data.created_utc .< window, :]
	if notenoughdata(before)
		return nothing
	end
	after = data[data.created_utc .>= first, :]
	if notenoughdata(after)
		return nothing
	end
	filtered = after[(!wholesome).(after.subreddit), :]
	if notenoughdata(filtered)
		return nothing
	end

	return Split(before, after, filtered)
end

const seen = Set{String}()

(set::Set)(element) = element ∈ set

function header()
	data = IOBuffer()
	write(data, "author,body,subreddit,created_utc\n")
	data
end

function Base.iterate(io::GZipStream)
	# get rid of header
	readline(io)
	iterate(io, header())
end
Base.iterate(::GZipStream, ::Nothing) = nothing
function Base.iterate(io::GZipStream, data::IOBuffer)
	line = readline(io, keep=true)
	name = split(line, ',', limit=2)[1] * ','
	seen(name) && throw("Files not sorted")
	push!(seen, name)
	write(data, line)
	while !eof(io) && (line = readline(io, keep=true); startswith(line, name))
		write(data, line)
	end
	state = if eof(io)
		nothing
	else
		state = header()
		write(state, line)
		state
	end
	String(take!(data)), state
end

# const startfile = if isempty(ARGS)
# 	0
# else
# 	in = tryparse(Int, ARGS[1])
# 	in == nothing ? 0 : (popfirst!(ARGS); in)
# end

const files = Set(map(i -> parse(Int, i), ARGS))

function processfiles(f, processall=false)
	function shouldprocess(filename)
		regex = r"^part-(\d{5})-.*.csv.gz$"
		m = match(regex, filename)
		m != nothing && (processall || parse(Int, m.captures[1])) ∈ files
	end
	inputdir = "wholesomedata/sorted"
	files = [joinpath(inputdir, file) for file in readdir(inputdir) if shouldprocess(file)]

	for file in files
		@info "Reading file" file
		prog = ProgressUnknown("Users processed:")
		for rawuserdata in gzopen(file)
			userdata = clean(rawuserdata)
			comments = splitcomments(userdata)
			update!(prog)
			comments == nothing && continue
			user = userdata[1, :author]
			f(comments, user)
			@debug "Finished processing user" user
		end
		finish!(prog)
	end
end

function partitioncomments()
	ispath("wholesomedata/preliwc") || mkdir("wholesomedata/preliwc")
	f = strings -> (io -> join(io, strings, "\n"))
	processfiles() do comments, user
		open(f(comments.before[:body]), joinpath("wholesomedata", "preliwc", user * "_before.txt"), "w")
		open(f(comments.after[:body]), joinpath("wholesomedata", "preliwc", user * "_after.txt"), "w")
		open(f(comments.filtered[:body]), joinpath("wholesomedata", "preliwc", user * "_afterfiltered.txt"), "w")
	end
end

function gettimes()
	before = Int[]
	after = Int[]
	processfiles(true) do comments
		append!(before, comments.before[:created_utc])
		append!(after, comments.after[:created_utc])
	end
	@info "before utc" describe(before)
	@info "after utc" describe(after)
end


partitioncomments()
