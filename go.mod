module bitbucket.org/votecube/votecube-ui-non-read

go 1.13

require (
	bitbucket.org/votecube/votecube-go-lib v0.0.0
	github.com/scylladb/gocqlx v1.3.3 // indirect
)

replace github.com/gocql/gocql => github.com/scylladb/gocql v1.3.1

replace bitbucket.org/votecube/votecube-go-lib v0.0.0 => ../votecube-go-lib
