module bitbucket.org/votecube/votecube-ui-non-read

go 1.13

require (
	bitbucket.org/votecube/votecube-go-lib v0.0.0
	github.com/robfig/cron v0.0.0-00010101000000-000000000000 // indirect
)

replace github.com/gocql/gocql => github.com/scylladb/gocql v1.3.1

replace github.com/robfig/cron => github.com/robfig/cron/v3 v3.0.0

replace bitbucket.org/votecube/votecube-go-lib v0.0.0 => ../votecube-go-lib
