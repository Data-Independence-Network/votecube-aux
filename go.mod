module bitbucket.org/votecube/votecube-ui-aux

go 1.13

require (
	bitbucket.org/votecube/votecube-go-lib v0.0.0
	github.com/fasthttp/router v0.5.3
	github.com/gocql/gocql v0.0.0-20200103014340-68f928edb90a
	github.com/robfig/cron v0.0.0-00010101000000-000000000000
	github.com/scylladb/gocqlx v1.3.3
	github.com/valyala/fasthttp v1.7.0
)

replace github.com/gocql/gocql => github.com/scylladb/gocql v1.3.1

replace github.com/robfig/cron => github.com/robfig/cron/v3 v3.0.0

replace bitbucket.org/votecube/votecube-go-lib v0.0.0 => ../votecube-go-lib
