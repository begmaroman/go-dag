module github.com/begmaroman/go-dag

go 1.23

// require github.com/hashicorp/terraform v0.12.20

require (
	github.com/emirpasic/gods v1.18.1
	github.com/go-test/deep v1.1.0
	github.com/google/uuid v1.3.0
	github.com/stretchr/testify v1.10.0
)

require (
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

retract [v1.4.1, v1.4.11]
