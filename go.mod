module github.com/vesoft-inc/nebula-go/v3

go 1.13

require (
	github.com/facebook/fbthrift v0.31.1-0.20211129061412-801ed7f9f295
	github.com/stretchr/testify v1.7.0
	golang.org/x/net v0.5.0
)

replace github.com/facebook/fbthrift => github.com/veezhang/fbthrift v0.0.0-20230203023025-87fccfe9a1bd
//replace golang.org/x/net => ../golang-net