package mocks

//go:generate mockery --name PreAggregateStore --srcpkg github.com/aevon-lab/project-aevon/internal/aggregation --output ./aggregation --outpkg aggregationmocks --with-expecter
//go:generate mockery --name EventStore --srcpkg github.com/aevon-lab/project-aevon/internal/core/storage --output ./storage --outpkg storagemocks --with-expecter
