package dag

type Task interface {
	Run() error
}
