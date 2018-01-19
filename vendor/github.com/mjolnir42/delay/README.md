# delay

```
package delay // import "github.com/mjolnir42/delay"

Package delay implements a flexible waiting policy

type Delay struct{ ... }
    func NewDelay() *Delay

func NewDelay() *Delay
    NewDelay returns a new delay

type Delay struct {
	// Has unexported fields.
}
    Delay can be used for continuous, lazy waiting for a set of goroutines.

    d := delay.NewDelay()
    ...
    d.Use()
    go func() {
        defer d.Done()
        ...
    }()
    d.Wait()

    Not calling Done() will cause Wait() to never return


func NewDelay() *Delay
func (d *Delay) Done()
func (d *Delay) Use()
func (d *Delay) Wait()

func (d *Delay) Use()
    Use signals d that it is in use by an additional goroutine

func (d *Delay) Done()
    Done signals d that a goroutine no longer uses it

func (d *Delay) Wait()
    Wait blocks until d is unused. Users of d can change while it is blocking.
```
