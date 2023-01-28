package mrpc

import (
	"context"
	"fmt"
	"io"
	"testing"

	"github.com/tychoish/birch/mrpc/mongowire"
)

func assertRegistryLen(t *testing.T, size int, registry *OperationRegistry) {
	t.Helper()
	if len(registry.ops) != size {
		t.Fatalf("registry had %d, %d items expected", len(registry.ops), size)
	}

}

func TestRegistry(t *testing.T) {
	cases := []struct {
		Name string
		Case func(t *testing.T, handler HandlerFunc, registry *OperationRegistry)
	}{
		{
			Name: "OperationCanOnlyBeRegisteredOnce",
			Case: func(t *testing.T, handler HandlerFunc, registry *OperationRegistry) {
				op := mongowire.OpScope{
					Type:    mongowire.OP_COMMAND,
					Context: "foo",
					Command: "bar",
				}

				assertRegistryLen(t, 0, registry)
				if len(registry.ops) != 0 {
					t.Fatal("registry initialized incorrectly")
				}
				if err := registry.Add(op, handler); err != nil {
					t.Fatal(err)
				}
				assertRegistryLen(t, 1, registry)

				for i := 0; i < 100; i++ {
					if err := registry.Add(op, handler); err == nil {
						t.Fatal(err)
					}
					assertRegistryLen(t, 1, registry)
				}

				// test with the same content, even if it's a different object
				op2 := mongowire.OpScope{
					Type:    mongowire.OP_COMMAND,
					Context: "foo",
					Command: "bar",
				}

				if err := registry.Add(op2, handler); err == nil {
					t.Fatal(err)
				}
				assertRegistryLen(t, 1, registry)
				// add something new and make sure that it is added
				op3 := mongowire.OpScope{
					Type:    mongowire.OP_COMMAND,
					Context: "bar",
					Command: "bar",
				}
				if err := registry.Add(op3, handler); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			Name: "InvalidScopeIsNotAdded",
			Case: func(t *testing.T, handler HandlerFunc, registry *OperationRegistry) {
				op := mongowire.OpScope{}
				if err := registry.Add(op, handler); err == nil {
					t.Fatal(err)
				}

				op.Type = mongowire.OP_KILL_CURSORS
				op.Context = "foo"
				if err := registry.Add(op, handler); err == nil {
					t.Fatal(err)
				}
				assertRegistryLen(t, 0, registry)

			},
		},
		{
			Name: "OpsMustHaveValidHandlers",
			Case: func(t *testing.T, handler HandlerFunc, registry *OperationRegistry) {
				op := mongowire.OpScope{
					Type:    mongowire.OP_COMMAND,
					Context: "foo",
					Command: "bar",
				}
				assertRegistryLen(t, 0, registry)

				if err := op.Validate(); err != nil {
					t.Fatal(err)

				}

				if err := registry.Add(op, nil); err == nil {
					t.Fatal(err)
				}

				assertRegistryLen(t, 0, registry)
			},
		},
		{
			Name: "UndefinedOperationsRetreiveNilResults",
			Case: func(t *testing.T, handler HandlerFunc, registry *OperationRegistry) {
				op := mongowire.OpScope{}

				assertRegistryLen(t, 0, registry)

				assertRegistryLen(t, 0, registry)
				h, ok := registry.Get(&op)
				if ok || h != nil {
					t.Fatal("should not find object from registry")
				}
			},
		},
		{
			Name: "OpsAreRetreivable",
			Case: func(t *testing.T, handler HandlerFunc, registry *OperationRegistry) {
				op := mongowire.OpScope{
					Type:    mongowire.OP_COMMAND,
					Context: "foo",
					Command: "bar",
				}

				if err := registry.Add(op, handler); err != nil {
					t.Fatal(err)
				}
				assertRegistryLen(t, 1, registry)

				h, ok := registry.Get(&op)
				if !ok || h == nil {
					t.Fatal("should find from registry")

				}
				if fmt.Sprint(h) != fmt.Sprint(handler) {
					t.Fatal("should find correct object")
				}
			},
		},
		{
			Name: "OpsWithContextFallBackToNoContext",
			Case: func(t *testing.T, handler HandlerFunc, registry *OperationRegistry) {
				op := mongowire.OpScope{
					Type:    mongowire.OP_COMMAND,
					Context: "foo",
					Command: "bar",
				}

				noContextOp := op
				noContextOp.Context = ""
				if err := registry.Add(noContextOp, handler); err != nil {
					t.Fatal(err)
				}

				h, ok := registry.Get(&op)
				if !ok || h == nil {
					t.Fatal("should find from registry")

				}
				if fmt.Sprint(h) != fmt.Sprint(handler) {
					t.Fatal("should find correct object")
				}
			},
		},
		{
			Name: "OpsWithoutContextFallBackToNoContext",
			Case: func(t *testing.T, handler HandlerFunc, registry *OperationRegistry) {
				op := mongowire.OpScope{
					Type:    mongowire.OP_COMMAND,
					Context: "foo",
					Command: "bar",
				}

				noContextOp := op
				noContextOp.Context = ""
				if err := registry.Add(op, handler); err != nil {
					t.Fatal(err)
				}

				h, ok := registry.Get(&noContextOp)
				if ok || h != nil {
					t.Fatal("should not find from registry")

				}

				var noContextHandler HandlerFunc = func(ctx context.Context, w io.Writer, m mongowire.Message) {}
				if err := registry.Add(noContextOp, noContextHandler); err != nil {
					t.Fatal(err)
				}

				h, ok = registry.Get(&noContextOp)

				if !ok || h == nil {
					t.Fatal("should find from registry")

				}
				if fmt.Sprint(noContextHandler) != fmt.Sprint(h) {
					t.Fatal("should find correct object")
				}
			},
		},
	}
	for _, test := range cases {
		t.Run(test.Name, func(t *testing.T) {
			var callCount int
			handler := func(ctx context.Context, w io.Writer, m mongowire.Message) {
				callCount++
				t.Logf("test handler, call %d", callCount)
			}
			registry := &OperationRegistry{
				ops: map[mongowire.OpScope]HandlerFunc{},
			}

			test.Case(t, handler, registry)
		})
	}
}
