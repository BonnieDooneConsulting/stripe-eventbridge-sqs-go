package publish

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/eventbridge"
	"github.com/aws/aws-sdk-go-v2/service/eventbridge/types"
)

type fakeEB struct {
	in  *eventbridge.PutEventsInput
	out *eventbridge.PutEventsOutput
}

func (f *fakeEB) PutEvents(_ context.Context, in *eventbridge.PutEventsInput, _ ...func(*eventbridge.Options)) (*eventbridge.PutEventsOutput, error) {
	f.in = in
	return f.out, nil
}

func TestPutUsesStripeShape(t *testing.T) {
	f := &fakeEB{out: &eventbridge.PutEventsOutput{Entries: []types.PutEventsResultEntry{{EventId: aws.String("eb-1")}}}}
	event := `{"id":"evt_1","object":"event","type":"invoice.paid"}`
	id, err := Put(context.Background(), f, "bus", LocalSource, []byte(event))
	if err != nil || id != "eb-1" {
		t.Fatalf("Put = %q, %v", id, err)
	}
	e := f.in.Entries[0]
	if aws.ToString(e.DetailType) != "invoice.paid" || aws.ToString(e.Source) != LocalSource || aws.ToString(e.Detail) != event {
		t.Fatalf("entry = %+v", e)
	}
}

func TestPutReportsFailedEntries(t *testing.T) {
	f := &fakeEB{out: &eventbridge.PutEventsOutput{
		FailedEntryCount: 1,
		Entries:          []types.PutEventsResultEntry{{ErrorCode: aws.String("NotAuthorizedForSourceException"), ErrorMessage: aws.String("no")}},
	}}
	if _, err := Put(context.Background(), f, "bus", LocalSource, []byte(`{"type":"x"}`)); err == nil {
		t.Fatal("want error for a rejected entry")
	}
}
