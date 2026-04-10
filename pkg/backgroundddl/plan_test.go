package backgroundddl

import "testing"

func TestPlanStatementsTracksSearchPathAndFunctionBodies(t *testing.T) {
	plans, err := PlanStatements(`
SET search_path TO app, public;
CREATE TABLE users (id int);
CREATE FUNCTION touch_users() RETURNS void AS $$
BEGIN
  PERFORM 1;
END;
$$ LANGUAGE plpgsql;
`)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if len(plans) != 3 {
		t.Fatalf("expected 3 plans, got %d", len(plans))
	}
	if plans[0].Kind != StatementKindSet {
		t.Fatalf("expected first statement to be SET, got %s", plans[0].Kind)
	}
	if plans[1].Kind != StatementKindTrackedDDL || plans[1].TargetType != "table" {
		t.Fatalf("expected second statement to be tracked table DDL, got %#v", plans[1])
	}
	if plans[1].TargetSchema != "app" || plans[1].TargetName != "users" {
		t.Fatalf("unexpected tracked target: %#v", plans[1])
	}
	if plans[2].Kind != StatementKindDirect || plans[2].TargetKind != TargetKindFunction {
		t.Fatalf("expected third statement to be direct function, got %#v", plans[2])
	}
	if plans[2].TargetSchema != "app" || plans[2].TargetName != "touch_users" {
		t.Fatalf("unexpected function target: %#v", plans[2])
	}
}

func TestPlanStatementsRejectsTransactionControl(t *testing.T) {
	_, err := PlanStatements("BEGIN; CREATE TABLE t (id int);")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestPlanStatementsSupportsTrackedSourceAndSink(t *testing.T) {
	plans, err := PlanStatements(`CREATE SOURCE s WITH (connector='kafka') FORMAT PLAIN ENCODE JSON; DROP SINK foo;`)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if len(plans) != 2 {
		t.Fatalf("expected 2 plans, got %d", len(plans))
	}
	if plans[0].Kind != StatementKindTrackedDDL || plans[0].TargetType != "source" || plans[0].TargetName != "s" {
		t.Fatalf("unexpected source plan: %#v", plans[0])
	}
	if plans[1].Kind != StatementKindTrackedDDL || plans[1].TargetType != "sink" || plans[1].TargetName != "foo" {
		t.Fatalf("unexpected sink plan: %#v", plans[1])
	}
	if plans[1].ExpectRelationExists == nil || *plans[1].ExpectRelationExists {
		t.Fatalf("expected drop sink to require relation absence, got %#v", plans[1].ExpectRelationExists)
	}
}
