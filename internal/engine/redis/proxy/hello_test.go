package proxy

import "testing"

func TestExtractHelloAuth_WithUsernameAndPassword(t *testing.T) {
	args := []string{"2", "AUTH", "myuser", "mypass"}
	user, pass, ok, twoArg := extractHelloAuth(args)
	if !ok {
		t.Fatal("expected AUTH to be found")
	}
	if !twoArg {
		t.Fatal("expected twoArg=true for two-arg AUTH")
	}
	if user != "myuser" || pass != "mypass" {
		t.Errorf("got user=%q pass=%q, want myuser/mypass", user, pass)
	}
}

func TestExtractHelloAuth_PasswordOnly(t *testing.T) {
	args := []string{"2", "AUTH", "secretpass"}
	user, pass, ok, twoArg := extractHelloAuth(args)
	if !ok {
		t.Fatal("expected AUTH to be found")
	}
	if twoArg {
		t.Fatal("expected twoArg=false for single-arg AUTH")
	}
	if user != "" || pass != "secretpass" {
		t.Errorf("got user=%q pass=%q, want empty/secretpass", user, pass)
	}
}

func TestExtractHelloAuth_NoAuth(t *testing.T) {
	args := []string{"2"}
	_, _, ok, _ := extractHelloAuth(args)
	if ok {
		t.Error("expected no AUTH found")
	}
}

func TestExtractHelloAuth_WithSetname(t *testing.T) {
	args := []string{"2", "AUTH", "user", "pass", "SETNAME", "myconn"}
	user, pass, ok, twoArg := extractHelloAuth(args)
	if !ok {
		t.Fatal("expected AUTH to be found")
	}
	if !twoArg {
		t.Fatal("expected twoArg=true")
	}
	if user != "user" || pass != "pass" {
		t.Errorf("got user=%q pass=%q, want user/pass", user, pass)
	}
}

func TestExtractHelloAuth_CaseInsensitive(t *testing.T) {
	args := []string{"2", "auth", "u", "p"}
	user, pass, ok, twoArg := extractHelloAuth(args)
	if !ok {
		t.Fatal("expected AUTH to be found (case-insensitive)")
	}
	if !twoArg {
		t.Fatal("expected twoArg=true")
	}
	if user != "u" || pass != "p" {
		t.Errorf("got user=%q pass=%q, want u/p", user, pass)
	}
}

func TestExtractHelloAuth_AuthWithNoArgs(t *testing.T) {
	args := []string{"2", "AUTH"}
	_, _, ok, _ := extractHelloAuth(args)
	if ok {
		t.Error("expected no AUTH found when AUTH has no arguments")
	}
}

func TestExtractHelloAuth_SetnameValueIsAUTH(t *testing.T) {
	// HELLO 2 SETNAME AUTH AUTH realuser realpass
	// The first "AUTH" is the SETNAME value, not the AUTH keyword.
	args := []string{"2", "SETNAME", "AUTH", "AUTH", "realuser", "realpass"}
	user, pass, ok, twoArg := extractHelloAuth(args)
	if !ok {
		t.Fatal("expected AUTH to be found")
	}
	if !twoArg {
		t.Fatal("expected twoArg=true")
	}
	if user != "realuser" || pass != "realpass" {
		t.Errorf("got user=%q pass=%q, want realuser/realpass", user, pass)
	}
}

func TestExtractHelloAuth_SetnameBeforeAuth(t *testing.T) {
	// HELLO 2 SETNAME myconn AUTH user pass
	args := []string{"2", "SETNAME", "myconn", "AUTH", "user", "pass"}
	user, pass, ok, twoArg := extractHelloAuth(args)
	if !ok {
		t.Fatal("expected AUTH to be found")
	}
	if !twoArg {
		t.Fatal("expected twoArg=true")
	}
	if user != "user" || pass != "pass" {
		t.Errorf("got user=%q pass=%q, want user/pass", user, pass)
	}
}

func TestExtractHelloAuth_EmptyUsername(t *testing.T) {
	args := []string{"2", "AUTH", "", "somepass"}
	user, pass, ok, twoArg := extractHelloAuth(args)
	if !ok {
		t.Fatal("expected AUTH to be found")
	}
	if !twoArg {
		t.Fatal("expected twoArg=true for explicit empty username")
	}
	if user != "" || pass != "somepass" {
		t.Errorf("got user=%q pass=%q, want empty/somepass", user, pass)
	}
}
