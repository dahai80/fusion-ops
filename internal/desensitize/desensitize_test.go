package desensitize

import "testing"

func TestTextEmail(t *testing.T) {
	got := Text("contact alice@example.com now")
	if got != "contact [EMAIL] now" {
		t.Fatalf("email not scrubbed: %q", got)
	}
}

func TestTextPhone(t *testing.T) {
	got := Text("call 13812345678 or 010-12345678")
	if got != "call [PHONE] or [PHONE]" {
		t.Fatalf("phone not scrubbed: %q", got)
	}
}

func TestTextIDCard(t *testing.T) {
	got := Text("id 110101199003077512")
	if got != "id [IDCARD]" {
		t.Fatalf("idcard not scrubbed: %q", got)
	}
}

func TestTextSecret(t *testing.T) {
	cases := []string{
		"key sk-abcdef1234567890abcdef",
		"token eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.payload.sig",
		"hex deadbeefdeadbeefdeadbeefdeadbeef",
	}
	for _, c := range cases {
		got := Text(c)
		if got == c {
			t.Fatalf("secret not scrubbed: %q", got)
		}
	}
}

func TestTextCardLuhn(t *testing.T) {
	// 4111111111111111 passes Luhn; 4111111111111112 does not.
	if got := Text("card 4111111111111111"); got != "card [CARD]" {
		t.Fatalf("luhn card not scrubbed: %q", got)
	}
	if got := Text("num 4111111111111112"); got != "num 4111111111111112" {
		t.Fatalf("non-luhn number wrongly scrubbed: %q", got)
	}
}

func TestTextIP(t *testing.T) {
	if got := Text("from 192.168.1.100"); got != "from [IP]" {
		t.Fatalf("ip not scrubbed: %q", got)
	}
}

func TestTextURLCredentials(t *testing.T) {
	got := Text("connect postgres://admin:s3cret@db.internal:5432/x")
	if got != "connect postgres://[CRED]@db.internal:5432/x" {
		t.Fatalf("url creds not stripped: %q", got)
	}
}

func TestTenantUser(t *testing.T) {
	ten := Tenant("acme-corp")
	usr := User("alice")
	if len(ten) != len("[TENANT:12345678]") || len(usr) != len("[USER:12345678]") {
		t.Fatalf("hash forms wrong: %q %q", ten, usr)
	}
	if Tenant("acme-corp") != ten {
		t.Fatalf("tenant hash not deterministic")
	}
}

func TestHasPII(t *testing.T) {
	if !HasPII("mail me at bob@x.io") {
		t.Fatal("HasPII should detect email")
	}
	if HasPII("all clean text here") {
		t.Fatal("HasPII false positive")
	}
}
