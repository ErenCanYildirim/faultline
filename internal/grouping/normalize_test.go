package grouping

import "testing"

func TestNormalizeFunction(t *testing.T) {
	tests := []struct{ in, want string }{
		{"(*Cart).Checkout", "(*Cart).Checkout"},
		{"main", "main"},
		{"(*Cart).Checkout.func2", "(*Cart).Checkout.func"},
		{"(*Cart).Checkout.func1.3", "(*Cart).Checkout.func.func"},
		{"Router.func12", "Router.func"},
		{"glob..func1", "glob..func"},
		{"(*Server).Handle-fm", "(*Server).Handle"},
		{"Map[...]", "Map[...]"},
		{"Map[go.shape.int,go.shape.string]", "Map[...]"},
		{"handler.0x1f3a", "handler.0x?"},
		{"funcy", "funcy"}, // a real name that starts with "func"
		{"function1", "function1"},
	}
	for _, tc := range tests {
		if got := NormalizeFunction(tc.in); got != tc.want {
			t.Errorf("NormalizeFunction(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestNormalizeMessage(t *testing.T) {
	tests := []struct{ in, want string }{
		{"user 42 not found", "user <n> not found"},
		{"took 2.5s", "took <dur>"},
		{"took 2.5 s", "took <n> s"},
		{"timeout after 2300ms", "timeout after <dur>"},
		{"retrying in 1m30s", "retrying in <dur>"},
		{"body 512kb over 1.5GiB quota", "body <size> over <size> quota"},
		{"2nd attempt", "2nd attempt"}, // ordinals aren't units
		{"order 0192f3c4-8b2a-7c1d-9e4f-5a6b7c8d9e0f failed", "order <uuid> failed"},
		{"mail to jdoe@example.com bounced", "mail to <email> bounced"},
		{"dial 10.0.0.12:5432 refused", "dial <ip> refused"},
		{"bad pointer 0xc000123456", "bad pointer <hex>"},
		{"key 9f86d081884c7d65 missing", "key <hex> missing"},
		{"id 12345678 missing", "id <n> missing"},    // pure digits are a number
		{"facade deadbeef ok", "facade deadbeef ok"}, // pure-letter words stay
		{`field "email" invalid`, "field <str> invalid"},
		{"value 'x' and `y`", "value <str> and <str>"},
		{"cart-8812.json", "cart-<n>.json"},
		{"utf8 and http2 and user123", "utf8 and http2 and user123"}, // digits inside names stay
		{"  many   spaces\n\there ", "many spaces here"},
	}
	for _, tc := range tests {
		if got := NormalizeMessage(tc.in); got != tc.want {
			t.Errorf("NormalizeMessage(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}
