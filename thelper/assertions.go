package thelper

import "testing"

func AssertString(t *testing.T, message, expected, actual string) {
	t.Helper()
	if expected != actual {
		t.Errorf("%s. expected: %s, actual: %s", message, expected, actual)
	}
}

func AssertInt(t *testing.T, message string, expected, actual int) {
	t.Helper()
	if expected != actual {
		t.Errorf("%s. expected: %d, actual: %d", message, expected, actual)
	}
}

func AssertBool(t *testing.T, message string, expected, actual bool) {
	t.Helper()
	if expected != actual {
		t.Errorf("%s. expected: %t, actual: %t", message, expected, actual)
	}
}

func AssertNoError(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Error(err)
	}
}
