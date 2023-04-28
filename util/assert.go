package util

func Assert(val bool) {
	if !val {
		panic("assert err")
	}
}
