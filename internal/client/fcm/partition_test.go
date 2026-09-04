package fcm

import "testing"

func tokensOfSize(n int) []string {
	tokens := make([]string, n)
	for i := range tokens {
		tokens[i] = "token"
	}
	return tokens
}

func TestPartition_DevuelveNilParaListaVacia(t *testing.T) {
	if batches := partition(nil, 500); batches != nil {
		t.Fatalf("se esperaba nil, se obtuvo: %v", batches)
	}
}

func TestPartition_UnLoteCuandoNoSuperaElLimite(t *testing.T) {
	batches := partition(tokensOfSize(200), 500)
	if len(batches) != 1 || len(batches[0]) != 200 {
		t.Fatalf("se esperaba un unico lote de 200, se obtuvo: %v", sizes(batches))
	}
}

func TestPartition_ParticionaEnLotesDe500(t *testing.T) {
	batches := partition(tokensOfSize(1200), 500)

	got := sizes(batches)
	want := []int{500, 500, 200}
	if len(got) != len(want) {
		t.Fatalf("cantidad de lotes = %v, se esperaba %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("tamanios de lote = %v, se esperaba %v", got, want)
		}
	}
}

func TestPartition_ExactamenteMultiploDelLimite(t *testing.T) {
	batches := partition(tokensOfSize(1000), 500)
	if got := sizes(batches); len(got) != 2 || got[0] != 500 || got[1] != 500 {
		t.Fatalf("se esperaban dos lotes de 500, se obtuvo: %v", got)
	}
}

func sizes(batches [][]string) []int {
	result := make([]int, len(batches))
	for i, batch := range batches {
		result[i] = len(batch)
	}
	return result
}
