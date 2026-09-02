package fcm

// partition divide tokens en lotes de a lo sumo size elementos,
// preservando el orden. FCM limita el multicast a 500 tokens por
// llamada: sin este particionado, un destinatario con muchos
// dispositivos provocaria un error del SDK.
func partition(tokens []string, size int) [][]string {
	if len(tokens) == 0 {
		return nil
	}
	if size <= 0 {
		size = len(tokens)
	}

	batches := make([][]string, 0, (len(tokens)+size-1)/size)
	for start := 0; start < len(tokens); start += size {
		end := start + size
		if end > len(tokens) {
			end = len(tokens)
		}
		batches = append(batches, tokens[start:end])
	}
	return batches
}
