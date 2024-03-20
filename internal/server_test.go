package internal_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"net/url"
	"scratch/internal"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestServerBasicOperations(t *testing.T) {
	store, err := internal.NewDuckDBStore()
	require.NoError(t, err)
	server := httptest.NewServer(internal.NewServer(store).NewServeMux())
	t.Cleanup(func() {
		server.Close()
		assert.NoError(t, store.Close())
	})

	postRes, err := http.Post(
		fmt.Sprintf("%s/data?Table=http_test_table", server.URL),
		"application/json",
		bytes.NewBufferString(`{
			"column_a": "a",
			"column_b": 2
		}`),
	)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, postRes.StatusCode)

	getRes, err := http.Get(fmt.Sprintf("%s/query?q=%s", server.URL, url.QueryEscape("select * from http_test_table")))
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, getRes.StatusCode)
	defer func() {
		_ = getRes.Body.Close()
	}()

	dataBytes, err := io.ReadAll(getRes.Body)
	require.NoError(t, err)

	var data []map[string]any
	require.NoError(t, json.Unmarshal(dataBytes, &data))
	assert.Len(t, data, 1)
	assert.Len(t, data[0], 2)
}

func BenchmarkServerWrites(b *testing.B) {
	store, err := internal.NewDuckDBStore()
	require.NoError(b, err)
	server := httptest.NewServer(internal.NewServer(store).NewServeMux())
	b.Cleanup(func() {
		server.Close()
		assert.NoError(b, store.Close())
	})

	for i := 0; i < b.N; i++ {
		str := fmt.Sprintf(`{
			"column_a": "%s",
			"column_b": "%s",
			"column_c": "%s",
			"column_d": "%s",
			"column_e": "%s"
		}`, randStr(), randStr(), randStr(), randStr(), randStr())

		postRes, postResErr := http.Post(
			fmt.Sprintf("%s/data?Table=http_test_table", server.URL),
			"application/json",
			bytes.NewBufferString(str),
		)
		require.NoError(b, postResErr)
		require.Equal(b, http.StatusOK, postRes.StatusCode)
	}
}

func randStr() string {
	return fmt.Sprintf("%x", rand.New(rand.NewSource(time.Now().UnixNano())).Int63())
}
