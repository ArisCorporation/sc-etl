package load

import "testing"

func TestMediaFileMatchesURL(t *testing.T) {
	url := "https://media.robertsspaceindustries.com/ship/wallpaper_3840x2160.jpg"
	state := mediaFileState{
		ID:          "file-1",
		Description: rsiMediaDescription(url),
	}

	if !mediaFileMatchesURL(state, url) {
		t.Fatalf("expected media file to match %q", url)
	}
	if mediaFileMatchesURL(state, "https://media.robertsspaceindustries.com/ship/store_large.jpg") {
		t.Fatal("expected media file mismatch for different url")
	}
}

func TestMediaGalleryMatchesURLs(t *testing.T) {
	urls := []string{
		"https://media.robertsspaceindustries.com/ship/one.jpg",
		"https://media.robertsspaceindustries.com/ship/two.jpg",
	}
	existing := []mediaFileState{
		{ID: "file-1", Description: rsiMediaDescription(urls[0])},
		{ID: "file-2", Description: rsiMediaDescription(urls[1])},
	}

	if !mediaGalleryMatchesURLs(existing, urls) {
		t.Fatal("expected gallery urls to match")
	}
	if mediaGalleryMatchesURLs(existing, urls[:1]) {
		t.Fatal("expected gallery length mismatch")
	}
}
