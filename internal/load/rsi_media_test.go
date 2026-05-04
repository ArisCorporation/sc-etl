package load

import (
	"reflect"
	"testing"
)

func TestPickMediaUsesLargestDerivedVariant(t *testing.T) {
	media := pickMedia([]rsiMatrixMedia{
		{
			MembershipSlot: "thumbnail",
			SourceStream:   rsiSourceStream{Progressive: "/media/aurora/source.jpg"},
			DerivedData: rsiDerivedData{
				Sizes: map[string]rsiDerivedSize{
					"store_thumb_listing_small": {Width: 186, Height: 63},
					"store_large":               {Width: 818, Height: 288},
					"wallpaper_3840x2160":       {Width: 3840, Height: 2160},
				},
			},
			Images: map[string]string{
				"store_thumb_listing_small": "/media/aurora/store_thumb_listing_small.jpg",
				"store_large":               "/media/aurora/store_large.jpg",
				"wallpaper_3840x2160":       "/media/aurora/wallpaper_3840x2160.jpg",
			},
		},
	})
	if media == nil {
		t.Fatal("expected media")
	}

	want := "https://robertsspaceindustries.com/media/aurora/wallpaper_3840x2160.jpg"
	if media.Thumbnail != want {
		t.Fatalf("thumbnail mismatch: got %q want %q", media.Thumbnail, want)
	}
	if media.Store != want {
		t.Fatalf("store mismatch: got %q want %q", media.Store, want)
	}
	if !reflect.DeepEqual(media.Gallery, []string{want}) {
		t.Fatalf("gallery mismatch: got %#v want %#v", media.Gallery, []string{want})
	}
}

func TestPickMediaFallsBackToSourceWithoutSizeMetadata(t *testing.T) {
	media := pickMedia([]rsiMatrixMedia{
		{
			MembershipSlot: "thumbnail",
			SourceURL:      "https://media.robertsspaceindustries.com/ship/source.jpg",
			Images: map[string]string{
				"store_thumb_listing_small": "https://media.robertsspaceindustries.com/ship/store_thumb_listing_small.jpg",
				"store_large":               "https://media.robertsspaceindustries.com/ship/store_large.jpg",
				"wallpaper_1920x1080":       "https://media.robertsspaceindustries.com/ship/wallpaper_1920x1080.jpg",
				"wallpaper_3840x2160":       "https://media.robertsspaceindustries.com/ship/wallpaper_3840x2160.jpg",
			},
		},
	})
	if media == nil {
		t.Fatal("expected media")
	}

	want := "https://media.robertsspaceindustries.com/ship/source.jpg"
	if media.Thumbnail != want {
		t.Fatalf("thumbnail mismatch: got %q want %q", media.Thumbnail, want)
	}
	if media.Store != want {
		t.Fatalf("store mismatch: got %q want %q", media.Store, want)
	}
	if !reflect.DeepEqual(media.Gallery, []string{want}) {
		t.Fatalf("gallery mismatch: got %#v want %#v", media.Gallery, []string{want})
	}
}

func TestPickMediaAggregatesDistinctMediaAssetsOnce(t *testing.T) {
	media := pickMedia([]rsiMatrixMedia{
		{
			MembershipSlot: "thumbnail",
			SourceURL:      "https://media.robertsspaceindustries.com/one/source.jpg",
			DerivedData: rsiDerivedData{
				Sizes: map[string]rsiDerivedSize{
					"store_large":         {Width: 818, Height: 288},
					"wallpaper_3840x2160": {Width: 3840, Height: 2160},
				},
			},
			Images: map[string]string{
				"store_large":         "https://media.robertsspaceindustries.com/one/store_large.jpg",
				"wallpaper_3840x2160": "https://media.robertsspaceindustries.com/one/wallpaper_3840x2160.jpg",
			},
		},
		{
			MembershipSlot: "gallery",
			SourceURL:      "https://media.robertsspaceindustries.com/two/source.jpg",
			DerivedData: rsiDerivedData{
				Sizes: map[string]rsiDerivedSize{
					"slideshow_wide":      {Width: 1200, Height: 800},
					"wallpaper_3840x2160": {Width: 3840, Height: 2160},
				},
			},
			Images: map[string]string{
				"slideshow_wide":      "https://media.robertsspaceindustries.com/two/slideshow_wide.jpg",
				"wallpaper_3840x2160": "https://media.robertsspaceindustries.com/two/wallpaper_3840x2160.jpg",
			},
		},
	})
	if media == nil {
		t.Fatal("expected media")
	}

	wantGallery := []string{
		"https://media.robertsspaceindustries.com/one/wallpaper_3840x2160.jpg",
		"https://media.robertsspaceindustries.com/two/wallpaper_3840x2160.jpg",
	}
	if !reflect.DeepEqual(media.Gallery, wantGallery) {
		t.Fatalf("gallery mismatch: got %#v want %#v", media.Gallery, wantGallery)
	}
}
