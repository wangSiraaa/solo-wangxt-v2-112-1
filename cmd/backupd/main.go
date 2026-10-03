// Command backupd runs the local incremental backup HTTP service.
//
// Layout under --repo:
//
//	manifest.sqlite     catalog (snapshots, entries, chunk index, errors)
//	chunks/ab/cdef...   content-addressed, immutable blobs
package main

import (
	"flag"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"incbackup/internal/api"
	"incbackup/internal/backup"
	"incbackup/internal/repo"
)

func main() {
	repoDir := flag.String("repo", "./backup-repo", "repository directory (manifest + chunks)")
	addr := flag.String("addr", "127.0.0.1:8090", "listen address (local API only)")
	flag.Parse()

	if err := os.MkdirAll(*repoDir, 0o755); err != nil {
		log.Fatalf("create repo dir: %v", err)
	}
	abs, err := filepath.Abs(*repoDir)
	if err != nil {
		log.Fatal(err)
	}
	manifest, err := repo.OpenManifest(filepath.Join(abs, "manifest.sqlite"))
	if err != nil {
		log.Fatalf("open manifest: %v", err)
	}
	defer manifest.Close()
	store, err := repo.NewContentStore(filepath.Join(abs, "chunks"))
	if err != nil {
		log.Fatalf("open content store: %v", err)
	}
	engine, err := backup.NewEngine(manifest, store)
	if err != nil {
		log.Fatalf("engine: %v", err)
	}

	// A hard crash between scan and commit leaves pending rows; reconcile
	// them on startup so maintenance sees a definitive verdict.
	if recovered, err := engine.RecoverPending(); err != nil {
		log.Printf("startup recovery: %v", err)
	} else {
		for _, r := range recovered {
			log.Printf("startup recovery: snapshot %d -> %s", r.SnapshotID, r.Status)
		}
	}

	// A GC job interrupted (crash/power loss) sits queued/running/failed with
	// its frozen plan intact. Resume it in place on startup: phase 2 only
	// removes blobs still unreferenced, so this can never delete a shared
	// chunk a snapshot taken while we were down depends on.
	if active, err := manifest.ActiveGCJob(); err != nil {
		log.Printf("startup gc inspect: %v", err)
	} else if active != nil {
		res, err := engine.ResumeGC(active.ID)
		if err != nil {
			log.Printf("startup gc resume job %d: %v", active.ID, err)
		} else {
			log.Printf("startup gc resume: job %d -> %s (deleted=%d kept_shared=%d bytes_freed=%d)",
				res.JobID, res.Status, res.BlobsDeleted, res.BlobsKept, res.BytesFreed)
		}
	}

	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatalf("listen %s: %v", *addr, err)
	}
	srv := &http.Server{
		Handler:           (&api.Server{Engine: engine}).NewRouter(),
		ReadHeaderTimeout: 10 * time.Second,
	}
	log.Printf("backupd listening on http://%s (repo=%s)", ln.Addr(), abs)
	if err := srv.Serve(ln); err != nil && err != http.ErrServerClosed {
		log.Fatal(err)
	}
}
