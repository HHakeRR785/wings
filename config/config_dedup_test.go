package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBackupDedupConfigDefaultsAndValidation(t *testing.T) {
	dir := t.TempDir()

	writeAndLoad := func(yamlBody string) (*Configuration, error) {
		p := filepath.Join(dir, "config.yml")
		if err := os.WriteFile(p, []byte(yamlBody), 0o600); err != nil {
			return nil, err
		}
		return Load(p)
	}

	t.Run("defaults preserve stock tar behavior", func(t *testing.T) {
		cfg, err := writeAndLoad("debug: false\n")
		require.NoError(t, err)
		assert.Equal(t, "tar", cfg.System.Backups.EffectiveDriver())
		assert.Equal(t, "zstd_fast", cfg.System.Backups.Dedup.EffectiveCompression())
		assert.Equal(t, "incremental", cfg.System.Backups.Dedup.EffectiveReportSize())
		assert.Equal(t, 7, cfg.System.Backups.Dedup.FullScanEvery)
		assert.False(t, cfg.System.Backups.DeleteOnServerDelete)
		assert.Equal(t, "0 4 * * *", cfg.System.Backups.Dedup.GCCron)
		assert.Equal(t, "", cfg.System.Backups.Dedup.VerifyCron)
	})

	t.Run("partial dedup block preserves remaining defaults", func(t *testing.T) {
		cfg, err := writeAndLoad(`
system:
  backups:
    driver: dedup
    dedup:
      compression: zstd_better
`)
		require.NoError(t, err)
		assert.Equal(t, "dedup", cfg.System.Backups.EffectiveDriver())
		assert.Equal(t, "zstd_better", cfg.System.Backups.Dedup.EffectiveCompression())
		assert.Equal(t, 7, cfg.System.Backups.Dedup.FullScanEvery)
		assert.False(t, cfg.System.Backups.DeleteOnServerDelete)
		assert.Equal(t, "0 4 * * *", cfg.System.Backups.Dedup.GCCron)
	})

	t.Run("explicit empty gc_cron and zero full_scan_every are respected", func(t *testing.T) {
		cfg, err := writeAndLoad(`
system:
  backups:
    driver: dedup
    dedup:
      full_scan_every: 0
      gc_cron: ""
      verify_cron: "0 5 * * 0"
`)
		require.NoError(t, err)
		assert.Equal(t, 0, cfg.System.Backups.Dedup.FullScanEvery)
		assert.Equal(t, "", cfg.System.Backups.Dedup.GCCron)
		assert.Equal(t, "0 5 * * 0", cfg.System.Backups.Dedup.VerifyCron)
	})

	t.Run("delete_on_server_delete can be explicitly enabled", func(t *testing.T) {
		cfg, err := writeAndLoad(`
system:
  backups:
    delete_on_server_delete: true
`)
		require.NoError(t, err)
		assert.True(t, cfg.System.Backups.DeleteOnServerDelete)
	})

	invalidConfigs := map[string]string{
		"bad driver": `
system:
  backups:
    driver: magic
`,
		"bad compression": `
system:
  backups:
    driver: dedup
    dedup:
      compression: rar
`,
		"bad report_size": `
system:
  backups:
    driver: dedup
    dedup:
      report_size: unknown
`,
		"negative full_scan_every": `
system:
  backups:
    driver: dedup
    dedup:
      full_scan_every: -3
`,
		"bad gc_cron": `
system:
  backups:
    driver: dedup
    dedup:
      gc_cron: "invalid cron"
`,
		"bad verify_cron": `
system:
  backups:
    driver: dedup
    dedup:
      verify_cron: "99 99 * * *"
`,
	}

	for name, body := range invalidConfigs {
		t.Run(name, func(t *testing.T) {
			_, err := writeAndLoad(body)
			require.Error(t, err)
		})
	}
}
