// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//go:build linux

package journald

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os/exec"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/stanza/entry"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/stanza/operator"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/stanza/testutil"
)

const journaldTestResponse = `{ "_BOOT_ID": "c4fa36de06824d21835c05ff80c54468", "_CAP_EFFECTIVE": "0", "_TRANSPORT": "journal", "_UID": "1000", "_EXE": "/usr/lib/systemd/systemd", "_AUDIT_LOGINUID": "1000", "MESSAGE": "run-docker-netns-4f76d707d45f.mount: Succeeded.", "_PID": "13894", "_CMDLINE": "/lib/systemd/systemd --user", "_MACHINE_ID": "d777d00e7caf45fbadedceba3975520d", "_SELINUX_CONTEXT": "unconfined\n", "CODE_FUNC": "unit_log_success", "SYSLOG_IDENTIFIER": "systemd", "_HOSTNAME": "myhostname", "MESSAGE_ID": "7ad2d189f7e94e70a38c781354912448", "_SYSTEMD_CGROUP": "/user.slice/user-1000.slice/user@1000.service/init.scope", "_SOURCE_REALTIME_TIMESTAMP": "1587047866229317", "USER_UNIT": "run-docker-netns-4f76d707d45f.mount", "SYSLOG_FACILITY": "3", "_SYSTEMD_SLICE": "user-1000.slice", "_AUDIT_SESSION": "286", "CODE_FILE": "../src/core/unit.c", "_SYSTEMD_USER_UNIT": "init.scope", "_COMM": "systemd", "USER_INVOCATION_ID": "88f7ca6bbf244dc8828fa901f9fe9be1", "CODE_LINE": "5487", "_SYSTEMD_INVOCATION_ID": "83f7fc7799064520b26eb6de1630429c", "PRIORITY": "6", "_GID": "1000", "__REALTIME_TIMESTAMP": "1587047866229555", "_SYSTEMD_UNIT": "user@1000.service", "_SYSTEMD_USER_SLICE": "-.slice", "__CURSOR": "s=b1e713b587ae4001a9ca482c4b12c005;i=1eed30;b=c4fa36de06824d21835c05ff80c54468;m=9f9d630205;t=5a369604ee333;x=16c2d4fd4fdb7c36", "__MONOTONIC_TIMESTAMP": "685540311557", "_SYSTEMD_OWNER_UID": "1000" }
`

// fakeJournaldCmd replaces the real journalctl command in tests.
type fakeJournaldCmd struct {
	startError    error
	exitError     *exec.ExitError
	stdErr        string
	stdoutContent string
}

func (f *fakeJournaldCmd) Start() error { return f.startError }

func (f *fakeJournaldCmd) StdoutPipe() (io.ReadCloser, error) {
	return io.NopCloser(bytes.NewReader([]byte(f.stdoutContent))), nil
}

func (f *fakeJournaldCmd) StderrPipe() (io.ReadCloser, error) {
	return io.NopCloser(bytes.NewReader([]byte(f.stdErr))), nil
}

func (f *fakeJournaldCmd) Wait() error {
	if f.exitError != nil {
		return f.exitError
	}
	return nil
}

// startInputOp builds and starts a journald input operator for testing.
// It returns a channel that receives processed entries and registers a cleanup
// to stop the operator at the end of the test.
func startInputOp(t *testing.T, mutateCfg func(*Config), newCmdFn func(context.Context, []byte) cmd) <-chan *entry.Entry {
	t.Helper()
	cfg := NewConfigWithID("my_journald_input")
	cfg.OutputIDs = []string{"output"}
	mutateCfg(cfg)

	op, err := cfg.Build(componenttest.NewNopTelemetrySettings())
	require.NoError(t, err)

	received := make(chan *entry.Entry)
	mockOutput := testutil.NewMockOperator("output")
	mockOutput.On("Process", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		received <- args.Get(1).(*entry.Entry)
	}).Return(nil)

	require.NoError(t, op.SetOutputs([]operator.Operator{mockOutput}))
	op.(*Input).newCmd = newCmdFn

	require.NoError(t, op.Start(testutil.NewUnscopedMockPersister()))
	t.Cleanup(func() { require.NoError(t, op.Stop()) })

	return received
}

func TestInputJournald(t *testing.T) {
	received := startInputOp(t, func(*Config) {}, func(_ context.Context, _ []byte) cmd {
		return &fakeJournaldCmd{stdoutContent: journaldTestResponse}
	})

	select {
	case e := <-received:
		require.Equal(t, map[string]any{
			"_BOOT_ID":                   "c4fa36de06824d21835c05ff80c54468",
			"_CAP_EFFECTIVE":             "0",
			"_TRANSPORT":                 "journal",
			"_UID":                       "1000",
			"_EXE":                       "/usr/lib/systemd/systemd",
			"_AUDIT_LOGINUID":            "1000",
			"MESSAGE":                    "run-docker-netns-4f76d707d45f.mount: Succeeded.",
			"_PID":                       "13894",
			"_CMDLINE":                   "/lib/systemd/systemd --user",
			"_MACHINE_ID":                "d777d00e7caf45fbadedceba3975520d",
			"_SELINUX_CONTEXT":           "unconfined\n",
			"CODE_FUNC":                  "unit_log_success",
			"SYSLOG_IDENTIFIER":          "systemd",
			"_HOSTNAME":                  "myhostname",
			"MESSAGE_ID":                 "7ad2d189f7e94e70a38c781354912448",
			"_SYSTEMD_CGROUP":            "/user.slice/user-1000.slice/user@1000.service/init.scope",
			"_SOURCE_REALTIME_TIMESTAMP": "1587047866229317",
			"USER_UNIT":                  "run-docker-netns-4f76d707d45f.mount",
			"SYSLOG_FACILITY":            "3",
			"_SYSTEMD_SLICE":             "user-1000.slice",
			"_AUDIT_SESSION":             "286",
			"CODE_FILE":                  "../src/core/unit.c",
			"_SYSTEMD_USER_UNIT":         "init.scope",
			"_COMM":                      "systemd",
			"USER_INVOCATION_ID":         "88f7ca6bbf244dc8828fa901f9fe9be1",
			"CODE_LINE":                  "5487",
			"_SYSTEMD_INVOCATION_ID":     "83f7fc7799064520b26eb6de1630429c",
			"PRIORITY":                   "6",
			"_GID":                       "1000",
			"_SYSTEMD_UNIT":              "user@1000.service",
			"_SYSTEMD_USER_SLICE":        "-.slice",
			"__CURSOR":                   "s=b1e713b587ae4001a9ca482c4b12c005;i=1eed30;b=c4fa36de06824d21835c05ff80c54468;m=9f9d630205;t=5a369604ee333;x=16c2d4fd4fdb7c36",
			"__MONOTONIC_TIMESTAMP":      "685540311557",
			"_SYSTEMD_OWNER_UID":         "1000",
		}, e.Body)
	case <-time.After(time.Second):
		require.FailNow(t, "Timed out waiting for entry to be read")
	}
}

func TestInputJournaldJSONSeq(t *testing.T) {
	received := startInputOp(t, func(cfg *Config) {
		cfg.OutputFormat = OutputFormatJSONSeq
	}, func(_ context.Context, _ []byte) cmd {
		// Prefix the record with the RFC 7464 Record Separator (0x1E).
		return &fakeJournaldCmd{stdoutContent: "\x1e" + journaldTestResponse}
	})

	select {
	case e := <-received:
		// Verify the RS prefix was stripped and the entry was correctly parsed,
		// including a field whose value contains a JSON-escaped newline character.
		require.Equal(t, "run-docker-netns-4f76d707d45f.mount: Succeeded.", e.Body.(map[string]any)["MESSAGE"])
		require.Equal(t, "unconfined\n", e.Body.(map[string]any)["_SELINUX_CONTEXT"])
		require.Equal(t, "s=b1e713b587ae4001a9ca482c4b12c005;i=1eed30;b=c4fa36de06824d21835c05ff80c54468;m=9f9d630205;t=5a369604ee333;x=16c2d4fd4fdb7c36", e.Body.(map[string]any)["__CURSOR"])
	case <-time.After(time.Second):
		require.FailNow(t, "Timed out waiting for entry to be read")
	}
}

// embeddedNewlineInvalidEntry is a json-seq record whose JSON string value contains a
// literal (unescaped) newline byte, making it invalid JSON. The RS-based reader reads
// the full record as one unit, but parsing fails.
const embeddedNewlineInvalidEntryJSONSeq = "\x1e{ \"_SELINUX_CONTEXT\": \"unconfined\n\"\n"

// embeddedNewlineInvalidEntryJSON is the same broken content without an RS prefix,
// for use with the newline-delimited json reader.
const embeddedNewlineInvalidEntryJSON = "{ \"_SELINUX_CONTEXT\": \"unconfined\n\", \"__REALTIME_TIMESTAMP\": \"1587047866229555\", \"__CURSOR\": \"cursor-invalid\" }\n"

// TestInputJournaldJSONSeqEmbeddedNewlineInvalid verifies that a json-seq record
// containing a literal (unescaped) newline byte inside a JSON string value is not
// delivered. The RS-based reader reads the full record as one unit, but the literal
// newline makes the JSON invalid so parsing fails and the entry is dropped.
func TestInputJournaldJSONSeqEmbeddedNewlineInvalid(t *testing.T) {
	received := startInputOp(t, func(cfg *Config) {
		cfg.OutputFormat = OutputFormatJSONSeq
	}, func(_ context.Context, _ []byte) cmd {
		return &fakeJournaldCmd{stdoutContent: embeddedNewlineInvalidEntryJSONSeq}
	})

	select {
	case <-received:
		require.FailNow(t, "Invalid entry must not be delivered")
	case <-time.After(100 * time.Millisecond):
		// correct: no entry was delivered
	}
}

// TestInputJournaldJSONSeqEmbeddedNewlineValid verifies that the RS-based reader
// continues processing after a failed parse. After the invalid record from
// TestInputJournaldJSONSeqEmbeddedNewlineInvalid, a well-formed second record
// must still be received.
func TestInputJournaldJSONSeqEmbeddedNewlineValid(t *testing.T) {
	received := startInputOp(t, func(cfg *Config) {
		cfg.OutputFormat = OutputFormatJSONSeq
	}, func(_ context.Context, _ []byte) cmd {
		return &fakeJournaldCmd{stdoutContent: embeddedNewlineInvalidEntryJSONSeq + "\x1e" + journaldTestResponse}
	})

	select {
	case e := <-received:
		require.Equal(t, "run-docker-netns-4f76d707d45f.mount: Succeeded.", e.Body.(map[string]any)["MESSAGE"])
	case <-time.After(time.Second):
		require.FailNow(t, "Timed out waiting for entry to be read")
	}
}

// TestInputJournaldJSONEmbeddedNewlineInvalid verifies that with the default json
// (newline-delimited) format, a record containing a literal newline in a field value
// is not delivered. ReadBytes('\n') splits the JSON at the embedded newline, producing
// fragments that both fail to parse — the same outcome as
// TestInputJournaldJSONSeqEmbeddedNewlineInvalid, but via splitting rather than a
// single failed parse.
func TestInputJournaldJSONEmbeddedNewlineInvalid(t *testing.T) {
	received := startInputOp(t, func(*Config) {}, func(_ context.Context, _ []byte) cmd {
		return &fakeJournaldCmd{stdoutContent: embeddedNewlineInvalidEntryJSON}
	})

	select {
	case <-received:
		require.FailNow(t, "Invalid entry must not be delivered")
	case <-time.After(100 * time.Millisecond):
		// correct: no entry was delivered
	}
}

// TestInputJournaldJSONEmbeddedNewlineValid verifies that the newline-delimited reader
// continues processing after dropping a broken record. After the invalid entry from
// TestInputJournaldJSONEmbeddedNewlineInvalid, a well-formed second entry must still
// be received.
func TestInputJournaldJSONEmbeddedNewlineValid(t *testing.T) {
	received := startInputOp(t, func(*Config) {}, func(_ context.Context, _ []byte) cmd {
		return &fakeJournaldCmd{stdoutContent: embeddedNewlineInvalidEntryJSON + journaldTestResponse}
	})

	select {
	case e := <-received:
		require.Equal(t, "run-docker-netns-4f76d707d45f.mount: Succeeded.", e.Body.(map[string]any)["MESSAGE"])
		require.NotEqual(t, "cursor-invalid", e.Body.(map[string]any)["__CURSOR"])
	case <-time.After(time.Second):
		require.FailNow(t, "Timed out waiting for entry to be read")
	}
}

// TestInputJournaldJSONSeqMultilineEntry verifies that the RS-based reader correctly
// parses a valid JSON entry that is formatted across multiple lines (newlines between
// tokens, not inside string values). Because records are delimited by RS (0x1E), the
// reader collects the entire multi-line object as one unit before parsing it.
// The counterpart test TestInputJournaldJSONMultilineEntry shows the same content is
// lost when using the newline-delimited json format.
func TestInputJournaldJSONSeqMultilineEntry(t *testing.T) {
	received := startInputOp(t, func(cfg *Config) {
		cfg.OutputFormat = OutputFormatJSONSeq
	}, func(_ context.Context, _ []byte) cmd {
		return &fakeJournaldCmd{stdoutContent: "\x1e" + multilineJSONEntry}
	})

	select {
	case e := <-received:
		require.Equal(t, "hello from multiline", e.Body.(map[string]any)["MESSAGE"])
		require.Equal(t, "cursor-multiline", e.Body.(map[string]any)["__CURSOR"])
	case <-time.After(time.Second):
		require.FailNow(t, "Timed out waiting for entry to be read")
	}
}

// multilineJSONEntry is a valid JSON object spread across multiple lines.
// The RS-based json-seq reader handles this correctly; the newline-delimited
// json reader splits it into fragments and loses the entry.
const multilineJSONEntry = "{\n" +
	"  \"MESSAGE\": \"hello from multiline\",\n" +
	"  \"__CURSOR\": \"cursor-multiline\",\n" +
	"  \"__REALTIME_TIMESTAMP\": \"1587047866229555\"\n" +
	"}\n"

// TestInputJournaldJSONMultilineEntryInvalid verifies that a valid JSON entry formatted
// across multiple lines is not delivered by the newline-delimited json reader.
// ReadBytes('\n') stops at the first newline between tokens, producing fragments that
// each fail JSON parsing. Compare with TestInputJournaldJSONSeqMultilineEntry which
// parses the same content correctly using the RS-based reader.
func TestInputJournaldJSONMultilineEntryInvalid(t *testing.T) {
	received := startInputOp(t, func(*Config) {}, func(_ context.Context, _ []byte) cmd {
		return &fakeJournaldCmd{stdoutContent: multilineJSONEntry}
	})

	select {
	case <-received:
		require.FailNow(t, "Multi-line entry must not be delivered by the json reader")
	case <-time.After(100 * time.Millisecond):
		// correct: no entry was delivered
	}
}

// TestInputJournaldJSONMultilineEntryValid verifies that the newline-delimited reader
// continues processing after dropping a multi-line entry. A well-formed single-line
// entry following the multi-line one must still be received.
func TestInputJournaldJSONMultilineEntryValid(t *testing.T) {
	received := startInputOp(t, func(*Config) {}, func(_ context.Context, _ []byte) cmd {
		return &fakeJournaldCmd{stdoutContent: multilineJSONEntry + journaldTestResponse}
	})

	select {
	case e := <-received:
		require.NotEqual(t, "cursor-multiline", e.Body.(map[string]any)["__CURSOR"])
		require.Equal(t, "run-docker-netns-4f76d707d45f.mount: Succeeded.", e.Body.(map[string]any)["MESSAGE"])
	case <-time.After(time.Second):
		require.FailNow(t, "Timed out waiting for entry to be read")
	}
}

func TestInputJournaldError(t *testing.T) {
	cfg := NewConfigWithID("my_journald_input")
	cfg.OutputIDs = []string{"output"}

	set := componenttest.NewNopTelemetrySettings()
	set.Logger, _ = zap.NewDevelopment()
	op, err := cfg.Build(set)
	require.NoError(t, err)

	mockOutput := testutil.NewMockOperator("output")
	received := make(chan *entry.Entry)
	mockOutput.On("Process", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		received <- args.Get(1).(*entry.Entry)
	}).Return(nil)

	err = op.SetOutputs([]operator.Operator{mockOutput})
	require.NoError(t, err)

	op.(*Input).newCmd = func(_ context.Context, _ []byte) cmd {
		return &fakeJournaldCmd{
			exitError:  &exec.ExitError{},
			startError: errors.New("fail to start"),
		}
	}

	err = op.Start(testutil.NewUnscopedMockPersister())
	assert.EqualError(t, err, "journalctl command failed: start journalctl: fail to start")
	require.NoError(t, op.Stop())
}

func TestBuildConfigArgsValid(t *testing.T) {
	testCases := []struct {
		Name     string
		Config   func(*Config)
		Expected []string
	}{
		{
			Name:     "empty config",
			Config:   func(_ *Config) {},
			Expected: []string{"--utc", "--output=json", "--follow", "--priority", "info"},
		},
		{
			Name: "output_format json",
			Config: func(cfg *Config) {
				cfg.OutputFormat = OutputFormatJSON
			},
			Expected: []string{"--utc", "--output=json", "--follow", "--priority", "info"},
		},
		{
			Name: "output_format json-seq",
			Config: func(cfg *Config) {
				cfg.OutputFormat = OutputFormatJSONSeq
			},
			Expected: []string{"--utc", "--output=json-seq", "--follow", "--priority", "info"},
		},
		{
			Name: "units",
			Config: func(cfg *Config) {
				cfg.Units = []string{
					"dbus.service",
					"user@1000.service",
				}
			},
			Expected: []string{"--utc", "--output=json", "--follow", "--unit", "dbus.service", "--unit", "user@1000.service", "--priority", "info"},
		},
		{
			Name: "matches",
			Config: func(cfg *Config) {
				cfg.Matches = []MatchConfig{
					{
						"_SYSTEMD_UNIT": "dbus.service",
					},
					{
						"_UID":          "1000",
						"_SYSTEMD_UNIT": "user@1000.service",
					},
				}
			},
			Expected: []string{"--utc", "--output=json", "--follow", "--priority", "info", "_SYSTEMD_UNIT=dbus.service", "+", "_SYSTEMD_UNIT=user@1000.service", "_UID=1000"},
		},
		{
			Name: "units and matches",
			Config: func(cfg *Config) {
				cfg.Units = []string{"ssh"}
				cfg.Matches = []MatchConfig{
					{
						"_SYSTEMD_UNIT": "dbus.service",
					},
				}
			},
			Expected: []string{"--utc", "--output=json", "--follow", "--unit", "ssh", "--priority", "info", "_SYSTEMD_UNIT=dbus.service"},
		},
		{
			Name: "identifiers",
			Config: func(cfg *Config) {
				cfg.Identifiers = []string{"wireplumber", "systemd"}
			},
			Expected: []string{"--utc", "--output=json", "--follow", "--identifier", "wireplumber", "--identifier", "systemd", "--priority", "info"},
		},
		{
			Name: "grep",
			Config: func(cfg *Config) {
				cfg.Grep = "test_grep"
			},
			Expected: []string{"--utc", "--output=json", "--follow", "--priority", "info", "--grep", "test_grep"},
		},
		{
			Name: "namespace",
			Config: func(cfg *Config) {
				cfg.Namespace = "foo"
			},
			Expected: []string{"--utc", "--output=json", "--follow", "--priority", "info", "--namespace", "foo"},
		},
		{
			Name: "dmesg",
			Config: func(cfg *Config) {
				cfg.Dmesg = true
			},
			Expected: []string{"--utc", "--output=json", "--follow", "--priority", "info", "--dmesg"},
		},
		{
			Name: "all",
			Config: func(cfg *Config) {
				cfg.All = true
			},
			Expected: []string{"--utc", "--output=json", "--follow", "--priority", "info", "--all"},
		},
		{
			Name: "merge",
			Config: func(cfg *Config) {
				cfg.Merge = true
			},
			Expected: []string{"--utc", "--output=json", "--follow", "--priority", "info", "--merge"},
		},
	}

	for _, tt := range testCases {
		t.Run(tt.Name, func(t *testing.T) {
			cfg := NewConfigWithID("my_journald_input")
			tt.Config(cfg)
			args, err := cfg.buildArgs()
			require.NoError(t, err)
			assert.Equal(t, tt.Expected, args)
		})
	}
}

func TestBuildConfigArgsInvalid(t *testing.T) {
	testCases := []struct {
		Name          string
		Config        func(*Config)
		ExpectedError string
	}{
		{
			Name: "invalid output_format",
			Config: func(cfg *Config) {
				cfg.OutputFormat = "invalid"
			},
			ExpectedError: "invalid value 'invalid' for parameter 'output_format'",
		},
		{
			Name: "invalid match",
			Config: func(cfg *Config) {
				cfg.Matches = []MatchConfig{
					{
						"-SYSTEMD_UNIT": "dbus.service",
					},
				}
			},
			ExpectedError: "'-SYSTEMD_UNIT' is not a valid Systemd field name",
		},
	}

	for _, tt := range testCases {
		t.Run(tt.Name, func(t *testing.T) {
			cfg := NewConfigWithID("my_journald_input")
			tt.Config(cfg)
			_, err := cfg.buildArgs()
			require.Error(t, err)
			require.ErrorContains(t, err, tt.ExpectedError)
		})
	}
}

func TestBuildConfigCmd(t *testing.T) {
	testCases := []struct {
		Name       string
		Config     func(_ *Config)
		RequireCmd func(*exec.Cmd)
	}{
		{
			Name:   "empty config",
			Config: func(_ *Config) {},
			RequireCmd: func(cmd *exec.Cmd) {
				require.Nil(t, cmd.SysProcAttr)
				require.NotEmpty(t, cmd.Args)
				assert.Equal(t, "journalctl", cmd.Args[0])
			},
		},
		{
			Name: "custom root_path",
			Config: func(cfg *Config) {
				cfg.RootPath = "/host"
			},
			RequireCmd: func(cmd *exec.Cmd) {
				require.NotNil(t, cmd.SysProcAttr)
				assert.Equal(t, "/host", cmd.SysProcAttr.Chroot)
			},
		},
		{
			Name: "custom journalctl_path",
			Config: func(cfg *Config) {
				cfg.JournalctlPath = "/usr/bin/journalctl"
			},
			RequireCmd: func(cmd *exec.Cmd) {
				require.NotEmpty(t, cmd.Args)
				assert.Equal(t, "/usr/bin/journalctl", cmd.Args[0])
			},
		},
		{
			Name: "custom root_path and journalctl_path",
			Config: func(cfg *Config) {
				cfg.RootPath = "/host"
				cfg.JournalctlPath = "/usr/bin/journalctl"
			},
			RequireCmd: func(cmd *exec.Cmd) {
				require.NotNil(t, cmd.SysProcAttr)
				require.NotEmpty(t, cmd.Args)
				assert.Equal(t, "/host", cmd.SysProcAttr.Chroot)
				// root_path should *not* be prepended to journalctl_path
				assert.Equal(t, "/usr/bin/journalctl", cmd.Args[0])
			},
		},
	}

	for _, tt := range testCases {
		t.Run(tt.Name, func(t *testing.T) {
			cfg := NewConfigWithID("my_journald_input")
			tt.Config(cfg)
			newCmdFunc, err := cfg.buildNewCmdFunc()

			require.NoError(t, err)
			cmd := newCmdFunc(t.Context(), nil).(*exec.Cmd)
			tt.RequireCmd(cmd)
		})
	}
}

func TestBuildConfigCmdCursor(t *testing.T) {
	cfg := NewConfigWithID("my_journald_input")
	newCmdFunc, err := cfg.buildNewCmdFunc()
	require.NoError(t, err)

	cmd := newCmdFunc(t.Context(), []byte("cursor-value")).(*exec.Cmd)
	assert.Contains(t, cmd.Args, "--after-cursor")
	assert.Contains(t, cmd.Args, "cursor-value")

	cmd = newCmdFunc(t.Context(), []byte("  ")).(*exec.Cmd)
	assert.NotContains(t, cmd.Args, "--after-cursor")

	cmd = newCmdFunc(t.Context(), []byte{}).(*exec.Cmd)
	assert.NotContains(t, cmd.Args, "--after-cursor")
}

func TestConfigValidationValid(t *testing.T) {
	testCases := []struct {
		Name   string
		Config func(*Config)
	}{
		{
			Name:   "empty config",
			Config: func(_ *Config) {},
		},
	}

	for _, tt := range testCases {
		t.Run(tt.Name, func(t *testing.T) {
			cfg := NewConfigWithID("my_journald_input")
			tt.Config(cfg)
			require.NoError(t, cfg.validate())
		})
	}
}

func TestConfigValidationInvalid(t *testing.T) {
	testCases := []struct {
		Name          string
		Config        func(*Config)
		ExpectedError string
	}{
		{
			Name: "invalid journalctl_path",
			Config: func(cfg *Config) {
				cfg.JournalctlPath = " "
			},
			ExpectedError: "'journalctl_path' must be non-whitespace",
		},
		{
			Name: "invalid root_path",
			Config: func(cfg *Config) {
				cfg.RootPath = "not/absolute"
				cfg.JournalctlPath = "/usr/bin/journalctl"
			},
			ExpectedError: "'root_path' must be an absolute path",
		},
		{
			Name: "invalid journalctl_path with valid root_path",
			Config: func(cfg *Config) {
				cfg.RootPath = "/host"
				cfg.JournalctlPath = "journalctl"
			},
			ExpectedError: "'journalctl_path' must be an absolute path when 'root_path' is set",
		},
		{
			Name: "invalid start_at",
			Config: func(cfg *Config) {
				cfg.StartAt = "middle"
			},
			ExpectedError: "invalid value 'middle' for parameter 'start_at'",
		},
		{
			Name: "invalid output_format",
			Config: func(cfg *Config) {
				cfg.OutputFormat = "json-pretty"
			},
			ExpectedError: "invalid value 'json-pretty' for parameter 'output_format'",
		},
	}

	for _, tt := range testCases {
		t.Run(tt.Name, func(t *testing.T) {
			cfg := NewConfigWithID("my_journald_input")
			tt.Config(cfg)
			err := cfg.validate()
			require.Error(t, err)
			require.ErrorContains(t, err, tt.ExpectedError)
		})
	}
}
