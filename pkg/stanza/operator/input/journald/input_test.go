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

type fakeJournaldCmd struct {
	startError error
	exitError  *exec.ExitError
	stdErr     string
}

func (f *fakeJournaldCmd) Start() error {
	return f.startError
}

func (*fakeJournaldCmd) StdoutPipe() (io.ReadCloser, error) {
	reader := bytes.NewReader([]byte(journaldTestResponse))
	return io.NopCloser(reader), nil
}

func (f *fakeJournaldCmd) StderrPipe() (io.ReadCloser, error) {
	reader := bytes.NewReader([]byte(f.stdErr))
	return io.NopCloser(reader), nil
}

func (f *fakeJournaldCmd) Wait() error {
	if f.exitError == nil {
		return nil
	}

	return f.exitError
}

type fakeJournaldCmdJSONSeq struct {
	fakeJournaldCmd
}

func (*fakeJournaldCmdJSONSeq) StdoutPipe() (io.ReadCloser, error) {
	// Prefix the record with the RFC 7464 Record Separator (0x1E)
	response := "\x1e" + journaldTestResponse
	reader := bytes.NewReader([]byte(response))
	return io.NopCloser(reader), nil
}

type fakeJournaldCmdJSONSeqWithNewline struct {
	fakeJournaldCmd
}

func (*fakeJournaldCmdJSONSeqWithNewline) StdoutPipe() (io.ReadCloser, error) {
	// First record: a JSON entry where the _SELINUX_CONTEXT field contains a
	// literal (unescaped) newline byte, as can happen with some journald versions.
	// With newline-based reading this would be split into two unparseable fragments.
	// With RS-based reading the full record is read as one unit. The JSON is
	// structurally incomplete (no closing brace), so parsing fails and a warning
	// is logged. The second valid record must still be received.
	invalidEntry := "\x1e{ \"_SELINUX_CONTEXT\": \"unconfined\n\"\n"
	// Second record: a well-formed json-seq entry.
	validEntry := "\x1e" + journaldTestResponse
	reader := bytes.NewReader([]byte(invalidEntry + validEntry))
	return io.NopCloser(reader), nil
}

type fakeJournaldCmdJSONWithNewline struct {
	fakeJournaldCmd
}

func (*fakeJournaldCmdJSONWithNewline) StdoutPipe() (io.ReadCloser, error) {
	// Entry where _SELINUX_CONTEXT contains a literal (unescaped) newline byte.
	// In plain json format, ReadBytes('\n') splits this at the embedded newline,
	// producing two fragments that both fail to parse. The entry is lost.
	// A second valid entry follows to confirm processing continues.
	invalidEntry := "{ \"_SELINUX_CONTEXT\": \"unconfined\n\", \"__REALTIME_TIMESTAMP\": \"1587047866229555\", \"__CURSOR\": \"cursor-invalid\" }\n"
	validEntry := journaldTestResponse
	reader := bytes.NewReader([]byte(invalidEntry + validEntry))
	return io.NopCloser(reader), nil
}

func TestInputJournald(t *testing.T) {
	cfg := NewConfigWithID("my_journald_input")
	cfg.OutputIDs = []string{"output"}

	set := componenttest.NewNopTelemetrySettings()
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
		return &fakeJournaldCmd{}
	}

	require.NoError(t, op.Start(testutil.NewUnscopedMockPersister()))
	defer func() {
		require.NoError(t, op.Stop())
	}()

	expected := map[string]any{
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
	}

	select {
	case e := <-received:
		require.Equal(t, expected, e.Body)
	case <-time.After(time.Second):
		require.FailNow(t, "Timed out waiting for entry to be read")
	}
}

func TestInputJournaldJSONSeq(t *testing.T) {
	cfg := NewConfigWithID("my_journald_input")
	cfg.OutputIDs = []string{"output"}
	cfg.OutputFormat = OutputFormatJSONSeq

	set := componenttest.NewNopTelemetrySettings()
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
		return &fakeJournaldCmdJSONSeq{}
	}

	require.NoError(t, op.Start(testutil.NewUnscopedMockPersister()))
	defer func() {
		require.NoError(t, op.Stop())
	}()

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

func TestInputJournaldJSONSeqEmbeddedNewline(t *testing.T) {
	cfg := NewConfigWithID("my_journald_input")
	cfg.OutputIDs = []string{"output"}
	cfg.OutputFormat = OutputFormatJSONSeq

	set := componenttest.NewNopTelemetrySettings()
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
		return &fakeJournaldCmdJSONSeqWithNewline{}
	}

	require.NoError(t, op.Start(testutil.NewUnscopedMockPersister()))
	defer func() {
		require.NoError(t, op.Stop())
	}()

	// The first record has a literal unescaped newline in the JSON text, making
	// it invalid JSON. The RS-based reader reads the full record across the newline
	// boundary; parsing fails and a warning is logged. The second valid record
	// must still be received, proving error recovery works correctly.
	select {
	case e := <-received:
		require.Equal(t, "run-docker-netns-4f76d707d45f.mount: Succeeded.", e.Body.(map[string]any)["MESSAGE"])
	case <-time.After(time.Second):
		require.FailNow(t, "Timed out waiting for entry to be read")
	}
}

func TestInputJournaldJSONEmbeddedNewline(t *testing.T) {
	cfg := NewConfigWithID("my_journald_input")
	cfg.OutputIDs = []string{"output"}
	// Default json format — newline-delimited reading.

	set := componenttest.NewNopTelemetrySettings()
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
		return &fakeJournaldCmdJSONWithNewline{}
	}

	require.NoError(t, op.Start(testutil.NewUnscopedMockPersister()))
	defer func() {
		require.NoError(t, op.Stop())
	}()

	// With json (newline-delimited) format, the literal newline in _SELINUX_CONTEXT
	// causes ReadBytes('\n') to split the JSON into two unparseable fragments.
	// Both fragments fail to parse and the entry with cursor "cursor-invalid" is lost.
	// The second valid entry must still be received.
	select {
	case e := <-received:
		require.Equal(t, "run-docker-netns-4f76d707d45f.mount: Succeeded.", e.Body.(map[string]any)["MESSAGE"])
		// The broken entry must not have been delivered.
		require.NotEqual(t, "cursor-invalid", e.Body.(map[string]any)["__CURSOR"])
	case <-time.After(time.Second):
		require.FailNow(t, "Timed out waiting for entry to be read")
	}
}

func TestBuildConfigArgs(t *testing.T) {
	testCases := []struct {
		Name          string
		Config        func(_ *Config)
		Expected      []string
		ExpectedError string
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
			Name: "invalid output_format",
			Config: func(cfg *Config) {
				cfg.OutputFormat = "invalid"
			},
			ExpectedError: "invalid value 'invalid' for parameter 'output_format'",
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

			if tt.ExpectedError != "" {
				require.Error(t, err)
				require.ErrorContains(t, err, tt.ExpectedError)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.Expected, args)
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

func TestConfigValidation(t *testing.T) {
	testCases := []struct {
		Name          string
		Config        func(_ *Config)
		ExpectedError string
	}{
		{
			Name:   "empty config",
			Config: func(_ *Config) {},
		},
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

			if tt.ExpectedError != "" {
				require.Error(t, err)
				require.ErrorContains(t, err, tt.ExpectedError)
				return
			}
			require.NoError(t, err)
		})
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
