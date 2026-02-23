package loghouse

import (
	"fmt"
	"os"
	"os/user"
	"path/filepath"
	"strconv"
	"syscall"
)

func EnsureNamedPipe(path string, mode os.FileMode, groupName string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}

	fi, err := os.Lstat(path)
	if err == nil {
		if fi.Mode()&os.ModeNamedPipe == 0 {
			return fmt.Errorf("path %q exists and is not a named pipe", path)
		}
	} else if !os.IsNotExist(err) {
		return err
	} else {
		// best effort: restrict umask effects at create time
		if err := syscall.Mkfifo(path, uint32(mode.Perm())); err != nil {
			return err
		}
	}

	if err := os.Chmod(path, mode); err != nil {
		return err
	}

	if groupName != "" {
		grp, err := user.LookupGroup(groupName)
		if err != nil {
			return fmt.Errorf("lookup group %q: %w", groupName, err)
		}
		gid, err := strconv.Atoi(grp.Gid)
		if err != nil {
			return fmt.Errorf("parse group gid %q: %w", grp.Gid, err)
		}
		if err := os.Chown(path, os.Getuid(), gid); err != nil {
			return err
		}
	}

	// best effort: restrict umask effects
	_ = syscall.Chmod(path, uint32(mode))
	return nil
}
