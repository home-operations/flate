package deterministic

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/sha256"
	"encoding/base64"
	"fmt"

	bcryptlib "golang.org/x/crypto/bcrypt"
)

var bcryptEncoding = base64.NewEncoding("./ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789").WithPadding(base64.NoPadding)

// bcrypt emits a bcrypt-shaped offline placeholder, not a password hash.
// The seeded salt and input digest avoid the expensive bcrypt cost loop.
func bcrypt(s *stream, input string) string {
	if len(input) > 72 {
		return fmt.Sprintf("failed to encrypt string with bcrypt: %s", bcryptlib.ErrPasswordTooLong)
	}
	var salt [16]byte
	_, _ = s.Read(salt[:])
	h := sha256.New()
	_, _ = h.Write(salt[:])
	_, _ = h.Write([]byte(input))
	digest := h.Sum(nil)
	return "$2a$10$" + bcryptEncoding.EncodeToString(salt[:]) + bcryptEncoding.EncodeToString(digest[:23])
}

// encryptAES preserves sprig's CBC envelope and decryptAES round trip.
// Its seeded IV is only suitable for reproducible offline render placeholders.
func encryptAES(s *stream, password, plaintext string) (string, error) {
	if plaintext == "" {
		return "", nil
	}
	var key [32]byte
	copy(key[:], password)
	block, err := aes.NewCipher(key[:])
	if err != nil {
		return "", err
	}
	padding := aes.BlockSize - len(plaintext)%aes.BlockSize
	ciphertext := make([]byte, aes.BlockSize+len(plaintext)+padding)
	iv, content := ciphertext[:aes.BlockSize], ciphertext[aes.BlockSize:]
	_, _ = s.Read(iv)
	copy(content, plaintext)
	for i := len(plaintext); i < len(content); i++ {
		content[i] = byte(padding)
	}
	cipher.NewCBCEncrypter(block, iv).CryptBlocks(content, content) //nolint:gosec // G407: offline placeholders deliberately use a seeded IV.
	return base64.StdEncoding.EncodeToString(ciphertext), nil
}
