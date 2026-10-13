package store

import (
	"encoding/json"
	"errors"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

func jsonBytes(v any) (string, error) {
	if v == nil {
		return "{}", nil
	}
	if s, ok := v.(string); ok {
		if s == "" {
			return "{}", nil
		}
		return s, nil
	}
	b, err := json.Marshal(v)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func cutoffStr(days int) string {
	return time.Now().UTC().AddDate(0, 0, -days).Format(time.RFC3339Nano)
}

func onConflictDoNothing() clause.Expression {
	return clause.OnConflict{DoNothing: true}
}

// TodosReplace replaces a session's todo rows wholesale.
func (d *DB) TodosReplace(tenant, session string, rows []struct {
	Content  string
	Status   string
	Priority string
}) error {
	return d.g.Transaction(func(tx *gorm.DB) error {
		if err := tx.Where("tenant = ? AND session_id = ?", tenant, session).Delete(&BundledTodo{}).Error; err != nil {
			return err
		}
		now := time.Now().Unix()
		for _, r := range rows {
			t := BundledTodo{Tenant: tenant, SessionID: session, Content: r.Content, Status: r.Status, Priority: r.Priority, CreatedAt: now}
			if err := tx.Create(&t).Error; err != nil {
				return err
			}
		}
		return nil
	})
}

// InsertPart appends one part of a message.
func (d *DB) InsertPart(tenant, messageID, partType string, seq int, data any) error {
	raw, err := jsonBytes(data)
	if err != nil {
		return err
	}
	p := Part{ID: newID(), Tenant: tenant, MessageID: messageID, Type: partType, Seq: seq, Data: raw}
	return d.g.Create(&p).Error
}

// PartsByMessages returns the ordered parts for the given message ids.
func (d *DB) PartsByMessages(tenant string, ids []string) ([]Part, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	var out []Part
	err := d.g.Where("tenant = ? AND message_id IN ?", tenant, ids).
		Order("message_id, seq").Find(&out).Error
	return out, err
}

// ---- Providers ----

// ListProviders returns a tenant's providers plus the shared (global) ones; a
// tenant-local row shadows a shared row with the same id.
func (d *DB) ListProviders(tenant string) ([]Provider, error) {
	var rows []Provider
	err := d.g.Where("tenant = ? OR tenant = ?", tenant, "global").
		Order("provider_id").Find(&rows).Error
	if err != nil {
		return nil, err
	}
	byID := map[string]Provider{}
	for _, r := range rows {
		if _, seen := byID[r.ProviderID]; !seen || r.Tenant != "global" {
			byID[r.ProviderID] = r
		}
	}
	out := make([]Provider, 0, len(byID))
	for _, r := range byID {
		out = append(out, r)
	}
	return out, nil
}

// UpsertProvider inserts or replaces a provider row.
func (d *DB) UpsertProvider(p Provider) error {
	if p.CreatedAt == "" {
		p.CreatedAt = nowStr()
	}
	p.UpdatedAt = nowStr()
	return d.g.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "tenant"}, {Name: "provider_id"}},
		DoUpdates: clause.AssignmentColumns([]string{"capability", "api_type", "base_url", "api_key", "headers", "models", "updated_at"}),
	}).Create(&p).Error
}

// DeleteProvider removes a tenant-local provider row.
func (d *DB) DeleteProvider(tenant, providerID string) error {
	return d.g.Where("tenant = ? AND provider_id = ?", tenant, providerID).Delete(&Provider{}).Error
}

// ---- Tenants ----

// CreateTenant inserts a tenant.
func (d *DB) CreateTenant(id, name string) error {
	t := Tenant{ID: id, Name: name, CreatedAt: nowStr(), UpdatedAt: nowStr()}
	return d.g.Create(&t).Error
}

// ListTenants returns all tenants.
func (d *DB) ListTenants() ([]Tenant, error) {
	var out []Tenant
	err := d.g.Order("id").Find(&out).Error
	return out, err
}

// GetTenant returns (tenant, true) or (nil, false).
func (d *DB) GetTenant(id string) (*Tenant, bool, error) {
	var t Tenant
	err := d.g.Where("id = ?", id).First(&t).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	return &t, true, nil
}

// UpdateTenant patches name/disabled.
func (d *DB) UpdateTenant(id string, name *string, disabled *bool) error {
	patch := map[string]any{"updated_at": nowStr()}
	if name != nil {
		patch["name"] = *name
	}
	if disabled != nil {
		patch["disabled"] = *disabled
	}
	return d.g.Model(&Tenant{}).Where("id = ?", id).Updates(patch).Error
}

// DeleteTenant removes a tenant row.
func (d *DB) DeleteTenant(id string) error {
	return d.g.Where("id = ?", id).Delete(&Tenant{}).Error
}

// CreateToken stores a token (sha256 only), returning its id.
func (d *DB) CreateToken(tenantID, tokenSHA256, label string) (string, error) {
	id := newID()
	t := TenantToken{TokenID: id, TenantID: tenantID, TokenSHA256: tokenSHA256, Label: label, CreatedAt: nowStr()}
	if err := d.g.Create(&t).Error; err != nil {
		return "", err
	}
	return id, nil
}

// TenantByTokenSHA returns the tenant owning an unrevoked token hash.
func (d *DB) TenantByTokenSHA(sha string) (*Tenant, *TenantToken, bool, error) {
	var tok TenantToken
	err := d.g.Where("token_sha256 = ?", sha).First(&tok).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil, false, nil
	}
	if err != nil {
		return nil, nil, false, err
	}
	if tok.RevokedAt != nil && *tok.RevokedAt != "" {
		return nil, nil, false, nil
	}
	t, ok, err := d.GetTenant(tok.TenantID)
	if err != nil || !ok {
		return nil, &tok, false, err
	}
	return t, &tok, true, nil
}

// TouchToken records last_used_at.
func (d *DB) TouchToken(tokenID string) error {
	return d.g.Model(&TenantToken{}).Where("token_id = ?", tokenID).
		Update("last_used_at", nowStr()).Error
}

// ListTokens returns a tenant's tokens.
func (d *DB) ListTokens(tenantID string) ([]TenantToken, error) {
	var out []TenantToken
	err := d.g.Where("tenant_id = ?", tenantID).Order("created_at").Find(&out).Error
	return out, err
}

// RevokeToken marks a token revoked.
func (d *DB) RevokeToken(tokenID string) error {
	return d.g.Model(&TenantToken{}).Where("token_id = ?", tokenID).
		Update("revoked_at", nowStr()).Error
}

// ---- Files ----

// UpsertFile inserts metadata if absent, returning the STORED record (the
// existing one wins on a (tenant, sha256) collision). Check-then-insert avoids
// the mysql `ON DUPLICATE KEY` semantic mismatch.
func (d *DB) UpsertFile(f AgentFile) (AgentFile, error) {
	var existing AgentFile
	err := d.g.Where("tenant = ? AND sha256 = ?", f.Tenant, f.SHA256).First(&existing).Error
	if err == nil {
		return existing, nil
	}
	if !errors.Is(err, gorm.ErrRecordNotFound) {
		return AgentFile{}, err
	}
	if f.CreatedAt == "" {
		f.CreatedAt = nowStr()
	}
	if err := d.g.Create(&f).Error; err != nil {
		var again AgentFile
		if e2 := d.g.Where("tenant = ? AND sha256 = ?", f.Tenant, f.SHA256).First(&again).Error; e2 == nil {
			return again, nil
		}
		return AgentFile{}, err
	}
	return f, nil
}

// FileByCode returns (file, true) or (nil, false).
func (d *DB) FileByCode(tenant, code string) (*AgentFile, bool, error) {
	var f AgentFile
	err := d.g.Where("tenant = ? AND code = ?", tenant, code).First(&f).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	return &f, true, nil
}

// UpdateFileMedia stores the async media probe results.
func (d *DB) UpdateFileMedia(code string, width, height *int, durationMS *int64, thumbCode, thumbhash *string) error {
	return d.g.Model(&AgentFile{}).Where("code = ?", code).Updates(map[string]any{
		"width": width, "height": height, "duration_ms": durationMS,
		"thumb_code": thumbCode, "thumbhash": thumbhash,
	}).Error
}
