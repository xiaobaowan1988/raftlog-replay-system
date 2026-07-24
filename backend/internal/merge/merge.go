// Package merge implements the Deep Merge described in the design doc:
// service Values are overlaid on top of the template's default Values.
package merge

// DeepMerge returns a new map that is `base` with `override` layered on top.
//
// Rules:
//   - keys present only in base or only in override are kept as-is;
//   - when both sides hold a nested map, they are merged recursively;
//   - otherwise (scalars, slices, or a type mismatch) override wins.
//
// Neither input is mutated. This matches Helm-style values merging: the
// service file only needs to specify the fields it wants to change.
func DeepMerge(base, override map[string]any) map[string]any {
	out := make(map[string]any, len(base)+len(override))
	for k, v := range base {
		out[k] = v
	}
	for k, ov := range override {
		if bv, ok := out[k]; ok {
			bm, bok := asMap(bv)
			om, ook := asMap(ov)
			if bok && ook {
				out[k] = DeepMerge(bm, om)
				continue
			}
		}
		out[k] = ov
	}
	return out
}

// asMap normalizes the two map shapes yaml.v3 can produce
// (map[string]any and map[any]any) into map[string]any.
func asMap(v any) (map[string]any, bool) {
	switch m := v.(type) {
	case map[string]any:
		return m, true
	case map[any]any:
		out := make(map[string]any, len(m))
		for k, val := range m {
			ks, ok := k.(string)
			if !ok {
				return nil, false
			}
			out[ks] = val
		}
		return out, true
	default:
		return nil, false
	}
}
