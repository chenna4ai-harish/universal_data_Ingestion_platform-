import { i as is_array, g as get_prototype_of, o as object_prototype } from './async-D55cHugf.js';
import { b as $locale } from './2-BYQShRaz.js';

/** @import { Snapshot } from './types' */

/**
 * In dev, we keep track of which properties could not be cloned. In prod
 * we don't bother, but we keep a dummy array around so that the
 * signature stays the same
 * @type {string[]}
 */
const empty = [];

/**
 * @template T
 * @param {T} value
 * @param {boolean} [skip_warning]
 * @param {boolean} [no_tojson]
 * @returns {Snapshot<T>}
 */
function snapshot(value, skip_warning = false, no_tojson = false) {

	return clone(value, new Map(), '', empty, null, no_tojson);
}

/**
 * @template T
 * @param {T} value
 * @param {Map<T, Snapshot<T>>} cloned
 * @param {string} path
 * @param {string[]} paths
 * @param {null | T} [original] The original value, if `value` was produced from a `toJSON` call
 * @param {boolean} [no_tojson]
 * @returns {Snapshot<T>}
 */
function clone(value, cloned, path, paths, original = null, no_tojson = false) {
	if (typeof value === 'object' && value !== null) {
		var unwrapped = cloned.get(value);
		if (unwrapped !== undefined) return unwrapped;

		if (value instanceof Map) return /** @type {Snapshot<T>} */ (new Map(value));
		if (value instanceof Set) return /** @type {Snapshot<T>} */ (new Set(value));

		if (is_array(value)) {
			var copy = /** @type {Snapshot<any>} */ (Array(value.length));
			cloned.set(value, copy);

			if (original !== null) {
				cloned.set(original, copy);
			}

			for (var i = 0; i < value.length; i += 1) {
				var element = value[i];
				if (i in value) {
					copy[i] = clone(element, cloned, path, paths, null, no_tojson);
				}
			}

			return copy;
		}

		if (get_prototype_of(value) === object_prototype) {
			/** @type {Snapshot<any>} */
			copy = {};
			cloned.set(value, copy);

			if (original !== null) {
				cloned.set(original, copy);
			}

			for (var key in value) {
				copy[key] = clone(
					// @ts-expect-error
					value[key],
					cloned,
					path,
					paths,
					null,
					no_tojson
				);
			}

			return copy;
		}

		if (value instanceof Date) {
			return /** @type {Snapshot<T>} */ (structuredClone(value));
		}

		if (typeof (/** @type {T & { toJSON?: any } } */ (value).toJSON) === 'function' && !no_tojson) {
			return clone(
				/** @type {T & { toJSON(): any } } */ (value).toJSON(),
				cloned,
				path,
				paths,
				// Associate the instance with the toJSON clone
				value
			);
		}
	}

	if (value instanceof EventTarget) {
		// can't be cloned
		return /** @type {Snapshot<T>} */ (value);
	}

	try {
		return /** @type {Snapshot<T>} */ (structuredClone(value));
	} catch (e) {

		return /** @type {Snapshot<T>} */ (value);
	}
}

const I18N_MARKER = "__i18n__";
const TRANSLATABLE_PROPS = [
  "label",
  "info",
  "placeholder",
  "description",
  "title",
  "value"
];
class ShareError extends Error {
  constructor(message) {
    super(message);
    this.name = "ShareError";
  }
}
async function uploadToHuggingFace(data, type) {
  if (window.__gradio_space__ == null) {
    throw new ShareError("Must be on Spaces to share.");
  }
  let blob;
  let contentType;
  let filename;
  {
    let url;
    if (typeof data === "object" && data.url) {
      url = data.url;
    } else if (typeof data === "string") {
      url = data;
    } else {
      throw new Error("Invalid data format for URL type");
    }
    const response = await fetch(url);
    blob = await response.blob();
    contentType = response.headers.get("content-type") || "";
    filename = response.headers.get("content-disposition") || "";
  }
  const file = new File([blob], filename, { type: contentType });
  const uploadResponse = await fetch("https://huggingface.co/uploads", {
    method: "POST",
    body: file,
    headers: {
      "Content-Type": file.type,
      "X-Requested-With": "XMLHttpRequest"
    }
  });
  if (!uploadResponse.ok) {
    if (uploadResponse.headers.get("content-type")?.includes("application/json")) {
      const error = await uploadResponse.json();
      throw new ShareError(`Upload failed: ${error.error}`);
    }
    throw new ShareError(`Upload failed.`);
  }
  const result = await uploadResponse.text();
  return result;
}
const format_time = (seconds) => {
  const hours = Math.floor(seconds / 3600);
  const minutes = Math.floor(seconds % 3600 / 60);
  const seconds_remainder = Math.round(seconds) % 60;
  const padded_minutes = `${minutes < 10 ? "0" : ""}${minutes}`;
  const padded_seconds = `${seconds_remainder < 10 ? "0" : ""}${seconds_remainder}`;
  if (hours > 0) {
    return `${hours}:${padded_minutes}:${padded_seconds}`;
  }
  return `${minutes}:${padded_seconds}`;
};
const is_browser = typeof window !== "undefined";
const allowed_shared_props = [
  "elem_id",
  "elem_classes",
  "visible",
  "interactive",
  "server_fns",
  "server",
  "id",
  "target",
  "theme_mode",
  "version",
  "root",
  "autoscroll",
  "max_file_size",
  "formatter",
  "client",
  "load_component",
  "scale",
  "min_width",
  "theme",
  "padding",
  "loading_status",
  "label",
  "show_label",
  "validation_error",
  "show_progress",
  "api_prefix",
  "container",
  "attached_events",
  "register_component",
  "dispatcher"
];
function has_i18n_marker(value) {
  return typeof value === "string" && value.includes(I18N_MARKER);
}
function translate_i18n_marker(value, translate) {
  const start = value.indexOf(I18N_MARKER);
  if (start === -1) return value;
  const json_start = start + I18N_MARKER.length;
  const json_end = value.indexOf("}", json_start) + 1;
  if (json_end === 0) return value;
  try {
    const metadata = JSON.parse(value.slice(json_start, json_end));
    if (metadata?.key) {
      const translated = translate(metadata.key);
      const result = translated !== metadata.key ? translated : metadata.key;
      return value.slice(0, start) + result + value.slice(json_end);
    }
  } catch {
  }
  return value;
}
class Gradio {
  load_component;
  shared = {};
  props = {};
  i18n = (v) => v;
  translatable_props = {};
  dispatcher;
  last_update = null;
  shared_props = allowed_shared_props;
  register_component;
  constructor(_props, default_values) {
    for (const key in _props.shared_props) {
      this.shared[key] = _props.shared_props[key];
    }
    for (const key in _props.props) {
      this.props[key] = _props.props[key];
    }
    if (default_values) {
      for (const key in default_values) {
        if (this.props[key] === void 0) {
          this.props[key] = default_values[key];
        }
      }
    }
    this.i18n = this.props.i18n ?? ((v) => v);
    for (const key of TRANSLATABLE_PROPS) {
      this.shared[key] = this._translate_and_store(
        "shared",
        key,
        // @ts-ignore
        _props.shared_props[key]
      );
      this.props[key] = this._translate_and_store(
        "props",
        key,
        // @ts-ignore
        _props.props[key]
      );
    }
    this.load_component = this.shared.load_component;
    if (!is_browser || _props.props?.__GRADIO_BROWSER_TEST__) {
      this.dispatcher = () => {
      };
      return;
    }
    this.register_component = this.shared.register_component || (() => {
    });
    this.dispatcher = this.shared.dispatcher || (() => {
    });
    this.register_component(
      _props.shared_props.id,
      // @ts-ignore
      this.set_data.bind(this),
      this.get_data.bind(this)
    );
    if (Object.keys(this.translatable_props).length > 0) {
      $locale.subscribe(() => {
        for (const [full_key, original] of Object.entries(this.translatable_props)) {
          const [target, key] = full_key.split(".");
          const translated = this.i18n(original);
          if (target === "shared") this.shared[key] = translated;
          else this.props[key] = translated;
        }
      });
    }
  }
  // check if props are translatable
  _is_i18n_managed(key, new_value) {
    const original_marker = this.translatable_props[key];
    if (!original_marker) return false;
    if (new_value === original_marker) return true;
    delete this.translatable_props[key];
    return false;
  }
  _translate_and_store(target, key, value) {
    if (typeof value !== "string") return value;
    const translated = this.i18n(value);
    if (translated !== value) {
      this.translatable_props[`${target}.${key}`] = value;
    }
    return translated;
  }
  dispatch(event_name, data) {
    this.dispatcher(this.shared.id, event_name, data);
  }
  async get_data() {
    return snapshot(this.props);
  }
  update(data) {
    this.set_data(data);
  }
  set_data(data) {
    for (const key in data) {
      const value = data[key];
      const translated = has_i18n_marker(value) ? this._translate_and_store(this.shared_props.includes(key) ? "shared" : "props", key, value) : value;
      if (this.shared_props.includes(key)) {
        const _key = key;
        this.shared[_key] = translated;
        continue;
      }
      this.props[key] = translated;
    }
  }
}
const css_units = (dimension_value) => {
  return typeof dimension_value === "number" ? dimension_value + "px" : dimension_value;
};

export { Gradio as G, I18N_MARKER as I, ShareError as S, allowed_shared_props as a, css_units as c, format_time as f, translate_i18n_marker as t, uploadToHuggingFace as u };
//# sourceMappingURL=utils.svelte-DcXWObof.js.map
