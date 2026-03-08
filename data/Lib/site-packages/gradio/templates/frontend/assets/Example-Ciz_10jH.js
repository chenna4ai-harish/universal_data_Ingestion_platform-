import { p as prop, k as each, d as bind_this, u as index, i as if_block, b as set_class, a as set_attribute } from './i18n-CGlVUOvE.js';
import { M as push, ad as user_effect, t as template_effect, a as append, O as pop, S as sibling, $ as set, R as from_html, Q as child, ab as state, a3 as comment, N as first_child, Z as get, X as set_text, T as reset, a4 as text } from './index-Bq2njFOY.js';
import { b as bind_element_size } from './size-CWi277d_.js';
import { I as Image } from './Image-XuMqTTwB.js';
import './ScrollFade.svelte_svelte_type_style_lang-D0JPkGar.js';
import './MarkdownCode.svelte_svelte_type_style_lang-B29zDiob.js';
/* empty css                                                    */
import { V as Video } from './Video-e9Pn2mbW.js';
import './misc-D8a4ZbMA.js';
/* empty css                                             */
import './snippet-BG7qkY_1.js';
import './prism-python-NrKIQnfs.js';
import './actions-CgRQ2lHA.js';

var root_6 = from_html(`<audio controls></audio>`);
var root = from_html(`<div><p> </p> <!></div>`);

function Example($$anchor, $$props) {
	push($$props, true);

	let value = prop($$props, 'value', 19, () => ({ text: "", files: [] })),
		selected = prop($$props, 'selected', 3, false);

	let size = state(0);
	let el;

	function set_styles(element, el_width) {
		element.style.setProperty("--local-text-width", `${el_width && el_width < 150 ? el_width : 200}px`);
		element.style.whiteSpace = "unset";
	}

	user_effect(() => {
		if (el && get(size)) {
			set_styles(el, get(size));
		}
	});

	var div = root();
	let classes;
	var p = child(div);
	var text$1 = child(p, true);

	reset(p);

	var node = sibling(p, 2);

	each(node, 17, () => value().files, index, ($$anchor, file) => {
		var fragment = comment();
		var node_1 = first_child(fragment);

		{
			var consequent = ($$anchor) => {
				Image($$anchor, {
					get src() {
						return get(file).url;
					},
					alt: ''
				});
			};

			var alternate_2 = ($$anchor) => {
				var fragment_2 = comment();
				var node_2 = first_child(fragment_2);

				{
					var consequent_1 = ($$anchor) => {
						Video($$anchor, {
							get src() {
								return get(file).url;
							},
							alt: '',
							loop: true,
							is_stream: false
						});
					};

					var alternate_1 = ($$anchor) => {
						var fragment_4 = comment();
						var node_3 = first_child(fragment_4);

						{
							var consequent_2 = ($$anchor) => {
								var audio = root_6();

								template_effect(() => set_attribute(audio, 'src', get(file).url));
								append($$anchor, audio);
							};

							var alternate = ($$anchor) => {
								var text_1 = text();

								template_effect(() => set_text(text_1, get(file).orig_name));
								append($$anchor, text_1);
							};

							if_block(
								node_3,
								($$render) => {
									if (get(file).mime_type && get(file).mime_type.includes("audio")) $$render(consequent_2); else $$render(alternate, false);
								},
								true
							);
						}

						append($$anchor, fragment_4);
					};

					if_block(
						node_2,
						($$render) => {
							if (get(file).mime_type && get(file).mime_type.includes("video")) $$render(consequent_1); else $$render(alternate_1, false);
						},
						true
					);
				}

				append($$anchor, fragment_2);
			};

			if_block(node_1, ($$render) => {
				if (get(file).mime_type && get(file).mime_type.includes("image")) $$render(consequent); else $$render(alternate_2, false);
			});
		}

		append($$anchor, fragment);
	});

	reset(div);
	bind_this(div, ($$value) => el = $$value, () => el);

	template_effect(() => {
		classes = set_class(div, 1, 'container svelte-xz0m7l', null, classes, {
			table: $$props.type === "table",
			gallery: $$props.type === "gallery",
			selected: selected(),
			border: value()
		});

		set_text(text$1, value().text ? value().text : "");
	});

	bind_element_size(div, 'clientWidth', ($$value) => set(size, $$value));
	append($$anchor, div);
	pop();
}

export { Example as default };
//# sourceMappingURL=Example-Ciz_10jH.js.map
