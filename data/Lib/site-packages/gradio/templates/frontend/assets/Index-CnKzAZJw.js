import { g as spread_props, i as if_block, k as each, r as rest_props, u as index, v as set_checked, b as set_class, a as set_attribute, t as remove_input_defaults } from './i18n-CGlVUOvE.js';
import { M as push, ad as user_effect, N as first_child, a3 as comment, a as append, Z as get, S as sibling, a2 as user_derived, O as pop, R as from_html, t as template_effect, X as set_text, U as event, Q as child, a4 as text, as as to_array, T as reset } from './index-Bq2njFOY.js';
import { G as Gradio, a as snapshot } from './utils.svelte-Bi9ASwv9.js';
import { B as Block } from './Block-DNK-JdLJ.js';
import { B as BlockTitle } from './BlockTitle-Bcj082QO.js';
import './ScrollFade.svelte_svelte_type_style_lang-D0JPkGar.js';
import './MarkdownCode.svelte_svelte_type_style_lang-B29zDiob.js';
import { I as IconButtonWrapper } from './IconButtonWrapper-DAsvkfJg.js';
import { S as Static } from './index-1GCmrSlD.js';
import './StreamingBar.svelte_svelte_type_style_lang-DWGn5tT_.js';
import { d as dequal } from './index-DnoGeqVF.js';
import './Info-CfqzbVCN.js';
import './MarkdownCode-BYKuypS7.js';
import './html-Bwif0JPw.js';
import './snippet-BG7qkY_1.js';
import './prism-python-NrKIQnfs.js';
import './Clear-C8Tfa7QP.js';

var root_4 = from_html(`<div class="select-all-container svelte-yb2gcx"><label class="select-all-label svelte-yb2gcx"><input class="select-all-checkbox svelte-yb2gcx" type="checkbox" title="Select/Deselect All"/></label> <button type="button" class="label-text svelte-yb2gcx"> </button></div>`);
var root_7 = from_html(`<label><input type="checkbox" class="svelte-yb2gcx"/> <span class="ml-2 svelte-yb2gcx"> </span></label>`);
var root_1 = from_html(`<!> <!> <!> <div class="wrap svelte-yb2gcx" data-testid="checkbox-group"></div>`, 1);

function Index($$anchor, $$props) {
	push($$props, true);

	let props = rest_props($$props, ['$$slots', '$$events', '$$legacy']);
	let gradio = new Gradio(props);

	function toggle_choice(choice) {
		if (gradio.props.value.includes(choice)) {
			gradio.props.value = gradio.props.value.filter((v) => v !== choice);
		} else {
			gradio.props.value = [...gradio.props.value, choice];
		}

		gradio.dispatch("input");
	}

	function toggle_select_all() {
		const all_values = gradio.props.choices.map(([, internal_value]) => internal_value);

		if (gradio.props.value.length === all_values.length) {
			gradio.props.value = [];
		} else {
			gradio.props.value = all_values.slice();
		}

		gradio.dispatch("input");
	}

	let select_all_state = user_derived(() => {
		const all_values = gradio.props.choices.map(([, internal_value]) => internal_value);

		if (gradio.props.value.length === 0) return "unchecked";
		if (gradio.props.value.length === all_values.length) return "checked";

		return "indeterminate";
	});

	let disabled = user_derived(() => !gradio.shared.interactive);
	let old_value = gradio.props.value;

	user_effect(() => {
		gradio.props.value;

		if (dequal(old_value, gradio.props.value)) {
			return;
		}

		old_value = gradio.props.value;
		gradio.dispatch("change", snapshot(gradio.props.value));
	});

	Block($$anchor, {
		get visible() {
			return gradio.shared.visible;
		},

		get elem_id() {
			return gradio.shared.elem_id;
		},

		get elem_classes() {
			return gradio.shared.elem_classes;
		},
		type: 'fieldset',
		get container() {
			return gradio.shared.container;
		},

		get scale() {
			return gradio.shared.scale;
		},

		get min_width() {
			return gradio.shared.min_width;
		},

		children: ($$anchor, $$slotProps) => {
			var fragment_1 = root_1();
			var node = first_child(fragment_1);

			Static(node, spread_props(
				{
					get autoscroll() {
						return gradio.shared.autoscroll;
					},

					get i18n() {
						return gradio.i18n;
					}
				},
				() => gradio.shared.loading_status,
				{
					on_clear_status: () => gradio.dispatch("clear_status", gradio.shared.loading_status)
				}
			));

			var node_1 = sibling(node, 2);

			{
				var consequent = ($$anchor) => {
					IconButtonWrapper($$anchor, {
						get buttons() {
							return gradio.props.buttons;
						},

						on_custom_button_click: (id) => {
							gradio.dispatch("custom_button_click", { id });
						}
					});
				};

				if_block(node_1, ($$render) => {
					if (gradio.shared.show_label && gradio.props.buttons && gradio.props.buttons.length > 0) $$render(consequent);
				});
			}

			var node_2 = sibling(node_1, 2);

			{
				let $0 = user_derived(() => gradio.shared.show_label || gradio.props.show_select_all && gradio.shared.interactive);

				BlockTitle(node_2, {
					get show_label() {
						return get($0);
					},

					get info() {
						return gradio.props.info;
					},

					children: ($$anchor, $$slotProps) => {
						var fragment_3 = comment();
						var node_3 = first_child(fragment_3);

						{
							var consequent_1 = ($$anchor) => {
								var div = root_4();
								var label = child(div);
								var input = child(label);

								remove_input_defaults(input);
								reset(label);

								var button = sibling(label, 2);
								var text = child(button, true);

								reset(button);
								reset(div);

								template_effect(() => {
									set_checked(input, get(select_all_state) === "checked");
									input.indeterminate = get(select_all_state) === "indeterminate";
									set_text(text, gradio.shared.show_label ? gradio.shared.label : "Select All");
								});

								event('change', input, toggle_select_all);
								event('click', button, toggle_select_all);
								append($$anchor, div);
							};

							var alternate = ($$anchor) => {
								var fragment_4 = comment();
								var node_4 = first_child(fragment_4);

								{
									var consequent_2 = ($$anchor) => {
										var text_1 = text();

										template_effect(($0) => set_text(text_1, $0), [
											() => gradio.shared.label || gradio.i18n("checkbox.checkbox_group")
										]);

										append($$anchor, text_1);
									};

									if_block(
										node_4,
										($$render) => {
											if (gradio.shared.show_label) $$render(consequent_2);
										},
										true
									);
								}

								append($$anchor, fragment_4);
							};

							if_block(node_3, ($$render) => {
								if (gradio.props.show_select_all && gradio.shared.interactive) $$render(consequent_1); else $$render(alternate, false);
							});
						}

						append($$anchor, fragment_3);
					},
					$$slots: { default: true }
				});
			}

			var div_1 = sibling(node_2, 2);

			each(div_1, 21, () => gradio.props.choices, index, ($$anchor, $$item, i) => {
				var $$array = user_derived(() => to_array(get($$item), 2));
				let display_value = () => get($$array)[0];
				let internal_value = () => get($$array)[1];
				var label_1 = root_7();
				let classes;
				var input_1 = child(label_1);

				remove_input_defaults(input_1);

				var span = sibling(input_1, 2);
				var text_2 = child(span, true);

				reset(span);
				reset(label_1);

				template_effect(
					($0, $1, $2, $3) => {
						classes = set_class(label_1, 1, 'svelte-yb2gcx', null, classes, $0);
						input_1.disabled = get(disabled);
						set_checked(input_1, $1);
						set_attribute(input_1, 'name', $2);
						set_attribute(input_1, 'title', $3);
						set_text(text_2, display_value());
					},
					[
						() => ({
							disabled: get(disabled),
							selected: gradio.props.value.includes(internal_value())
						}),
						() => gradio.props.value.includes(internal_value()),
						() => internal_value()?.toString(),
						() => internal_value()?.toString()
					]
				);

				event('change', input_1, () => toggle_choice(internal_value()));

				event('input', input_1, (evt) => gradio.dispatch("select", {
					index: i,
					value: internal_value(),
					selected: evt.currentTarget.checked
				}));

				event('keydown', input_1, (event) => {
					if (event.key === "Enter") {
						toggle_choice(internal_value());

						gradio.dispatch("select", {
							index: i,
							value: internal_value(),
							selected: !gradio.props.value.includes(internal_value())
						});
					}
				});

				append($$anchor, label_1);
			});

			reset(div_1);
			append($$anchor, fragment_1);
		},
		$$slots: { default: true }
	});

	pop();
}

export { Index as default };
//# sourceMappingURL=Index-CnKzAZJw.js.map
