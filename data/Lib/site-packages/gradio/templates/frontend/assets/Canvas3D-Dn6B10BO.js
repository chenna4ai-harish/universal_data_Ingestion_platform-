const __vite__mapDeps=(i,m=__vite__mapDeps,d=(m.f||(m.f=["./index-BeVTUo08.js","./index-Bq2njFOY.js","./index-JtiO_h80.css"])))=>i.map(i=>d[i]);
import { M as push, a8 as onMount, ad as user_effect, Z as get, a as append, O as pop, a2 as user_derived, ab as state, R as from_html, ag as __vitePreload, $ as set } from './index-Bq2njFOY.js';
import { d as bind_this } from './i18n-CGlVUOvE.js';

var root = from_html(`<canvas></canvas>`);

function Canvas3D($$anchor, $$props) {
	push($$props, true);

	let BABYLON_VIEWER;
	let url = user_derived(() => $$props.value.url);
	let canvas;
	let viewer = state(void 0);
	let viewerDetails = state(void 0);
	let mounted = state(false);

	onMount(() => {
		const initViewer = async () => {
			BABYLON_VIEWER = await __vitePreload(() => import('./index-BeVTUo08.js').then(n => n.cx),true              ?__vite__mapDeps([0,1,2]):void 0,import.meta.url);

			BABYLON_VIEWER.CreateViewerForCanvas(canvas, {
				clearColor: $$props.clear_color,
				useRightHandedSystem: true,
				animationAutoPlay: true,
				cameraAutoOrbit: { enabled: false },
				onInitialized: (details) => {
					set(viewerDetails, details, true);
				}
			}).then((promiseViewer) => {
				set(viewer, promiseViewer, true);
				set(mounted, true);
			});
		};

		initViewer();

		return () => {
			get(viewer)?.dispose();
		};
	});

	user_effect(() => {
		if (get(mounted)) {
			load_model(get(url));
		}
	});

	function setRenderingMode(pointsCloud, wireframe) {
		if (!get(viewerDetails)) return;

		get(viewerDetails).scene.forcePointsCloud = pointsCloud;
		get(viewerDetails).scene.forceWireframe = wireframe;
	}

	function load_model(url) {
		if (get(viewer)) {
			if (url) {
				get(viewer).loadModel(url, { pluginOptions: { obj: { importVertexColors: true } } }).then(() => {
					if ($$props.display_mode === "point_cloud") {
						setRenderingMode(true, false);
					} else if ($$props.display_mode === "wireframe") {
						setRenderingMode(false, true);
					} else {
						update_camera($$props.camera_position, $$props.zoom_speed, $$props.pan_speed);
					}
				});
			} else {
				get(viewer).resetModel();
			}
		}
	}

	function update_camera(camera_position, zoom_speed, pan_speed) {
		if (!get(viewerDetails)) return;

		const camera = get(viewerDetails).camera;

		if (camera_position[0] !== null) {
			camera.alpha = camera_position[0] * Math.PI / 180;
		}

		if (camera_position[1] !== null) {
			camera.beta = camera_position[1] * Math.PI / 180;
		}

		if (camera_position[2] !== null) {
			camera.radius = camera_position[2];
		}

		camera.lowerRadiusLimit = 0.1;

		const updateCameraSensibility = () => {
			camera.wheelPrecision = 250 / (camera.radius * zoom_speed);
			camera.panningSensibility = 10000 * pan_speed / camera.radius;
		};

		updateCameraSensibility();
		camera.onAfterCheckInputsObservable.add(updateCameraSensibility);
	}

	function reset_camera_position() {
		if (get(viewerDetails) && get(viewer)) {
			get(viewer).resetCamera();
		}
	}

	var $$exports = { update_camera, reset_camera_position };
	var canvas_1 = root();

	bind_this(canvas_1, ($$value) => canvas = $$value, () => canvas);
	append($$anchor, canvas_1);

	return pop($$exports);
}

export { Canvas3D as default };
//# sourceMappingURL=Canvas3D-Dn6B10BO.js.map
