/* 3D volume viewer — raymarched through a Data3DTexture.
 *
 * Lifted from gently/ui/web/static/js/projection-viewer.js (origin/main,
 * commit cf15449 "raymarch through a Data3DTexture"). Adapted to be a
 * reusable Viewer3D class instead of a global singleton, and to mount
 * inside an arbitrary container element.
 *
 * Requires WebGL2 (Three.js r128 with THREE.DataTexture3D + GLSL3). On
 * a WebGL1-only browser, mount() shows a clear error message instead of
 * silently producing a blank canvas.
 */

(function () {
  function hasWebGL2() {
    try {
      const c = document.createElement("canvas");
      return !!c.getContext("webgl2");
    } catch (e) {
      return false;
    }
  }

  function decodeBase64ToUint8(b64) {
    const bin = atob(b64);
    const out = new Uint8Array(bin.length);
    for (let i = 0; i < bin.length; i++) out[i] = bin.charCodeAt(i);
    return out;
  }

  const VERTEX_SHADER = `
    out vec3 vObjectPos;
    void main() {
      vObjectPos = position;
      gl_Position = projectionMatrix * modelViewMatrix * vec4(position, 1.0);
    }
  `;

  // Front-to-back raymarching fragment shader — works at any camera
  // angle because it samples the 3D texture continuously along the
  // view ray using hardware trilinear interpolation. No slice planes
  // to go edge-on, no gaps between samples.
  const FRAGMENT_SHADER = `
    precision highp float;
    precision highp sampler3D;

    uniform sampler3D uVolume;
    uniform vec3 uBoxSize;
    uniform float uThreshold;
    uniform float uContrast;
    uniform vec3 uCameraObjectPos;
    uniform int uMaxSteps;

    in vec3 vObjectPos;
    out vec4 outColor;

    bool rayBoxIntersect(vec3 ro, vec3 rd, vec3 boxMin, vec3 boxMax,
                         out float tMin, out float tMax) {
      vec3 invD = 1.0 / rd;
      vec3 t1 = (boxMin - ro) * invD;
      vec3 t2 = (boxMax - ro) * invD;
      vec3 tmn = min(t1, t2);
      vec3 tmx = max(t1, t2);
      tMin = max(max(tmn.x, tmn.y), tmn.z);
      tMax = min(min(tmx.x, tmx.y), tmx.z);
      return tMax > max(tMin, 0.0);
    }

    // Hash for per-pixel jitter. Without this, the fixed step size
    // beats against the voxel grid and produces visible "wood grain"
    // bands.
    float hash12(vec2 p) {
      vec3 p3 = fract(vec3(p.xyx) * 0.1031);
      p3 += dot(p3, p3.yzx + 33.33);
      return fract((p3.x + p3.y) * p3.z);
    }

    void main() {
      vec3 boxHalf = uBoxSize * 0.5;
      vec3 ro = uCameraObjectPos;
      vec3 rd = normalize(vObjectPos - uCameraObjectPos);

      float tMin, tMax;
      if (!rayBoxIntersect(ro, rd, -boxHalf, boxHalf, tMin, tMax)) discard;
      tMin = max(tMin, 0.0);

      float totalLen = tMax - tMin;
      float stepSize = totalLen / float(uMaxSteps);
      float jitter = hash12(gl_FragCoord.xy) * stepSize;
      vec3 pos = ro + rd * (tMin + jitter);
      vec3 step = rd * stepSize;

      const float NOMINAL_STEPS = 192.0;
      float opacityScale = NOMINAL_STEPS / float(uMaxSteps);

      vec4 accum = vec4(0.0);
      for (int i = 0; i < 512; i++) {
        if (i >= uMaxSteps) break;
        vec3 uvw = (pos + boxHalf) / uBoxSize;
        if (any(lessThan(uvw, vec3(0.0))) || any(greaterThan(uvw, vec3(1.0)))) {
          pos += step;
          continue;
        }
        float sampleVal = texture(uVolume, uvw).r;
        // Smooth transfer function — no quantization rings.
        float density = smoothstep(uThreshold, min(uThreshold + 0.45, 1.0), sampleVal);
        if (density > 0.001) {
          float v = clamp((sampleVal - 0.5) * uContrast + 0.5, 0.0, 1.0);
          vec3 color = vec3(v);
          // Per-step opacity is intentionally LOW so density accumulates
          // smoothly over many samples instead of saturating in one or
          // two steps (which produces black-centered concentric rings).
          float alpha = density * 0.18 * opacityScale;
          accum.rgb += (1.0 - accum.a) * color * alpha;
          accum.a += (1.0 - accum.a) * alpha;
        }
        pos += step;
        if (accum.a > 0.999) break;
      }
      if (accum.a < 0.005) discard;
      outColor = accum;
    }
  `;

  class Viewer3D {
    constructor(container) {
      this.container = container;
      this.scene = null;
      this.camera = null;
      this.renderer = null;
      this.volumeGroup = null;
      this.volumeMesh = null;
      this.volumeMaterial = null;
      this.volumeTexture3D = null;

      // Accumulated rotation as a quaternion. Quaternion multiplication is
      // commutative-free and avoids the gimbal lock you get from accumulating
      // Euler angles. The default matches the old (rotation.x=-0.5, .y=0.5,
      // order XYZ) so existing volumes look the same after the upgrade.
      this.DEFAULT_QUATERNION = new THREE.Quaternion().setFromEuler(
        new THREE.Euler(-0.5, 0.5, 0, "XYZ")
      );
      this.savedQuaternion = this.DEFAULT_QUATERNION.clone();
      this.savedZoom = 0.9;
      this.threshold = 30;          // 0–100, scaled to 0–1 in shader
      this.contrast = 1.0;          // 0.5–3.0
      this.isDragging = false;
      this.prevMouse = { x: 0, y: 0 };
      this.animationId = null;

      this._cameraObjectPos = null;
      this._resizeObserver = null;
      this._mounted = false;

      // Orientation gizmo lives in a separate scene/camera so it can be
      // rendered as a small corner inset rather than overlaid on the volume.
      this._gizmoScene = null;
      this._gizmoCamera = null;
      this._gizmoGroup = null;     // rotates/flips like volumeGroup each frame
      this._gizmoArrows = null;    // sub-group holding the actual arrow meshes
      this._axesVisible = true;
    }

    mount() {
      if (this._mounted) return true;
      if (!hasWebGL2()) {
        this.container.innerHTML =
          `<div class="viewer-status error">WebGL2 not available.\nUse Chrome / Firefox / Edge.</div>`;
        return false;
      }

      const w = this.container.clientWidth || 600;
      const h = this.container.clientHeight || 400;

      this.scene = new THREE.Scene();
      this.camera = new THREE.PerspectiveCamera(50, w / h, 0.1, 100);
      this.camera.position.z = this.savedZoom;

      this.renderer = new THREE.WebGLRenderer({ antialias: true });
      this.renderer.setSize(w, h);
      this.renderer.setClearColor(0x000000);
      this.container.innerHTML = "";
      this.container.appendChild(this.renderer.domElement);

      // volumeGroup carries the user's accumulated rotation (as a quaternion)
      // and a Y-flip so the embryo orientation matches the 2D dual_view
      // projection (matching gently's viewer).
      this.volumeGroup = new THREE.Group();
      this.volumeGroup.quaternion.copy(this.savedQuaternion);
      this.volumeGroup.scale.y = -1;
      this.scene.add(this.volumeGroup);

      // Tiny separate scene for the AP/DV/LR axis gizmo. Rendered after the
      // main scene as an overlay viewport in the bottom-right corner. Same
      // rotation/scale as volumeGroup so the axes track the user's view.
      this._gizmoScene = new THREE.Scene();
      this._gizmoCamera = new THREE.PerspectiveCamera(50, 1, 0.1, 10);
      this._gizmoCamera.position.z = 2.5;
      this._gizmoGroup = new THREE.Group();
      this._gizmoArrows = new THREE.Group();
      this._gizmoGroup.add(this._gizmoArrows);
      this._gizmoScene.add(this._gizmoGroup);

      this._cameraObjectPos = new THREE.Vector3();
      this._installInteractions();
      this._installResize();
      this._startRenderLoop();
      this._mounted = true;
      // Show the empty reference axes from the start.
      this.setOrientationAxes({ ap: null, dv: null });
      return true;
    }

    setVolume({ data, shape, voxelSizeUm }) {
      // data: Uint8Array | base64 string. shape: [zd, h, w]. voxelSizeUm: [dz, dy, dx].
      //
      // Atomic swap: build the new mesh fully, replace the live references,
      // THEN dispose the old. Doing dispose-first leaves the scene empty for
      // a frame and shows a black flicker between timepoints.
      if (!this._mounted) return;
      const bytes = data instanceof Uint8Array ? data : decodeBase64ToUint8(data);

      const [zd, h, w] = shape;
      if (bytes.length !== zd * h * w) {
        console.error(`Volume size mismatch: bytes=${bytes.length}, expected=${zd * h * w}`);
        return;
      }

      // 3D texture upload. RedFormat + UnsignedByte = 1 byte per voxel,
      // hardware trilinear filtering. Data layout matches numpy
      // (z*h*w + y*w + x); GLSL sampler3D coords are (x, y, z).
      const tex3d = new THREE.DataTexture3D(bytes, w, h, zd);
      tex3d.format = THREE.RedFormat;
      tex3d.type = THREE.UnsignedByteType;
      tex3d.minFilter = THREE.LinearFilter;
      tex3d.magFilter = THREE.LinearFilter;
      tex3d.wrapR = THREE.ClampToEdgeWrapping;
      tex3d.wrapS = THREE.ClampToEdgeWrapping;
      tex3d.wrapT = THREE.ClampToEdgeWrapping;
      tex3d.unpackAlignment = 1;
      tex3d.needsUpdate = true;

      // Physical extents normalized to the largest axis so the cube
      // fits inside a unit sphere. Without this you get a Z-squished
      // embryo (default diSPIM voxel ratio is 1:0.1625:0.1625).
      const [dz, dy, dx] = voxelSizeUm || [1.0, 0.1625, 0.1625];
      const xExtent = w * dx;
      const yExtent = h * dy;
      const zExtent = zd * dz;
      const maxExtent = Math.max(xExtent, yExtent, zExtent);
      const boxW = xExtent / maxExtent;
      const boxH = yExtent / maxExtent;
      const boxD = zExtent / maxExtent;
      const boxSize = new THREE.Vector3(boxW, boxH, boxD);

      const material = new THREE.ShaderMaterial({
        glslVersion: THREE.GLSL3,
        uniforms: {
          uVolume: { value: tex3d },
          uBoxSize: { value: boxSize },
          uThreshold: { value: this.threshold / 255.0 },
          uContrast: { value: this.contrast },
          uCameraObjectPos: { value: new THREE.Vector3() },
          uMaxSteps: { value: 256 },
        },
        vertexShader: VERTEX_SHADER,
        fragmentShader: FRAGMENT_SHADER,
        transparent: true,
        side: THREE.BackSide,    // Render back faces — rays always start inside the cube.
        depthWrite: false,
      });

      const geo = new THREE.BoxGeometry(boxW, boxH, boxD);
      const mesh = new THREE.Mesh(geo, material);

      // --- Atomic swap ---
      const oldMesh = this.volumeMesh;
      const oldMaterial = this.volumeMaterial;
      const oldTexture = this.volumeTexture3D;

      this.volumeGroup.add(mesh);
      this.volumeMesh = mesh;
      this.volumeMaterial = material;
      this.volumeTexture3D = tex3d;

      if (oldMesh) {
        this.volumeGroup.remove(oldMesh);
        oldMesh.geometry?.dispose();
      }
      if (oldMaterial) oldMaterial.dispose();
      if (oldTexture) oldTexture.dispose();
    }

    setThreshold(t) {
      this.threshold = t;
      if (this.volumeMaterial) {
        this.volumeMaterial.uniforms.uThreshold.value = t / 255.0;
      }
    }

    setContrast(c) {
      this.contrast = c;
      if (this.volumeMaterial) {
        this.volumeMaterial.uniforms.uContrast.value = c;
      }
    }

    /** Return the world "up" direction transformed into the volumeGroup's
     *  local frame as a unit Vector3. Used to capture an orientation axis
     *  at the moment the user clicks "Save AP/DV". */
    captureLocalUp() {
      if (!this.volumeGroup) return null;
      this.volumeGroup.updateMatrixWorld(true);
      const p1 = new THREE.Vector3(0, 0, 0);
      const p2 = new THREE.Vector3(0, 1, 0);
      this.volumeGroup.worldToLocal(p1);
      this.volumeGroup.worldToLocal(p2);
      const dir = p2.sub(p1);
      const len = dir.length();
      if (len < 1e-6) return null;
      return [dir.x / len, dir.y / len, dir.z / len];
    }

    /** Replace the orientation axis gizmo. ap/dv are unit-vector arrays in
     *  volume-local coords, or null. The arrows live in a separate
     *  gizmo scene (rendered as a small corner overlay) — NOT overlaid on
     *  the volume itself. The gizmo group's rotation is synced to the
     *  volumeGroup each animate frame so the axes track the user's view. */
    setOrientationAxes({ ap, dv }) {
      if (!this._gizmoArrows) return;
      while (this._gizmoArrows.children.length) {
        const c = this._gizmoArrows.children[0];
        c.traverse?.((o) => { o.geometry?.dispose(); o.material?.dispose(); });
        this._gizmoArrows.remove(c);
      }
      // Three short reference axes (gray, length 1) for context — so even
      // an empty gizmo box shows the camera frame.
      this._gizmoArrows.add(this._makeRefAxes());

      if (ap) this._gizmoArrows.add(this._makeAxisArrow(ap, "#ff5577", "A"));
      if (dv) this._gizmoArrows.add(this._makeAxisArrow(dv, "#56d364", "D"));
      if (ap && dv) {
        const apV = new THREE.Vector3(...ap);
        const dvV = new THREE.Vector3(...dv);
        const lr = new THREE.Vector3().crossVectors(apV, dvV).normalize();
        this._gizmoArrows.add(this._makeAxisArrow([lr.x, lr.y, lr.z], "#79c0ff", "L"));
      }
      this._gizmoArrows.visible = !!this._axesVisible;
    }

    setAxesVisible(v) {
      this._axesVisible = !!v;
      if (this._gizmoArrows) this._gizmoArrows.visible = this._axesVisible;
    }

    /** A faint world-axes reference (X red-ish, Y green-ish, Z blue-ish)
     *  shown inside the gizmo so even an empty embryo has a frame to read. */
    _makeRefAxes() {
      const g = new THREE.Group();
      const len = 0.95;
      const refMat = (c) => new THREE.LineBasicMaterial({
        color: c, transparent: true, opacity: 0.18, depthTest: false,
      });
      const seg = (a, b, mat) => {
        const geo = new THREE.BufferGeometry().setFromPoints([a, b]);
        return new THREE.Line(geo, mat);
      };
      g.add(seg(new THREE.Vector3(-len, 0, 0), new THREE.Vector3(len, 0, 0), refMat(0x888888)));
      g.add(seg(new THREE.Vector3(0, -len, 0), new THREE.Vector3(0, len, 0), refMat(0x888888)));
      g.add(seg(new THREE.Vector3(0, 0, -len), new THREE.Vector3(0, 0, len), refMat(0x888888)));
      return g;
    }

    _makeAxisArrow(dir, color, label) {
      const g = new THREE.Group();
      // Gizmo coords: arrow length ~0.95 fits the gizmo viewport nicely.
      const ARROW_LEN = 0.9;
      const TIP_LEN = 0.18;
      const TIP_RADIUS = 0.07;
      const v = new THREE.Vector3(...dir).normalize();

      const shaftGeo = new THREE.BufferGeometry().setFromPoints([
        new THREE.Vector3(0, 0, 0),
        v.clone().multiplyScalar(ARROW_LEN - TIP_LEN),
      ]);
      const shaftMat = new THREE.LineBasicMaterial({ color, depthTest: false, transparent: true, opacity: 0.95 });
      g.add(new THREE.Line(shaftGeo, shaftMat));

      const coneGeo = new THREE.ConeGeometry(TIP_RADIUS, TIP_LEN, 14);
      const coneMat = new THREE.MeshBasicMaterial({ color, depthTest: false, transparent: true, opacity: 0.95 });
      const cone = new THREE.Mesh(coneGeo, coneMat);
      cone.position.copy(v).multiplyScalar(ARROW_LEN - TIP_LEN / 2);
      cone.quaternion.setFromUnitVectors(new THREE.Vector3(0, 1, 0), v);
      g.add(cone);
      g.userData.label = label;
      return g;
    }

    resetView() {
      this.savedQuaternion.copy(this.DEFAULT_QUATERNION);
      this.savedZoom = 0.9;
      if (this.volumeGroup) this.volumeGroup.quaternion.copy(this.savedQuaternion);
      if (this.camera) this.camera.position.z = this.savedZoom;
    }

    /** Snap to a canonical view direction, looking along ±X / ±Y / ±Z in
     *  the volume's local frame. Useful for "show me the front", "now from
     *  the top", etc. without having to orbit by hand. The argument is a
     *  string like 'front' | 'back' | 'top' | 'bottom' | 'left' | 'right'.
     *
     *  Mapping (local axes after the Y-flip already applied to volumeGroup):
     *   front  = looking along -Z in volume local
     *   back   = looking along +Z
     *   top    = looking along -Y
     *   bottom = looking along +Y
     *   right  = looking along -X
     *   left   = looking along +X
     */
    snapView(name) {
      // We need a quaternion such that the chosen local axis becomes the
      // camera's -Z (i.e. points TOWARD the camera). The camera looks down
      // -Z in world space, so the local axis we want to face the camera
      // should map to world +Z. Three.js Quaternion.setFromUnitVectors(a, b)
      // returns the rotation that rotates `a` onto `b`.
      const target = new THREE.Vector3();
      switch (name) {
        case "front":  target.set(0, 0, 1);  break;
        case "back":   target.set(0, 0, -1); break;
        case "top":    target.set(0, 1, 0);  break;
        case "bottom": target.set(0, -1, 0); break;
        case "right":  target.set(1, 0, 0);  break;
        case "left":   target.set(-1, 0, 0); break;
        default: return;
      }
      const worldZ = new THREE.Vector3(0, 0, 1);
      const q = new THREE.Quaternion().setFromUnitVectors(target, worldZ);
      this.savedQuaternion.copy(q);
      if (this.volumeGroup) this.volumeGroup.quaternion.copy(q);
    }

    _installInteractions() {
      const el = this.renderer.domElement;

      // Trackball-style drag: each mousemove builds a tiny rotation in
      // SCREEN space (camera-aligned axes) and pre-multiplies it onto the
      // accumulated quaternion. Pre-multiplying applies the new rotation in
      // world frame, which — since the camera is fixed at +Z looking at
      // origin — IS the screen frame. So drag-right always rotates the
      // volume "around screen-Y" no matter how it's already oriented:
      // no gimbal lock, no twisty surprises near the poles.
      const SENS = 0.005;
      const X_AXIS = new THREE.Vector3(1, 0, 0);
      const Y_AXIS = new THREE.Vector3(0, 1, 0);
      const Z_AXIS = new THREE.Vector3(0, 0, 1);
      const _q = new THREE.Quaternion();

      el.addEventListener("mousedown", (e) => {
        this.isDragging = true;
        this.prevMouse = { x: e.clientX, y: e.clientY };
      });
      el.addEventListener("mousemove", (e) => {
        if (!this.isDragging) return;
        const dx = (e.clientX - this.prevMouse.x) * SENS;
        const dy = (e.clientY - this.prevMouse.y) * SENS;
        if (e.shiftKey) {
          // Roll: rotate around the camera's view axis (screen Z).
          _q.setFromAxisAngle(Z_AXIS, -dx);
          this.volumeGroup.quaternion.premultiply(_q);
        } else {
          // Yaw + pitch in screen frame.
          _q.setFromAxisAngle(Y_AXIS, dx);
          this.volumeGroup.quaternion.premultiply(_q);
          _q.setFromAxisAngle(X_AXIS, dy);
          this.volumeGroup.quaternion.premultiply(_q);
        }
        this.savedQuaternion.copy(this.volumeGroup.quaternion);
        this.prevMouse = { x: e.clientX, y: e.clientY };
      });
      window.addEventListener("mouseup", () => (this.isDragging = false));
      el.addEventListener("wheel", (e) => {
        e.preventDefault();
        const z = this.camera.position.z + e.deltaY * 0.002;
        this.camera.position.z = Math.max(0.5, Math.min(5, z));
        this.savedZoom = this.camera.position.z;
      }, { passive: false });
      el.addEventListener("dblclick", () => this.resetView());
    }

    _installResize() {
      const onResize = () => {
        if (!this.renderer || !this.camera) return;
        const w = this.container.clientWidth || 600;
        const h = this.container.clientHeight || 400;
        // updateStyle=true (default) keeps the canvas CSS box in sync with
        // the drawing buffer. Without this the canvas's style.width/height
        // stay at their initial values, the browser stretches the (smaller)
        // buffer into the (larger) box, and the volume drifts off-center.
        this.renderer.setSize(w, h);
        this.camera.aspect = w / h;
        this.camera.updateProjectionMatrix();
      };
      this._resizeObserver = new ResizeObserver(onResize);
      this._resizeObserver.observe(this.container);
    }

    _startRenderLoop() {
      const animate = () => {
        this.animationId = requestAnimationFrame(animate);
        if (this.volumeMesh && this.volumeMaterial) {
          this.volumeMesh.updateMatrixWorld(true);
          this._cameraObjectPos.copy(this.camera.position);
          this.volumeMesh.worldToLocal(this._cameraObjectPos);
          this.volumeMaterial.uniforms.uCameraObjectPos.value.copy(this._cameraObjectPos);
        }
        // Main scene: full canvas viewport, full clear.
        const w = this.container.clientWidth || 1;
        const h = this.container.clientHeight || 1;
        this.renderer.setViewport(0, 0, w, h);
        this.renderer.setScissor(0, 0, w, h);
        this.renderer.setScissorTest(false);
        this.renderer.autoClear = true;
        this.renderer.render(this.scene, this.camera);

        // Gizmo overlay: small bottom-right inset. Sync rotation to the
        // volumeGroup each frame so the axes track the user's view.
        if (this._gizmoGroup && this.volumeGroup) {
          this._gizmoGroup.quaternion.copy(this.volumeGroup.quaternion);
          this._gizmoGroup.scale.copy(this.volumeGroup.scale);
        }
        const SIZE = Math.min(110, Math.max(70, Math.floor(Math.min(w, h) * 0.16)));
        const MARGIN = 12;
        const gx = MARGIN;            // bottom-left
        const gy = MARGIN;  // y is from bottom-left in WebGL viewport coords
        this.renderer.setViewport(gx, gy, SIZE, SIZE);
        this.renderer.setScissor(gx, gy, SIZE, SIZE);
        this.renderer.setScissorTest(true);
        this.renderer.autoClear = false;
        this.renderer.clearDepth();
        this.renderer.render(this._gizmoScene, this._gizmoCamera);
        this.renderer.setScissorTest(false);
        this.renderer.autoClear = true;
      };
      animate();
    }

    _disposeVolume() {
      if (this.volumeMesh) {
        this.volumeMesh.geometry?.dispose();
        this.volumeGroup.remove(this.volumeMesh);
        this.volumeMesh = null;
      }
      if (this.volumeMaterial) {
        this.volumeMaterial.dispose();
        this.volumeMaterial = null;
      }
      if (this.volumeTexture3D) {
        this.volumeTexture3D.dispose();
        this.volumeTexture3D = null;
      }
    }

    dispose() {
      if (this.animationId) cancelAnimationFrame(this.animationId);
      this._disposeVolume();
      if (this._gizmoArrows) {
        this._gizmoArrows.traverse((o) => {
          o.geometry?.dispose();
          o.material?.dispose();
        });
        this._gizmoArrows = null;
      }
      this._gizmoGroup = null;
      this._gizmoScene = null;
      this._gizmoCamera = null;
      if (this._resizeObserver) this._resizeObserver.disconnect();
      if (this.renderer) {
        this.renderer.dispose();
        this.renderer = null;
      }
      this.scene = null;
      this.camera = null;
      this.volumeGroup = null;
      this._mounted = false;
    }
  }

  window.Viewer3D = Viewer3D;
})();
