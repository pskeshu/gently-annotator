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

      this.savedRotation = { x: -0.5, y: 0.5 };
      this.savedZoom = 0.9;
      this.threshold = 30;          // 0–100, scaled to 0–1 in shader
      this.contrast = 1.0;          // 0.5–3.0
      this.isDragging = false;
      this.prevMouse = { x: 0, y: 0 };
      this.animationId = null;

      this._cameraObjectPos = null;
      this._resizeObserver = null;
      this._mounted = false;
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

      // sliceGroup is the rotated group; the volume cube is added here.
      // scale.y = -1 flips the volume so its orientation matches the 2D
      // dual_view projection (matching gently's viewer).
      this.volumeGroup = new THREE.Group();
      this.volumeGroup.rotation.x = this.savedRotation.x;
      this.volumeGroup.rotation.y = this.savedRotation.y;
      this.volumeGroup.scale.y = -1;
      this.scene.add(this.volumeGroup);

      this._cameraObjectPos = new THREE.Vector3();
      this._installInteractions();
      this._installResize();
      this._startRenderLoop();
      this._mounted = true;
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

    resetView() {
      this.savedRotation = { x: -0.5, y: 0.5 };
      this.savedZoom = 0.9;
      if (this.volumeGroup) {
        this.volumeGroup.rotation.x = this.savedRotation.x;
        this.volumeGroup.rotation.y = this.savedRotation.y;
        this.volumeGroup.rotation.z = 0;
      }
      if (this.camera) this.camera.position.z = this.savedZoom;
    }

    _installInteractions() {
      const el = this.renderer.domElement;
      el.addEventListener("mousedown", (e) => {
        this.isDragging = true;
        this.prevMouse = { x: e.clientX, y: e.clientY };
      });
      el.addEventListener("mousemove", (e) => {
        if (!this.isDragging) return;
        const dx = e.clientX - this.prevMouse.x;
        const dy = e.clientY - this.prevMouse.y;
        if (e.shiftKey) {
          this.volumeGroup.rotation.z += dx * 0.01;
        } else {
          this.volumeGroup.rotation.y += dx * 0.01;
          this.volumeGroup.rotation.x += dy * 0.01;
        }
        this.savedRotation.x = this.volumeGroup.rotation.x;
        this.savedRotation.y = this.volumeGroup.rotation.y;
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
          // Force the matrix update before reading worldToLocal. Three.js
          // only updates matrixWorld during render(), so on the first frame
          // after a fresh mesh swap the worldToLocal would use an identity
          // matrix and the uniform would briefly point at the un-rotated
          // camera position — causing a 1-frame "camera reset" flicker.
          this.volumeMesh.updateMatrixWorld(true);
          // Camera position in the volume cube's local space (accounts
          // for the volumeGroup's rotation AND the Y scale flip).
          this._cameraObjectPos.copy(this.camera.position);
          this.volumeMesh.worldToLocal(this._cameraObjectPos);
          this.volumeMaterial.uniforms.uCameraObjectPos.value.copy(this._cameraObjectPos);
        }
        this.renderer.render(this.scene, this.camera);
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
