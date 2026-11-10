import { afterAll, describe, expect, it } from "vitest";
import { deleteApp, initializeApp } from "firebase/app";
import * as firestoreLib from "firebase/firestore";
import driver, { escapeKey } from "../../src/drivers/firebase-firestore.ts";
import { testDriver } from "./utils.ts";
import { createStorage } from "../../src/index.ts";

// Runs against the Firestore emulator, e.g.:
//   npx firebase-tools emulators:exec --only firestore --project demo-unstorage "pnpm vitest run test/drivers/firebase-firestore"
const emulatorHost = process.env.FIRESTORE_EMULATOR_HOST;

describe.skipIf(!emulatorHost)("drivers: firebase-firestore", () => {
  const app = initializeApp({ projectId: "demo-unstorage" }, "unstorage-test");
  const firestore = firestoreLib.getFirestore(app);
  const [host, port] = (emulatorHost || "localhost:8080").split(":");
  firestoreLib.connectFirestoreEmulator(firestore, host!, Number(port));

  // A second client that keeps what it has fetched, as a persistent cache would.
  const cachingApp = initializeApp({ projectId: "demo-unstorage" }, "unstorage-test-cache");
  const cachingFirestore = firestoreLib.initializeFirestore(cachingApp, {
    localCache: firestoreLib.memoryLocalCache({
      garbageCollector: firestoreLib.memoryLruGarbageCollector({
        cacheSizeBytes: firestoreLib.CACHE_SIZE_UNLIMITED,
      }),
    }),
  });
  firestoreLib.connectFirestoreEmulator(cachingFirestore, host!, Number(port));

  afterAll(async () => {
    await Promise.all([
      firestoreLib.terminate(firestore),
      firestoreLib.terminate(cachingFirestore),
    ]);
    await Promise.all([deleteApp(app), deleteApp(cachingApp)]);
  });

  testDriver({
    driver: driver({ firestore, collection: "test", lib: firestoreLib }),
    additionalTests: (ctx) => {
      it("should throw error if no firestore is provided", async () => {
        await expect(() =>
          driver({ lib: firestoreLib } as any).getItem!("a", {}),
        ).rejects.toThrowError(
          "[unstorage] [firebase-firestore] Missing required option `firestore`.",
        );
      });
      it("stores keys that are not valid Firestore document ids", async () => {
        const keys = ["__name__", "a.b", "...x", "100%", "emoji:🥨"];
        for (const key of keys) await ctx.storage.setItem(key, key);
        for (const key of keys) expect(await ctx.storage.getItem(key)).toBe(key);
        expect(escapeKey("..")).toBe("%2E%2E");
        expect(decodeURIComponent(escapeKey("a/b.c_d%e"))).toBe("a/b.c_d%e");
      });
      it("gets more items than one `in` query allows", async () => {
        const keys = Array.from({ length: 45 }, (_, n) => `many:${n}`);
        await ctx.storage.setItems(keys.map((key, n) => ({ key, value: n })));
        const items = await ctx.storage.getItems([...keys, "many:missing"]);
        expect(items[44]).toMatchObject({ key: "many:44", value: 44 });
        expect(items[45]).toMatchObject({ key: "many:missing", value: null });
      });
      it("clears only keys under a base", async () => {
        await ctx.storage.setItems([
          { key: "a:1", value: 1 },
          { key: "b:1", value: 2 },
        ]);
        await ctx.driver.clear!("b:", {});
        expect(await ctx.storage.getKeys()).toMatchObject(["a:1"]);
      });
      it("has mtime in meta", async () => {
        await ctx.storage.setItem("dated", 1);
        expect((await ctx.storage.getMeta("dated")).mtime).toBeInstanceOf(Date);
      });
      it("watches changes", async () => {
        await ctx.storage.setItem("existing", 1);
        const events: string[] = [];
        const unwatch = await ctx.storage.watch((event, key) => events.push(`${event} ${key}`));
        await ctx.storage.setItem("fresh", 2);
        await ctx.storage.removeItem("existing");
        await expect.poll(() => events).toEqual(["update fresh", "remove existing"]);
        await unwatch();
      });
      it("answers reads from the listener while watching, kept current by other clients", async () => {
        await ctx.storage.setItems([
          { key: "log:1", value: 1 },
          { key: "other", value: 2 },
        ]);
        const unwatch = await ctx.storage.watch(() => {});
        expect(await ctx.storage.getKeys("log")).toEqual(["log:1"]);
        const otherClient = createStorage({
          driver: driver({ firestore, collection: "test", lib: firestoreLib }),
        });
        await otherClient.setItem("log:2", 3);
        await expect.poll(() => ctx.storage.getItem("log:2")).toBe(3);
        expect(await ctx.storage.getItems(["log:1", "log:3"])).toMatchObject([
          { key: "log:1", value: 1 },
          { key: "log:3", value: null },
        ]);
        await unwatch();
        expect(await ctx.storage.getItem("log:2")).toBe(3);
      });
      it("incrementally, a reopened client starts from its cache and still gets what is new", async () => {
        await ctx.storage.setItems([
          { key: "log:1", value: 1 },
          { key: "log:2", value: 2 },
        ]);
        const reader = () =>
          createStorage({
            driver: driver({
              firestore: cachingFirestore,
              collection: "test",
              lib: firestoreLib,
              incremental: true,
            }),
          });
        await (
          await reader().watch(() => {})
        )();

        // Offline, the next start has everything from the cache alone.
        await firestoreLib.disableNetwork(cachingFirestore);
        const reopened = reader();
        const heard: string[] = [];
        const unwatch = await reopened.watch((event, key) => heard.push(`${event} ${key}`));
        expect((await reopened.getKeys("log")).sort()).toEqual(["log:1", "log:2"]);

        // Back online, entries written meanwhile arrive; older ones are not reported again.
        await ctx.storage.setItem("log:3", 3);
        await firestoreLib.enableNetwork(cachingFirestore);
        await expect.poll(() => reopened.getItem("log:3")).toBe(3);
        expect(heard).toEqual(["update log:3"]);
        await unwatch();
      });
    },
  });
});
