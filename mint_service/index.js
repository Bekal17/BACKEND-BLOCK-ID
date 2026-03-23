/**
 * BlockID NFT Mint Service
 * Metaplex Core SDK — standard Core NFT (NOT compressed), soul-bound via PermanentFreezeDelegate
 *
 * Runs as standalone Express server on port 3001
 * Called by FastAPI via HTTP POST /mint
 *
 * Environment variables:
 *   MINT_KEYPAIR_PATH - path to BlockID authority keypair JSON
 *   BLOCKID_MINT_AUTHORITY - alternative: base64 (64 bytes) or base58 private key
 *   SOLANA_RPC_URL - RPC endpoint
 *   PORT - server port (default 3001)
 */

import express from "express";
import { createUmi } from "@metaplex-foundation/umi-bundle-defaults";
import { mplCore, create, update, fetchAsset } from "@metaplex-foundation/mpl-core";
import { generateSigner, keypairIdentity, publicKey } from "@metaplex-foundation/umi";
import { fromWeb3JsKeypair } from "@metaplex-foundation/umi-web3js-adapters";
import { Keypair } from "@solana/web3.js";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import bs58 from "bs58";

const __dirname = path.dirname(fileURLToPath(import.meta.url));

const PORT = parseInt(process.env.PORT || "3001", 10);
const SOLANA_RPC_URL = process.env.SOLANA_RPC_URL || "https://api.devnet.solana.com";
const RPC_URL = SOLANA_RPC_URL;
const MINT_KEYPAIR_PATH = process.env.MINT_KEYPAIR_PATH || path.join(__dirname, "keypair.json");
const BLOCKID_MINT_AUTHORITY = process.env.BLOCKID_MINT_AUTHORITY || "";

/** Load raw secret key (64 bytes) for Solana Keypair. Supports base64 or base58. */
function loadSecretKey() {
  if (BLOCKID_MINT_AUTHORITY) {
    const raw = BLOCKID_MINT_AUTHORITY.trim();
    let secretKey;

    // Try base64 first
    try {
      const decoded = Buffer.from(raw, "base64");
      if (decoded.length === 64) {
        secretKey = new Uint8Array(decoded);
      }
    } catch {}

    // Fallback: try bs58
    if (!secretKey) {
      try {
        const decoded = bs58.decode(raw);
        if (decoded && decoded.length === 64) {
          secretKey = new Uint8Array(decoded);
        }
      } catch {}
    }

    if (!secretKey || secretKey.length !== 64) {
      console.error(
        "[mint_service] ERROR: BLOCKID_MINT_AUTHORITY invalid. Use base64 (64 bytes) or base58 format. Or set MINT_KEYPAIR_PATH.",
      );
      process.exit(1);
    }
    return secretKey;
  }

  const keyPath = path.resolve(MINT_KEYPAIR_PATH);
  if (!fs.existsSync(keyPath)) {
    console.error(
      "[mint_service] ERROR: No keypair configured. Set BLOCKID_MINT_AUTHORITY or MINT_KEYPAIR_PATH",
    );
    process.exit(1);
  }
  const raw = JSON.parse(fs.readFileSync(keyPath, "utf8"));
  return new Uint8Array(raw);
}

/** Web3.js Keypair for UMI adapter (Metaplex Core uses this for correct program/signer behavior). */
function loadKeypair() {
  return Keypair.fromSecretKey(loadSecretKey());
}

const app = express();
app.use(express.json());

let umi = null;
let keypairLoaded = false;

function getUmi() {
  if (!umi) {
    umi = createUmi(RPC_URL).use(mplCore());
    try {
      const keypair = loadKeypair();
      const umiKeypair = fromWeb3JsKeypair(keypair);
      umi.use(keypairIdentity(umiKeypair));
      keypairLoaded = true;
    } catch (e) {
      console.error("[mint_service] Keypair load failed:", e.message);
      throw e;
    }
  }
  return umi;
}

app.post("/mint", async (req, res) => {
  const { wallet, metadata_uri } = req.body || {};
  if (!wallet || typeof wallet !== "string") {
    return res.status(400).json({ error: "Missing wallet" });
  }
  const uri = metadata_uri || `https://api.blockidscore.fun/identity/${wallet}`;

  try {
    const umiInstance = getUmi();
    const asset = generateSigner(umiInstance);
    const recipientPubkey = publicKey(wallet);

    // Soul-bound: immutable metadata + permanently frozen with no authority (no one can unfreeze)
    const result = await create(umiInstance, {
      asset,
      name: `BlockID Identity`,
      uri,
      owner: recipientPubkey,
      plugins: [
        { type: "PermanentFreezeDelegate", frozen: true, authority: { type: "None" } },
        { type: "ImmutableMetadata" },
      ],
    }).sendAndConfirm(umiInstance);

    const sig =
      typeof result.signature === "string"
        ? result.signature
        : result.signature != null
          ? bs58.encode(Buffer.from(result.signature))
          : "";
    const mintAddress = asset.publicKey.toString();

    console.log(`[mint_service] Minted for ${wallet.slice(0, 16)}... mint=${mintAddress}`);
    return res.json({
      mint_address: mintAddress,
      signature: sig,
    });
  } catch (e) {
    console.error("[mint_service] Mint failed:", e.message);
    return res.status(500).json({
      error: "Mint failed",
      message: e.message,
    });
  }
});

// POST /mint-handle — Handle NFT: transferable (NO PermanentFreezeDelegate, NO ImmutableMetadata)
app.post("/mint-handle", async (req, res) => {
  const { wallet, handle, metadata_uri } = req.body || {};
  if (!wallet || !handle) {
    return res.status(400).json({ error: "wallet and handle required" });
  }
  const handleNorm = (handle + "").trim().replace(/^@/, "");
  const uri =
    metadata_uri ||
    `${process.env.METADATA_BASE_URL || "https://api.blockidscore.fun/handle"}/${handleNorm}`;

  try {
    const umiInstance = getUmi();
    const assetSigner = generateSigner(umiInstance);
    const result = await create(umiInstance, {
      asset: assetSigner,
      name: `@${handleNorm}`,
      uri,
      owner: publicKey(wallet),
      plugins: [],
    }).sendAndConfirm(umiInstance);

    const mintAddress = assetSigner.publicKey.toString();
    const sig =
      typeof result.signature === "string"
        ? result.signature
        : result.signature != null
          ? bs58.encode(Buffer.from(result.signature))
          : mintAddress;
    console.log(`[mint_service] Handle minted: @${handleNorm} → ${mintAddress}`);
    res.json({
      mint_address: mintAddress,
      signature: sig,
    });
  } catch (err) {
    console.error("[mint_service] mint-handle error:", err.message);
    res.status(500).json({ error: err.message });
  }
});

// POST /update-uri — Update metadata URI of an existing NFT (fix wrong METADATA_BASE_URL)
app.post("/update-uri", async (req, res) => {
  const { mint_address, new_uri } = req.body || {};
  if (!mint_address || !new_uri) {
    return res.status(400).json({ error: "mint_address and new_uri required" });
  }

  try {
    const umiInstance = getUmi();
    const assetAddress = publicKey(mint_address);
    const asset = await fetchAsset(umiInstance, assetAddress);

    await update(umiInstance, {
      asset,
      uri: new_uri,
    }).sendAndConfirm(umiInstance);

    console.log(`[mint_service] Updated URI for ${mint_address.slice(0, 16)}... → ${new_uri}`);
    res.json({
      success: true,
      mint_address,
      new_uri,
      message: "Metadata URI updated successfully",
    });
  } catch (err) {
    console.error("[mint_service] update-uri error:", err.message);
    res.status(500).json({ error: err.message });
  }
});

app.get("/health", (req, res) => {
  res.json({
    status: "ok",
    service: "blockid-mint-service",
    keypair_loaded: keypairLoaded,
    network: SOLANA_RPC_URL.includes("devnet") ? "devnet" : "mainnet",
    timestamp: new Date().toISOString(),
  });
});

app.listen(PORT, () => {
  console.log(`BlockID Mint Service listening on port ${PORT}`);
  try {
    getUmi();
    console.log("[mint_service] Keypair loaded successfully");
  } catch (e) {
    console.error("[mint_service] ERROR: No keypair configured. Set BLOCKID_MINT_AUTHORITY or MINT_KEYPAIR_PATH");
    process.exit(1);
  }
});
