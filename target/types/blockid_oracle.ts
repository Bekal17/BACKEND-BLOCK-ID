/**
 * Program IDL in camelCase format in order to be used in JS/TS.
 *
 * Note that this is only a type helper and is not the actual IDL. The original
 * IDL can be found at `target/idl/blockid_oracle.json`.
 */
export type BlockidOracle = {
  "address": "9etRwVdKdVkvRsMbYVroGPzcdDxZnRmDH1D8Ho6waXGA",
  "metadata": {
    "name": "blockidOracle",
    "version": "0.1.0",
    "spec": "0.1.0"
  },
  "instructions": [
    {
      "name": "updateTrustScore",
      "discriminator": [
        100,
        231,
        130,
        250,
        180,
        196,
        20,
        248
      ],
      "accounts": [
        {
          "name": "trustScoreAccount",
          "writable": true,
          "pda": {
            "seeds": [
              {
                "kind": "const",
                "value": [
                  116,
                  114,
                  117,
                  115,
                  116,
                  95,
                  115,
                  99,
                  111,
                  114,
                  101
                ]
              },
              {
                "kind": "account",
                "path": "oracle"
              },
              {
                "kind": "account",
                "path": "wallet"
              }
            ]
          }
        },
        {
          "name": "oracle",
          "writable": true,
          "signer": true
        },
        {
          "name": "wallet"
        },
        {
          "name": "systemProgram",
          "address": "11111111111111111111111111111111"
        }
      ],
      "args": [
        {
          "name": "score",
          "type": "u8"
        },
        {
          "name": "risk",
          "type": "u8"
        }
      ]
    }
  ],
  "accounts": [
    {
      "name": "trustScoreAccount",
      "discriminator": [
        102,
        5,
        198,
        10,
        40,
        255,
        164,
        252
      ]
    }
  ],
  "types": [
    {
      "name": "trustScoreAccount",
      "type": {
        "kind": "struct",
        "fields": [
          {
            "name": "wallet",
            "type": "pubkey"
          },
          {
            "name": "score",
            "type": "u8"
          },
          {
            "name": "risk",
            "type": "u8"
          },
          {
            "name": "updatedAt",
            "type": "i64"
          }
        ]
      }
    }
  ]
};
